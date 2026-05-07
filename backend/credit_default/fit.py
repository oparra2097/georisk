"""
Fit the sovereign credit default model from a (country, year) panel of
macro indicators paired with the binary default-within-N-years target.

Two estimators are provided:

  1. ``fit_logit``  — class-weighted logistic regression (statsmodels Logit
     for inference, fallback to sklearn if statsmodels missing).
     Produces interpretable coefficients in standard-deviation units.

  2. ``fit_gbm``    — gradient-boosted classifier (sklearn). Better
     out-of-sample AUC on rare-event sovereign panels but harder to
     read; still gives a permutation-importance ranking.

Both estimators output a unified "fit_state" JSON consumed by
``rating_model.py``:

    {
      "estimator": "logit" | "gbm",
      "horizon_years": 1 | 3 | 5,
      "coefficients": {indicator: float}     # logit only
      "feature_importance": {indicator: float},
      "intercept": float                      # logit only
      "scaler": {indicator: {mean, std}},
      "pd_calibration": [{score_lo, score_hi, pd}],
      "auc_in_sample": float,
      "n_obs": int,
      "n_events": int,
      "trained_at": ISO timestamp,
    }

Usage from the CLI runner::

    python scripts/fit_credit_default.py --estimator logit --horizon 1
"""

from __future__ import annotations

import json
import math
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from backend.credit_default import data as cd_data
from backend.credit_default import defaults as cd_defaults
from backend.credit_default.rating_model import WEIGHTS as SCAFFOLD_WEIGHTS


# State persisted to disk so the live service can read it without re-fitting.
_FIT_DIR = Path(__file__).resolve().parent.parent.parent / 'data' / 'credit_default_fit'
_FIT_DIR.mkdir(parents=True, exist_ok=True)


def fit_state_path(horizon_years: int) -> Path:
    return _FIT_DIR / f'fit_state_h{horizon_years}.json'


# ── Panel construction ──────────────────────────────────────────────────


def build_training_panel(years_back: int = 25, horizon_years: int = 1):
    """Return (X, y, feature_names, meta) ready for a sklearn estimator.

    X is a pandas DataFrame of indicator columns, y a pandas Series of 0/1
    labels. Rows where the country is *already* in default this year are
    dropped (predicting "default next year" doesn't apply when you've
    already missed payments). Rows with all indicators NaN are dropped.
    Remaining NaNs are median-imputed within column.
    """
    try:
        import pandas as pd  # noqa: F401
        import numpy as np
    except ImportError as e:
        raise RuntimeError(f'pandas/numpy required: {e}')

    panel = cd_data.get_history_panel(years_back=years_back)
    if panel is None or panel.empty:
        raise RuntimeError('history panel empty — IMF/WB fetch failed?')

    label_df = cd_defaults.build_label_frame(
        panel[['iso3', 'year']].itertuples(index=False, name=None),
        horizons=(horizon_years,),
    )
    df = panel.merge(label_df, on=['iso3', 'year'], how='left')

    # Drop years where the country is currently in default — those are
    # not "predict next year" rows.
    df = df[df['in_default_year'] != 1].copy()

    feature_names = list(SCAFFOLD_WEIGHTS.keys())
    feature_names = [f for f in feature_names if f in df.columns]

    X = df[feature_names].copy()
    # Coerce columns to numeric (some come back as object after merge).
    for col in feature_names:
        X[col] = pd.to_numeric(X[col], errors='coerce')

    y_col = f'defaulted_within_{horizon_years}y'
    y = df[y_col].fillna(0).astype(int)

    # Drop rows with no indicators at all (typical for tiny states pre-2000).
    keep = X.notna().any(axis=1)
    X, y, df = X[keep], y[keep], df[keep]

    # Median-impute within column. This is a defensible choice for cross-
    # sectional macro data and keeps rare-event observations in the panel.
    medians = X.median(numeric_only=True)
    X = X.fillna(medians)

    # Standardize so coefficients are in σ units and the GBM is well-conditioned.
    means = X.mean()
    stds = X.std().replace(0, 1.0)
    X_std = (X - means) / stds

    meta = {
        'feature_names': feature_names,
        'scaler': {f: {'mean': float(means[f]), 'std': float(stds[f])} for f in feature_names},
        'medians': {f: float(medians[f]) for f in feature_names},
        'horizon_years': horizon_years,
        'n_obs': int(len(X_std)),
        'n_events': int(y.sum()),
        'iso_years': df[['iso3', 'year']].reset_index(drop=True),
    }
    return X_std, y, feature_names, meta


# ── Logistic regression ─────────────────────────────────────────────────


def fit_logit(horizon_years: int = 1, years_back: int = 25) -> Dict:
    X, y, features, meta = build_training_panel(years_back, horizon_years)

    coefs: Dict[str, float] = {}
    intercept: float = 0.0
    auc: Optional[float] = None
    method = 'sklearn'

    try:
        from sklearn.linear_model import LogisticRegression
        from sklearn.metrics import roc_auc_score

        model = LogisticRegression(
            penalty='l2', C=1.0, solver='lbfgs', max_iter=2000,
            class_weight='balanced',  # rare events
        )
        model.fit(X.values, y.values)
        for f, w in zip(features, model.coef_[0]):
            coefs[f] = float(w)
        intercept = float(model.intercept_[0])
        proba = model.predict_proba(X.values)[:, 1]
        if y.sum() > 0 and y.sum() < len(y):
            auc = float(roc_auc_score(y.values, proba))
    except ImportError as e:
        raise RuntimeError(f'sklearn required for fit_logit: {e}')

    # Calibrate PD by score decile on the in-sample distribution. This is
    # an empirical hazard table — replaces the hand-set RATING_BUCKETS PDs.
    pd_calibration = _calibrate_pd_buckets(proba, y.values)

    state = {
        'estimator': 'logit',
        'method': method,
        'horizon_years': horizon_years,
        'coefficients': coefs,
        'intercept': intercept,
        'feature_importance': {f: abs(coefs[f]) for f in features},
        'scaler': meta['scaler'],
        'medians': meta['medians'],
        'pd_calibration': pd_calibration,
        'auc_in_sample': auc,
        'n_obs': meta['n_obs'],
        'n_events': meta['n_events'],
        'trained_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
    }
    _save_state(state, horizon_years)
    return state


# ── Gradient boost ──────────────────────────────────────────────────────


def fit_gbm(horizon_years: int = 1, years_back: int = 25,
            n_estimators: int = 300, max_depth: int = 3,
            learning_rate: float = 0.05) -> Dict:
    X, y, features, meta = build_training_panel(years_back, horizon_years)

    try:
        from sklearn.ensemble import GradientBoostingClassifier
        from sklearn.metrics import roc_auc_score
        from sklearn.inspection import permutation_importance
    except ImportError as e:
        raise RuntimeError(f'sklearn required for fit_gbm: {e}')

    # subsample < 1 + balanced sample weights handles rare-event imbalance.
    pos_weight = (len(y) - y.sum()) / max(1, y.sum())
    sample_weight = (y * (pos_weight - 1) + 1).values

    model = GradientBoostingClassifier(
        n_estimators=n_estimators, max_depth=max_depth,
        learning_rate=learning_rate, subsample=0.8,
        random_state=42, min_samples_leaf=20,
    )
    model.fit(X.values, y.values, sample_weight=sample_weight)
    proba = model.predict_proba(X.values)[:, 1]
    auc = float(roc_auc_score(y.values, proba)) if 0 < y.sum() < len(y) else None

    perm = permutation_importance(
        model, X.values, y.values, n_repeats=10,
        random_state=42, scoring='roc_auc',
    )
    importance = {f: float(perm.importances_mean[i]) for i, f in enumerate(features)}

    pd_calibration = _calibrate_pd_buckets(proba, y.values)

    # We persist the model's leaf-value structure as JSON-incompatible —
    # callers that want to *score* with the GBM must call ``fit_gbm`` at
    # bootstrap. We still publish feature importance + calibration for
    # rating_model to consume in "weighted average" mode.
    state = {
        'estimator': 'gbm',
        'horizon_years': horizon_years,
        'feature_importance': importance,
        'coefficients': importance,  # interpreted as relative weights
        'intercept': 0.0,
        'scaler': meta['scaler'],
        'medians': meta['medians'],
        'pd_calibration': pd_calibration,
        'auc_in_sample': auc,
        'n_obs': meta['n_obs'],
        'n_events': meta['n_events'],
        'trained_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'hyperparams': {
            'n_estimators': n_estimators, 'max_depth': max_depth,
            'learning_rate': learning_rate,
        },
    }
    _save_state(state, horizon_years)
    return state


# ── PD calibration ──────────────────────────────────────────────────────


def _calibrate_pd_buckets(proba, y_true, n_buckets: int = 20) -> List[Dict]:
    """Empirical PD by predicted-score bucket.

    Sort observations by predicted probability, slice into ``n_buckets``
    equal-count buckets, report the realized default rate inside each.
    This replaces the hand-set PD column in RATING_BUCKETS with an
    actual empirical hazard once we have enough events.
    """
    try:
        import numpy as np
    except ImportError:
        return []

    order = np.argsort(proba)
    proba_sorted = np.array(proba)[order]
    y_sorted = np.array(y_true)[order]
    n = len(proba_sorted)
    if n == 0:
        return []

    edges = np.linspace(0, n, n_buckets + 1).astype(int)
    out: List[Dict] = []
    for i in range(n_buckets):
        lo, hi = edges[i], edges[i + 1]
        if hi <= lo:
            continue
        slc_p = proba_sorted[lo:hi]
        slc_y = y_sorted[lo:hi]
        out.append({
            'bucket': i + 1,
            'score_lo': float(slc_p.min()),
            'score_hi': float(slc_p.max()),
            'pd_empirical': float(slc_y.mean()),
            'n_obs': int(hi - lo),
            'n_events': int(slc_y.sum()),
        })
    return out


# ── Persistence ─────────────────────────────────────────────────────────


def _save_state(state: Dict, horizon_years: int) -> None:
    path = fit_state_path(horizon_years)
    # ``iso_years`` snuck in via meta — strip non-JSON-serializable bits.
    serializable = {k: v for k, v in state.items() if k != 'iso_years'}
    with open(path, 'w') as f:
        json.dump(serializable, f, indent=2)
    print(f'[credit_default.fit] wrote {path}')


def load_state(horizon_years: int = 1) -> Optional[Dict]:
    path = fit_state_path(horizon_years)
    if not path.exists():
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def has_fitted_model(horizon_years: int = 1) -> bool:
    return fit_state_path(horizon_years).exists()


# ── Scoring (used by rating_model when a fit is loaded) ─────────────────


def score_with_state(country_indicators: Dict[str, Optional[float]],
                     state: Dict) -> Tuple[float, float]:
    """Return (latent_logit, predicted_pd) for one country.

    Works for *both* logit and gbm states: the linear combination of
    standardized features × coefficients (or × normalized importances) is
    used as a univariate score, then mapped to PD via the empirical
    calibration table. For the gbm state this is an approximation of the
    real GBM (since the tree ensemble isn't JSON-serialised); rerun
    ``fit_gbm`` in-process if you need exact GBM probabilities.
    """
    coefs = state.get('coefficients') or {}
    scaler = state.get('scaler') or {}
    medians = state.get('medians') or {}
    intercept = float(state.get('intercept') or 0.0)

    z = intercept
    for feat, w in coefs.items():
        raw = country_indicators.get(feat)
        if raw is None or (isinstance(raw, float) and math.isnan(raw)):
            raw = medians.get(feat)
        if raw is None:
            continue
        s = scaler.get(feat) or {}
        mean = float(s.get('mean', 0.0))
        std = float(s.get('std', 1.0)) or 1.0
        z += w * ((raw - mean) / std)

    if state.get('estimator') == 'logit':
        # Logistic squash for a true probability.
        try:
            pd_hat = 1.0 / (1.0 + math.exp(-z))
        except OverflowError:
            pd_hat = 0.0 if z < 0 else 1.0
    else:
        # GBM state: map z through the empirical calibration table.
        pd_hat = _pd_from_calibration(z, state.get('pd_calibration') or [])
    return z, pd_hat


def _pd_from_calibration(score: float, calibration: List[Dict]) -> float:
    if not calibration:
        return 0.0
    for bucket in calibration:
        if score <= bucket['score_hi']:
            return float(bucket['pd_empirical'])
    return float(calibration[-1]['pd_empirical'])
