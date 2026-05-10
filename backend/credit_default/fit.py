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

from backend.credit_default import agency_ratings as cd_agencies
from backend.credit_default import data as cd_data
from backend.credit_default import defaults as cd_defaults
from backend.credit_default.rating_model import (
    HIGHER_IS_WORSE as SCAFFOLD_HIGHER_IS_WORSE,
    WEIGHTS as SCAFFOLD_WEIGHTS,
    _adjusted_shift,
)


# State persisted to disk so the live service can read it without re-fitting.
_FIT_DIR = Path(__file__).resolve().parent.parent.parent / 'data' / 'credit_default_fit'
_FIT_DIR.mkdir(parents=True, exist_ok=True)


def fit_state_path(horizon_years: int) -> Path:
    return _FIT_DIR / f'fit_state_h{horizon_years}.json'


def fit_state_path_q(horizon_quarters: int) -> Path:
    return _FIT_DIR / f'fit_state_q{horizon_quarters}.json'


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
        raise RuntimeError(
            'history panel is empty — IMF WEO and World Bank APIs returned '
            'no data. Confirm outbound network access to '
            'imf.org and api.worldbank.org, then re-run.'
        )
    if 'iso3' not in panel.columns or 'year' not in panel.columns:
        raise RuntimeError(
            'history panel is malformed — missing iso3/year columns. '
            'Likely every indicator fetch failed; check upstream API status.'
        )

    label_df = cd_defaults.build_label_frame(
        panel[['iso3', 'year']].itertuples(index=False, name=None),
        horizons=(horizon_years,),
    )
    df = panel.merge(label_df, on=['iso3', 'year'], how='left')

    # Drop years where the country is currently in default — those are
    # not "predict next year" rows. The dashboard separately
    # force-overrides ``defaulted=True`` and ``pd_1y=1.0`` at inference
    # time for ISOs inside an active CRAG hard-default spell, so the
    # GBM never has to learn "in-default → in-default-next-year". That
    # keeps the trained PD distribution centered around onset risk
    # instead of conflating ongoing-default with next-period onset.
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
        'iso_years': df[['iso3', 'year', 'in_default_year']].reset_index(drop=True),
    }
    return X_std, y, feature_names, meta


def build_training_panel_quarterly(years_back: int = 25,
                                   horizon_quarters: int = 4):
    """Quarterly analog of :func:`build_training_panel`. Returns
    ``(X, y, feature_names, meta)`` at (iso3, year, quarter) grain.

    With our current data, annual indicator values get forward-filled
    across each year's four quarters, so feature distributions per
    quarter are identical within a year. Once we add quarterly-native
    features (CDS, FX vol, etc.) this is where the signal lives.
    """
    try:
        import pandas as pd  # noqa: F401
        import numpy as np
    except ImportError as e:
        raise RuntimeError(f'pandas/numpy required: {e}')

    panel = cd_data.get_history_panel_quarterly(years_back=years_back)
    if panel is None or panel.empty:
        raise RuntimeError(
            'quarterly history panel is empty — annual upstream pull '
            'returned nothing. Confirm IMF WEO and WB connectivity.'
        )
    for col in ('iso3', 'year', 'quarter'):
        if col not in panel.columns:
            raise RuntimeError(
                f'quarterly panel is missing column {col!r}; cannot fit.'
            )

    label_df = cd_defaults.build_quarterly_label_frame(
        panel[['iso3', 'year', 'quarter']].itertuples(index=False, name=None),
        horizons_quarters=(horizon_quarters,),
    )
    df = panel.merge(label_df, on=['iso3', 'year', 'quarter'], how='left')
    df = df[df['in_default_quarter'] != 1].copy()

    feature_names = list(SCAFFOLD_WEIGHTS.keys())
    feature_names = [f for f in feature_names if f in df.columns]

    X = df[feature_names].copy()
    for col in feature_names:
        X[col] = pd.to_numeric(X[col], errors='coerce')

    y_col = f'defaulted_within_{horizon_quarters}q'
    y = df[y_col].fillna(0).astype(int)

    keep = X.notna().any(axis=1)
    X, y, df = X[keep], y[keep], df[keep]

    medians = X.median(numeric_only=True)
    X = X.fillna(medians)
    means = X.mean()
    stds = X.std().replace(0, 1.0)
    X_std = (X - means) / stds

    meta = {
        'feature_names': feature_names,
        'scaler': {f: {'mean': float(means[f]), 'std': float(stds[f])} for f in feature_names},
        'medians': {f: float(medians[f]) for f in feature_names},
        'horizon_quarters': horizon_quarters,
        'n_obs': int(len(X_std)),
        'n_events': int(y.sum()),
        'iso_quarters': df[['iso3', 'year', 'quarter', 'in_default_quarter']].reset_index(drop=True),
    }
    return X_std, y, feature_names, meta


# ── Logistic regression ─────────────────────────────────────────────────


def fit_logit(horizon_years: int = 1, years_back: int = 25) -> Dict:
    X, y, features, meta = build_training_panel(years_back, horizon_years)

    try:
        import numpy as np
        from sklearn.metrics import roc_auc_score
    except ImportError as e:
        raise RuntimeError(f'numpy/sklearn required for fit_logit: {e}')

    sign_vec = np.array([
        +1.0 if SCAFFOLD_HIGHER_IS_WORSE.get(f, True) else -1.0
        for f in features
    ])

    coefs_signed, intercept, proba = _fit_logit_sign_constrained(
        X.values, y.values, sign_vec,
    )
    coefs: Dict[str, float] = {f: float(coefs_signed[i]) for i, f in enumerate(features)}

    # Class-balanced sigmoid PD overstates absolute risk because the loss
    # treats positives and negatives as equally prevalent. Platt-rescale
    # back to the natural panel base rate (log(n_pos / n_neg)) so the
    # model produces *its own* probability of default — independent of
    # the rating agencies. The dashboard still overlays an agency
    # reference line on the chart for context, but the headline PD is
    # purely from the macro-features-on-historical-defaults fit.
    n_pos = int(y.sum())
    n_neg = int(len(y) - n_pos)
    pd_log_odds_shift = (
        math.log(max(1, n_pos) / max(1, n_neg)) if n_pos and n_neg else 0.0
    )

    auc: Optional[float] = None
    if y.sum() > 0 and y.sum() < len(y):
        auc = float(roc_auc_score(y.values, proba))

    pd_calibration = _calibrate_pd_buckets(proba, y.values)
    rating_buckets = _calibrate_rating_buckets(
        meta['iso_years'], proba, horizon_years=horizon_years,
        class_balance_log_odds=pd_log_odds_shift,
    )

    state = {
        'estimator': 'logit',
        'method': 'scipy-l-bfgs-b (sign-constrained), natural-rate Platt',
        'horizon_years': horizon_years,
        'coefficients': coefs,
        'intercept': float(intercept),
        'class_balance_log_odds': float(pd_log_odds_shift),
        'feature_importance': {f: abs(coefs[f]) for f in features},
        'scaler': meta['scaler'],
        'medians': meta['medians'],
        'pd_calibration': pd_calibration,
        'rating_buckets': rating_buckets,
        'auc_in_sample': auc,
        'n_obs': meta['n_obs'],
        'n_events': meta['n_events'],
        'trained_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
    }
    _save_state(state, horizon_years)
    return state


def _fit_logit_sign_constrained(X, y, sign_vec):
    """L2-penalized class-weighted logistic regression with the sign of
    each coefficient pinned by ``sign_vec``.

    We rewrite β_k = s_k · α_k where s_k = ±1 and α_k ≥ 0, then optimize
    α by L-BFGS-B with non-negativity bounds. The intercept is
    unconstrained. Returns (β, intercept, predicted_probabilities).

    This replaces sklearn's unconstrained LogisticRegression — without
    sign constraints, multicollinearity in the macro panel was flipping
    inflation/external-debt/governance coefs to the wrong sign and
    pulling deep-junk sovereigns toward AAA.
    """
    import numpy as np
    from scipy.optimize import minimize

    X = np.asarray(X, dtype=float)
    y = np.asarray(y, dtype=float)
    sign_vec = np.asarray(sign_vec, dtype=float)
    n, k = X.shape

    # Class-balanced weights (matches sklearn class_weight='balanced'):
    # rare events get up-weighted so the loss isn't dominated by negatives.
    n_pos = max(1.0, float(y.sum()))
    n_neg = max(1.0, float(n - y.sum()))
    w_pos = n / (2.0 * n_pos)
    w_neg = n / (2.0 * n_neg)
    sample_w = np.where(y > 0.5, w_pos, w_neg)

    # Pre-flip features: X_signed = X * sign_vec along the feature axis.
    # Then β_k = s_k · α_k means β·x = α · X_signed.
    X_signed = X * sign_vec[None, :]

    # L2 strength matched to sklearn's C=1.0 default (penalty 1 / (2C·n)).
    lam = 1.0 / (2.0 * 1.0 * n)

    def neg_log_likelihood(theta):
        alpha, b = theta[:-1], theta[-1]
        z = X_signed @ alpha + b
        # log(1+exp(z)) computed safely.
        log1pez = np.where(z > 0, z + np.log1p(np.exp(-z)), np.log1p(np.exp(z)))
        nll = np.sum(sample_w * (log1pez - y * z)) / n
        nll += lam * np.sum(alpha * alpha)
        return nll

    def grad(theta):
        alpha, b = theta[:-1], theta[-1]
        z = X_signed @ alpha + b
        p = 1.0 / (1.0 + np.exp(-z))
        resid = sample_w * (p - y)
        g_alpha = X_signed.T @ resid / n + 2.0 * lam * alpha
        g_b = float(np.sum(resid) / n)
        return np.concatenate([g_alpha, [g_b]])

    bounds = [(0.0, None)] * k + [(None, None)]
    theta0 = np.zeros(k + 1)
    theta0[-1] = math.log(n_pos / n_neg)  # warm-start intercept at base-rate logit

    result = minimize(
        neg_log_likelihood, theta0, jac=grad, method='L-BFGS-B',
        bounds=bounds, options={'maxiter': 500, 'ftol': 1e-9},
    )
    alpha = np.maximum(result.x[:-1], 0.0)
    intercept = float(result.x[-1])
    beta = sign_vec * alpha

    z = X @ beta + intercept
    proba = 1.0 / (1.0 + np.exp(-z))
    return beta, intercept, proba


# ── Gradient boost ──────────────────────────────────────────────────────


def fit_gbm(horizon_years: int = 1, years_back: int = 25,
            n_estimators: int = 300, max_depth: int = 3,
            learning_rate: float = 0.05) -> Dict:
    X, y, features, meta = build_training_panel(years_back, horizon_years)

    try:
        import numpy as np
        from sklearn.ensemble import GradientBoostingClassifier
        from sklearn.metrics import roc_auc_score, brier_score_loss
        from sklearn.inspection import permutation_importance
        from sklearn.model_selection import GroupKFold
    except ImportError as e:
        raise RuntimeError(f'sklearn/numpy required for fit_gbm: {e}')

    # ── Switch from class_weight='balanced' to scale_pos_weight ──────
    # Petropoulos 2022 + Savona-Vezzoli 2015: random over-weighting on
    # rare events leaks information across folds and inflates AUC.
    # XGBoost-style scale_pos_weight (= n_neg / n_pos) up-weights
    # positives only, less aggressive at flattening signal than
    # class_weight='balanced'.
    n_pos = int(y.sum())
    n_neg = int(len(y) - n_pos)
    scale_pos_weight = n_neg / max(1, n_pos)
    sample_weight = np.where(y.values == 1, scale_pos_weight, 1.0)

    model = GradientBoostingClassifier(
        n_estimators=n_estimators, max_depth=max_depth,
        learning_rate=learning_rate, subsample=0.8,
        # max_features='sqrt' picks ~sqrt(n_features) candidates per
        # split, preventing any single dominant feature (previously
        # gdp_per_capita_ppp at 27% importance) from being chosen at
        # every split level. Spreads importance more evenly.
        max_features='sqrt',
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

    # ── Out-of-sample validation (GroupKFold by country) ─────────────
    # In-sample AUC is fits, not predictions. Petropoulos 2022 and
    # Savona-Vezzoli 2015 both warn that random-fold AUC is leaky on
    # sovereign panels because the same country appears in train and
    # test. GroupKFold by iso3 holds out *whole countries*, so the
    # reported AUC is what you'd see on a brand-new sovereign that
    # was never observed at fit time. Brier score on the same folds
    # gives the SRDSF-style headline calibration metric.
    oos = _evaluate_oos_groupkfold(
        X, y, meta, n_estimators, max_depth, learning_rate,
        scale_pos_weight, GroupKFold, GradientBoostingClassifier,
        roc_auc_score, brier_score_loss,
    )

    # GBM trained with class-balanced sample_weight overstates absolute
    # PD just like the logit; rescale to the natural panel base rate so
    # the model is independent of agency PD tables.
    n_pos = int(y.sum())
    n_neg = int(len(y) - n_pos)
    pd_log_odds_shift = (
        math.log(max(1, n_pos) / max(1, n_neg)) if n_pos and n_neg else 0.0
    )

    pd_calibration = _calibrate_pd_buckets(proba, y.values)
    rating_buckets = _calibrate_rating_buckets(
        meta['iso_years'], proba, horizon_years=horizon_years,
        class_balance_log_odds=pd_log_odds_shift,
    )

    # Sign-correct GBM importances using HIGHER_IS_WORSE so the linear
    # scorer in rating_model gets directionally correct contributions.
    # (GBM importances themselves carry no sign — without this, "higher
    # GDP-per-capita is good" gets scored as if it were bad.)
    importance_signed: Dict[str, float] = {}
    for f, imp in importance.items():
        s = +1.0 if SCAFFOLD_HIGHER_IS_WORSE.get(f, True) else -1.0
        importance_signed[f] = float(s * imp)

    # Persist the trained GBM model alongside the JSON state so the
    # dashboard score path can run the actual tree ensemble at inference
    # time instead of approximating it with a linear sum of importances.
    # The linear approximation flattens every country to ~base-rate PD
    # (no tree interactions = no sensitivity); using the real model
    # restores GBM's non-linear discrimination.
    import pickle
    model_path = _FIT_DIR / f'fit_model_h{horizon_years}.pkl'
    with open(model_path, 'wb') as f:
        pickle.dump({'model': model, 'features': features}, f)

    state = {
        'estimator': 'gbm',
        'horizon_years': horizon_years,
        'feature_importance': importance,
        'coefficients': importance_signed,
        'intercept': 0.0,
        'class_balance_log_odds': float(pd_log_odds_shift),
        'scaler': meta['scaler'],
        'medians': meta['medians'],
        'pd_calibration': pd_calibration,
        'rating_buckets': rating_buckets,
        'auc_in_sample': auc,
        'auc_oos': oos.get('auc'),
        'brier_oos': oos.get('brier'),
        'oos_method': oos.get('method'),
        'oos_n_folds': oos.get('n_folds'),
        'n_obs': meta['n_obs'],
        'n_events': meta['n_events'],
        'trained_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'hyperparams': {
            'n_estimators': n_estimators, 'max_depth': max_depth,
            'learning_rate': learning_rate,
            'scale_pos_weight': scale_pos_weight,
        },
        'model_pickle': f'fit_model_h{horizon_years}.pkl',
    }
    _save_state(state, horizon_years)
    return state


def _evaluate_oos_groupkfold(
    X, y, meta, n_estimators, max_depth, learning_rate,
    scale_pos_weight, GroupKFold, GradientBoostingClassifier,
    roc_auc_score, brier_score_loss,
    n_splits: int = 5,
) -> Dict:
    """Cross-validated OOS metrics with country-level group folds, so
    the AUC reflects performance on countries the model never saw at
    fit time. Returns ``{auc, brier, method, n_folds}``."""
    try:
        import numpy as np
    except ImportError:
        return {}
    iso_years = meta.get('iso_years')
    if iso_years is None or iso_years.empty:
        return {}
    groups = iso_years['iso3'].values
    unique_groups = np.unique(groups)
    splits = min(n_splits, len(unique_groups))
    if splits < 2:
        return {}
    gkf = GroupKFold(n_splits=splits)
    aucs: List[float] = []
    briers: List[float] = []
    for train_idx, test_idx in gkf.split(X.values, y.values, groups=groups):
        X_tr, X_te = X.values[train_idx], X.values[test_idx]
        y_tr, y_te = y.values[train_idx], y.values[test_idx]
        if y_te.sum() == 0 or y_te.sum() == len(y_te):
            continue  # fold has no positives or no negatives — skip
        sw_tr = np.where(y_tr == 1, scale_pos_weight, 1.0)
        m = GradientBoostingClassifier(
            n_estimators=n_estimators, max_depth=max_depth,
            learning_rate=learning_rate, subsample=0.8,
            random_state=42, min_samples_leaf=20,
        )
        m.fit(X_tr, y_tr, sample_weight=sw_tr)
        proba_te = m.predict_proba(X_te)[:, 1]
        aucs.append(float(roc_auc_score(y_te, proba_te)))
        briers.append(float(brier_score_loss(y_te, proba_te)))
    if not aucs:
        return {}
    return {
        'auc': sum(aucs) / len(aucs),
        'brier': sum(briers) / len(briers),
        'method': f'GroupKFold by iso3 ({splits} folds)',
        'n_folds': len(aucs),
    }


def fit_gbm_quarterly(horizon_quarters: int = 4, years_back: int = 25,
                      n_estimators: int = 300, max_depth: int = 3,
                      learning_rate: float = 0.05) -> Dict:
    """Quarterly-grain GBM fit. Same hyperparameters as :func:`fit_gbm`
    but the panel is re-grained to (iso3, year, quarter) and the target
    is ``defaulted_within_{horizon_quarters}q`` (onset in next N
    quarters). State persists to fit_state_q{N}.json + fit_model_q{N}.pkl
    so the annual files stay untouched.
    """
    X, y, features, meta = build_training_panel_quarterly(
        years_back, horizon_quarters,
    )
    try:
        import numpy as np
        from sklearn.ensemble import GradientBoostingClassifier
        from sklearn.metrics import roc_auc_score, brier_score_loss
        from sklearn.inspection import permutation_importance
        from sklearn.model_selection import GroupKFold
    except ImportError as e:
        raise RuntimeError(f'sklearn/numpy required for fit_gbm_quarterly: {e}')

    n_pos = int(y.sum())
    n_neg = int(len(y) - n_pos)
    scale_pos_weight = n_neg / max(1, n_pos)
    sample_weight = np.where(y.values == 1, scale_pos_weight, 1.0)

    model = GradientBoostingClassifier(
        n_estimators=n_estimators, max_depth=max_depth,
        learning_rate=learning_rate, subsample=0.8,
        # max_features='sqrt' picks ~sqrt(n_features) candidates per
        # split, preventing any single dominant feature (previously
        # gdp_per_capita_ppp at 27% importance) from being chosen at
        # every split level. Spreads importance more evenly.
        max_features='sqrt',
        random_state=42, min_samples_leaf=20,
    )
    model.fit(X.values, y.values, sample_weight=sample_weight)
    proba = model.predict_proba(X.values)[:, 1]
    auc = float(roc_auc_score(y.values, proba)) if 0 < y.sum() < len(y) else None

    perm = permutation_importance(
        model, X.values, y.values, n_repeats=5,
        random_state=42, scoring='roc_auc',
    )
    importance = {f: float(perm.importances_mean[i]) for i, f in enumerate(features)}

    # OOS via GroupKFold by iso3 — same methodology as the annual fit.
    # Use iso_quarters['iso3'] as the group label.
    iso_meta = dict(meta)
    iso_meta['iso_years'] = meta['iso_quarters'][['iso3']]  # only iso3 needed
    oos = _evaluate_oos_groupkfold(
        X, y, iso_meta, n_estimators, max_depth, learning_rate,
        scale_pos_weight, GroupKFold, GradientBoostingClassifier,
        roc_auc_score, brier_score_loss,
    )

    pd_log_odds_shift = (
        math.log(max(1, n_pos) / max(1, n_neg)) if n_pos and n_neg else 0.0
    )

    pd_calibration = _calibrate_pd_buckets(proba, y.values)

    # Bucket calibration for the quarterly path. The annual fit uses
    # this to align display sp_equiv with the agency CDF; without it,
    # the dashboard's quarterly chart maps low quarterly PDs (5-15%)
    # to AAA via the hand-set RATING_BUCKETS, which is wrong. Use the
    # year-equivalent horizon to pick the right sensitivity multiplier.
    years_eq = max(1, int(round(horizon_quarters / 4)))
    iso_years_for_calib = meta['iso_quarters'].rename(
        columns={'in_default_quarter': 'in_default_year'}
    )
    rating_buckets = _calibrate_rating_buckets(
        iso_years_for_calib, proba, horizon_years=years_eq,
        class_balance_log_odds=pd_log_odds_shift,
    )

    importance_signed: Dict[str, float] = {}
    for f, imp in importance.items():
        s = +1.0 if SCAFFOLD_HIGHER_IS_WORSE.get(f, True) else -1.0
        importance_signed[f] = float(s * imp)

    import pickle
    model_path = _FIT_DIR / f'fit_model_q{horizon_quarters}.pkl'
    with open(model_path, 'wb') as f:
        pickle.dump({'model': model, 'features': features}, f)

    state = {
        'estimator': 'gbm',
        'cadence': 'quarterly',
        'horizon_quarters': horizon_quarters,
        'feature_importance': importance,
        'coefficients': importance_signed,
        'intercept': 0.0,
        'class_balance_log_odds': float(pd_log_odds_shift),
        'scaler': meta['scaler'],
        'medians': meta['medians'],
        'pd_calibration': pd_calibration,
        'rating_buckets': rating_buckets,
        'auc_in_sample': auc,
        'auc_oos': oos.get('auc'),
        'brier_oos': oos.get('brier'),
        'oos_method': oos.get('method'),
        'oos_n_folds': oos.get('n_folds'),
        'n_obs': meta['n_obs'],
        'n_events': meta['n_events'],
        'trained_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'hyperparams': {
            'n_estimators': n_estimators, 'max_depth': max_depth,
            'learning_rate': learning_rate,
            'scale_pos_weight': scale_pos_weight,
        },
        'model_pickle': f'fit_model_q{horizon_quarters}.pkl',
    }
    path = fit_state_path_q(horizon_quarters)
    serializable = {k: v for k, v in state.items() if k != 'iso_quarters'}
    with open(path, 'w') as f:
        json.dump(serializable, f, indent=2)
    print(f'[credit_default.fit] wrote {path}')
    return state


def load_gbm_model(horizon_years: int):
    """Load the pickled GBM tree ensemble for live scoring. Returns
    ``(model, feature_names)`` or ``None`` on any failure — including
    sklearn version mismatches between the build environment that
    pickled the model and the runtime environment that's loading it.
    Callers must fall back to the linear-importance approximation
    when this returns None."""
    import pickle
    path = _FIT_DIR / f'fit_model_h{horizon_years}.pkl'
    if not path.exists():
        return None
    try:
        with open(path, 'rb') as f:
            payload = pickle.load(f)
        return payload.get('model'), payload.get('features')
    except Exception as e:  # noqa: BLE001
        # AttributeError, ModuleNotFoundError, ImportError on sklearn
        # internal renames; broad catch is intentional so a stale
        # pickle never takes the dashboard down.
        print(f'[credit_default.fit] load_gbm_model({horizon_years}) failed: {e}')
        return None


# ── PD calibration ──────────────────────────────────────────────────────


# ── Rating-bucket recalibration (against agency consensus) ──────────────


# PM ORR scale → maximum agency consensus_num (1..22, AAA..D) covered by
# that ORR. Mirrors the rating_model.RATING_BUCKETS letter mapping but
# expressed against the 22-notch agency ladder so we can match CDFs
# against the consensus_num column.
_PM_ORR_TO_MAX_CONSENSUS_NUM = {
    1: 1,   2: 2,   3: 3,   4: 4,   5: 5,   6: 6,   7: 7,
    8: 8,   9: 9,   10: 10, 11: 11, 12: 12, 13: 13, 14: 14,
    15: 15, 16: 16,
    17: 19,  # CCC bucket spans CCC+/CCC/CCC- (17/18/19)
    18: 21,  # CC bucket spans CC/C (20/21)
    19: 22,  # SD/RD
    20: 22,  # D — same numeric notch as SD; defaulted flag governs
}


# Agency-consensus notch (1=AAA, 22=D) → benchmark default probability
# at horizons {1, 3, 5}. Same source as RATING_BUCKETS in rating_model.py
# so the back-end and front-end stay aligned. Used to anchor the
# displayed PD scale to what S&P/Moody's/Fitch publish, instead of the
# much-thinner empirical default rate (~4%) in our own panel.
_CONSENSUS_NUM_TO_PD = {
    1: {1: 0.000, 3: 0.001, 5: 0.002},
    2: {1: 0.001, 3: 0.002, 5: 0.005},
    3: {1: 0.001, 3: 0.003, 5: 0.007},
    4: {1: 0.002, 3: 0.005, 5: 0.010},
    5: {1: 0.003, 3: 0.008, 5: 0.015},
    6: {1: 0.005, 3: 0.012, 5: 0.020},
    7: {1: 0.008, 3: 0.018, 5: 0.030},
    8: {1: 0.012, 3: 0.030, 5: 0.050},
    9: {1: 0.018, 3: 0.045, 5: 0.075},
    10: {1: 0.025, 3: 0.060, 5: 0.100},
    11: {1: 0.040, 3: 0.090, 5: 0.150},
    12: {1: 0.060, 3: 0.130, 5: 0.200},
    13: {1: 0.080, 3: 0.170, 5: 0.250},
    14: {1: 0.110, 3: 0.230, 5: 0.330},
    15: {1: 0.150, 3: 0.300, 5: 0.420},
    16: {1: 0.200, 3: 0.380, 5: 0.520},
    17: {1: 0.350, 3: 0.560, 5: 0.700},
    18: {1: 0.350, 3: 0.560, 5: 0.700},
    19: {1: 0.350, 3: 0.560, 5: 0.700},
    20: {1: 0.580, 3: 0.750, 5: 0.830},
    21: {1: 0.700, 3: 0.830, 5: 0.880},
    22: {1: 1.000, 3: 1.000, 5: 1.000},
}


def _logit(p: float, eps: float = 1e-6) -> float:
    p_clipped = min(max(p, eps), 1.0 - eps)
    return math.log(p_clipped / (1.0 - p_clipped))


def _compute_agency_calibration_shift(proba, iso_years_df,
                                      horizon_years: int = 1) -> float:
    """Single log-odds shift that aligns the model's mean PD on anchor
    countries with the agency-consensus mean PD at the given horizon.
    Replaces the natural-base-rate Platt shift, which under-states
    absolute risk relative to how S&P / Moody's / Fitch publish PDs
    (e.g. agencies treat B− as ~20% 1y default vs the panel's 4%
    empirical rate)."""
    try:
        import numpy as np
    except ImportError:
        return 0.0

    proba = np.asarray(proba, dtype=float)
    iso = list(iso_years_df['iso3'].values)
    yrs = list(iso_years_df['year'].values)
    latest_idx: Dict[str, int] = {}
    latest_year: Dict[str, int] = {}
    for i, (s, yr) in enumerate(zip(iso, yrs)):
        if not s:
            continue
        if s not in latest_year or yr > latest_year[s]:
            latest_year[s] = int(yr)
            latest_idx[s] = i

    agencies = cd_agencies.get_agency_ratings()
    diffs: List[float] = []
    for iso3, idx in latest_idx.items():
        ag = agencies.get(iso3) or {}
        c = ag.get('consensus_num')
        if c is None:
            continue
        anchor = _CONSENSUS_NUM_TO_PD.get(int(c)) or {}
        agency_pd = anchor.get(horizon_years)
        if agency_pd is None or agency_pd <= 0 or agency_pd >= 1:
            continue
        diffs.append(_logit(agency_pd) - _logit(float(proba[idx])))
    if len(diffs) < 30:
        return 0.0
    return float(sum(diffs) / len(diffs))


def _calibrate_rating_buckets(iso_years_df, proba, horizon_years: int,
                              class_balance_log_odds: float = 0.0):
    """Compute ``max_score`` per ORR bucket so the model-score CDF lines
    up with the agency-consensus CDF across panel countries.

    For each country in the training panel, take its most recent year's
    predicted PD (in-sample), score = 100·PD. Pair with that country's
    agency consensus_num (median S&P/Moody's/Fitch numeric notch). For
    each ORR k, set ``max_score`` to the empirical model-score quantile
    matching the cumulative share of countries with consensus_num ≤ the
    ORR's max-consensus mapping above. Without this, the headline
    ``score = 100·PD`` lands almost every non-defaulter in the AAA
    bucket because non-defaulter PDs cluster near zero while the
    hand-set ``max_score`` boundaries assume a sigmoid-spread score.
    """
    try:
        import numpy as np
    except ImportError:
        return None

    proba = np.asarray(proba, dtype=float)
    iso = list(iso_years_df['iso3'].values)
    yrs = list(iso_years_df['year'].values)
    if 'in_default_year' in iso_years_df.columns:
        in_def = list(iso_years_df['in_default_year'].values)
    else:
        in_def = [0] * len(iso)

    # Use the SAME shift the dashboard uses at this horizon. The score
    # path applies a per-horizon sensitivity multiplier to the
    # natural-rate Platt shift (1y gets the biggest lift); without
    # mirroring it here, bucket boundaries live in a different score
    # space than displayed PDs, and AAA-rated countries land in BB
    # because the bucket's "AAA boundary" was set against
    # un-amplified scores while displayed scores are amplified.
    display_shift = _adjusted_shift(class_balance_log_odds, horizon_years)

    def _to_score(p: float) -> float:
        p_clipped = min(max(p, 1e-9), 1.0 - 1e-9)
        balanced_logit = math.log(p_clipped / (1.0 - p_clipped))
        natural_logit = balanced_logit + display_shift
        if natural_logit > 0:
            natural_pd = 1.0 / (1.0 + math.exp(-natural_logit))
        else:
            ez = math.exp(natural_logit)
            natural_pd = ez / (1.0 + ez)
        return float(100.0 * natural_pd)

    # Per-country most-recent NON-default-spell year. Currently-in-
    # default rows score near pd=1.0 and would push the agency-CDF
    # mapping to compress all sub-default tiers into the bottom 1% of
    # scores (USA / DEU / JPN ended up in BBB+ because everything
    # between A- and the high-PD cluster got squashed into one
    # boundary). Anchoring on healthy-state observations preserves
    # the AAA-A spread.
    latest_score: Dict[str, float] = {}
    latest_year: Dict[str, int] = {}
    for i, (s, yr, d) in enumerate(zip(iso, yrs, in_def)):
        if not s or int(d) == 1:
            continue
        if s not in latest_year or yr > latest_year[s]:
            latest_year[s] = int(yr)
            latest_score[s] = _to_score(float(proba[i]))

    agencies = cd_agencies.get_agency_ratings()
    # Region lookup for the per-region calibration. Use the live
    # ``data.get_panel`` region field (sourced from the sovereign-
    # debt overlay), so calibration regions match scoring regions.
    try:
        from backend.credit_default import data as _cd_data
        panel_now = _cd_data.get_panel() or {}
        region_by_iso = {
            iso: (c.get('region') or '')
            for iso, c in (panel_now.get('countries') or {}).items()
        }
    except Exception:
        region_by_iso = {}

    triples: List[Tuple[int, float, str]] = []
    for s, score in latest_score.items():
        ag = agencies.get(s) or {}
        c = ag.get('consensus_num')
        if c is not None:
            triples.append((int(c), float(score), region_by_iso.get(s, '') or 'Unknown'))

    if len(triples) < 30:
        # Too few overlap points to recalibrate — let rating_model fall
        # back to its hand-set RATING_BUCKETS.
        return None

    n_total = len(triples)

    def _bucketize(triples_subset: List[Tuple[int, float, str]]) -> Optional[List[Dict]]:
        if not triples_subset:
            return None
        cons = np.array([t[0] for t in triples_subset], dtype=float)
        scs = np.array([t[1] for t in triples_subset], dtype=float)
        n = len(triples_subset)
        sorted_scs = np.sort(scs)
        rows: List[Dict] = []
        for orr in range(1, 21):
            max_c = _PM_ORR_TO_MAX_CONSENSUS_NUM[orr]
            share = float(np.sum(cons <= max_c)) / n
            if share <= 0:
                ms = 0.0
            elif share >= 1.0:
                ms = 100.0
            else:
                idx = max(0, min(n - 1, int(round(share * n)) - 1))
                ms = float(sorted_scs[idx])
            rows.append({
                'pm_numeric': orr,
                'max_consensus_num': max_c,
                'max_score': round(ms, 4),
            })
        # SD/D buckets reserved for the explicit ``defaulted`` flag.
        rows[-1]['max_score'] = 100.0
        rows[-2]['max_score'] = 100.0
        prev = -1e9
        for row in rows:
            if row['max_score'] < prev:
                row['max_score'] = prev
            prev = row['max_score']
        return rows

    # ── Global calibration (fallback) ─────────────────────────────────
    out = _bucketize(triples) or []

    # ── Per-region calibration ────────────────────────────────────────
    # Fit independent bucket maps for each region with at least
    # ``MIN_REGION_ANCHORS`` rated countries. Regions below the
    # threshold (North America, Other, Unknown) fall back to the
    # global map at scoring time.
    MIN_REGION_ANCHORS = 10
    by_region_triples: Dict[str, List[Tuple[int, float, str]]] = {}
    for t in triples:
        by_region_triples.setdefault(t[2], []).append(t)
    by_region_buckets: Dict[str, List[Dict]] = {}
    for region, subset in by_region_triples.items():
        if not region or region in ('Unknown', 'Other') or len(subset) < MIN_REGION_ANCHORS:
            continue
        rows = _bucketize(subset)
        if rows:
            by_region_buckets[region] = rows

    return {
        'method': 'isotonic-regression',
        'horizon_years': horizon_years,
        'n_anchor_countries': n_total,
        'by_region_anchor_count': {r: len(s) for r, s in by_region_triples.items()},
        'by_region': by_region_buckets,
        'buckets': out,
    }


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


def load_state_quarterly(horizon_quarters: int = 4) -> Optional[Dict]:
    path = fit_state_path_q(horizon_quarters)
    if not path.exists():
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def load_gbm_model_quarterly(horizon_quarters: int):
    """Load the pickled quarterly-grain GBM tree ensemble."""
    import pickle
    path = _FIT_DIR / f'fit_model_q{horizon_quarters}.pkl'
    if not path.exists():
        return None
    try:
        with open(path, 'rb') as f:
            payload = pickle.load(f)
        return payload.get('model'), payload.get('features')
    except Exception as e:  # noqa: BLE001
        print(f'[credit_default.fit] load_gbm_model_quarterly({horizon_quarters}) failed: {e}')
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
