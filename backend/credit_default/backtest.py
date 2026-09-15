"""
Backtest harness for the sovereign credit-default model — Tellimer-style
event-time analysis (2025 white paper §3).

Produces the same operational metrics that Tellimer's headline table
reports:

  - recall           — % of crises correctly signalled in the pre-crisis window
  - false_alarm_rate — % of tranquil months incorrectly flagged
  - precision        — P(crisis | signal) = TP / (TP + FP)
  - noise_to_signal  — false_alarm_rate / recall
  - true_signal_rate — TP / total pre-crisis months (Tellimer's stricter recall)
  - lead_time_months — avg months between first alarm and default onset
  - persistence      — avg months the alarm stayed elevated in the pre-crisis window

Two evaluation modes:

  * ``groupkfold``   — 5-fold country-level holdout, refit per fold. Fast,
                       cheap, matches Petropoulos 2022 / Savona-Vezzoli 2015
                       for sovereign panels. Default.
  * ``walkforward``  — expanding-window (fit on ≤year t, predict year t+1),
                       roll forward. Tellimer's headline numbers come from
                       this mode; costlier but tighter on temporal
                       information-leak concerns.

Pre-crisis window (Tellimer §3): 12 months immediately preceding a
default. Alarms earlier than 12 months are counted as *noise*, not
signal — even if fundamentals are actually deteriorating. This is the
paper's deliberately-stringent recall definition.

Multi-horizon evaluation (Tellimer §5): when evaluating the 3Y model, we
EXCLUDE the 0-12 month window before default (that's the 1Y model's
territory) and score only the 12-36 month window. Same principle for 5Y:
skip 0-36 months, score only 36-60. This is what stops 3Y/5Y "recall"
from just being 1Y signal in disguise.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple

from backend.credit_default import defaults as cd_defaults
from backend.credit_default import data as cd_data
from backend.credit_default.rating_model import (
    HIGHER_IS_WORSE as SCAFFOLD_HIGHER_IS_WORSE,
    WEIGHTS as SCAFFOLD_WEIGHTS,
    ALPHA_3Y, ALPHA_5Y,
)


# ── Event-time labelling ────────────────────────────────────────────────


PRE_CRISIS_MONTHS = 12         # Tellimer §3
MONTHS_PER_YEAR = 12


def _horizon_windows(horizon_years: int) -> Tuple[int, int]:
    """Return the (excluded_lower_months, evaluated_upper_months) for
    each horizon per Tellimer §5. For 1Y this is (0, 12) — the standard
    window. For 3Y we skip 0-12 and evaluate 12-36. For 5Y we skip
    0-36 and evaluate 36-60."""
    if horizon_years == 1:
        return (0, 12)
    if horizon_years == 3:
        return (12, 36)
    if horizon_years == 5:
        return (36, 60)
    # Fallback: [0, horizon_years * 12).
    return (0, horizon_years * MONTHS_PER_YEAR)


@dataclass
class Prediction:
    """One row of the backtest panel: PD estimate at a (country, year)
    with the ground-truth default label attached."""
    iso3: str
    year: int
    pd_hat: float                 # predicted probability
    is_pre_crisis: bool           # in the pre-crisis window for the active horizon
    months_to_default: Optional[int] = None    # distance to nearest future onset
    is_in_default: bool = False   # currently in an active default spell


def label_predictions(
    iso3_years: Iterable[Tuple[str, int]],
    pd_values: Iterable[float],
    horizon_years: int,
) -> List[Prediction]:
    """Attach event-time labels to a stream of (iso3, year, pd) triples.

    Uses annual grain (one row per country-year). Months-to-default is
    computed as 12 * (default_year - obs_year) so a default that starts
    IN the observation year has months_to_default == 0.
    """
    lo_excl, hi_eval = _horizon_windows(horizon_years)
    starts_by_iso = cd_defaults.default_starts_by_country()
    in_default_by_iso = cd_defaults.in_default_years_by_country()

    out: List[Prediction] = []
    for (iso3, year), p in zip(iso3_years, pd_values):
        if p is None or (isinstance(p, float) and math.isnan(p)):
            continue
        starts = starts_by_iso.get(iso3, set())
        active = in_default_by_iso.get(iso3, set())
        is_in_default = year in active

        # Find the closest FUTURE default (or same-year onset).
        forward = [s for s in starts if s >= year]
        if forward:
            nearest = min(forward)
            months_to_default = MONTHS_PER_YEAR * (nearest - year)
        else:
            months_to_default = None

        # Pre-crisis flag: within the horizon's evaluation window
        # (excluded_lower, evaluated_upper].  Skip in-default rows —
        # they're neither pre-crisis nor tranquil.
        is_pre = False
        if months_to_default is not None and not is_in_default:
            is_pre = lo_excl < months_to_default <= hi_eval

        out.append(Prediction(
            iso3=iso3, year=int(year), pd_hat=float(p),
            is_pre_crisis=is_pre,
            months_to_default=months_to_default,
            is_in_default=is_in_default,
        ))
    return out


# ── Signal metrics ──────────────────────────────────────────────────────


@dataclass
class SignalMetrics:
    threshold: float
    n_crises: int                  # unique defaults with any observation in the pre-crisis window
    n_crises_signalled: int        # unique defaults that fired at least one alarm in-window
    n_pre_crisis_months: int
    n_pre_crisis_signals: int      # TP months
    n_tranquil_months: int
    n_false_alarms: int            # FP months (signal, no upcoming default)

    @property
    def recall(self) -> float:
        return (self.n_crises_signalled / self.n_crises) if self.n_crises else 0.0

    @property
    def true_signal_rate(self) -> float:
        """Fraction of pre-crisis months correctly flagged. Tellimer's
        stricter recall metric (paper §3)."""
        return (self.n_pre_crisis_signals / self.n_pre_crisis_months) if self.n_pre_crisis_months else 0.0

    @property
    def false_alarm_rate(self) -> float:
        return (self.n_false_alarms / self.n_tranquil_months) if self.n_tranquil_months else 0.0

    @property
    def precision(self) -> float:
        total = self.n_pre_crisis_signals + self.n_false_alarms
        return (self.n_pre_crisis_signals / total) if total else 0.0

    @property
    def noise_to_signal(self) -> float:
        if self.true_signal_rate <= 0:
            return float('inf')
        return self.false_alarm_rate / self.true_signal_rate

    def to_dict(self, lead_time: float, persistence: float, base_rate: float) -> Dict:
        return {
            'threshold': self.threshold,
            'n_crises': self.n_crises,
            'pct_crises_signalled': round(self.recall * 100, 2),
            'true_signals_pct': round(self.true_signal_rate * 100, 2),
            'false_alarms_pct': round(self.false_alarm_rate * 100, 2),
            'noise_to_signal': round(self.noise_to_signal, 3),
            'precision_pct': round(self.precision * 100, 2),
            'unconditional_pct': round(base_rate * 100, 2),
            'lead_time_months': round(lead_time, 2),
            'persistence_months': round(persistence, 2),
        }


def apply_threshold(predictions: List[Prediction], threshold: float,
                    horizon_years: int) -> Tuple[SignalMetrics, float, float]:
    """Confusion-matrix + timing metrics for one threshold.

    Returns (SignalMetrics, avg_lead_time_months, avg_persistence_months).
    In-default rows are dropped from both numerators and denominators —
    they're neither pre-crisis nor tranquil.
    """
    lo_excl, hi_eval = _horizon_windows(horizon_years)

    n_pre_crisis_months = 0
    n_pre_crisis_signals = 0
    n_tranquil_months = 0
    n_false_alarms = 0

    # crisis-level tracking
    crises: Dict[Tuple[str, int], Dict] = {}  # (iso3, default_year) → {'signal_years': set()}

    # persistence per crisis: fraction of pre-crisis months that were flagged
    for p in predictions:
        if p.is_in_default:
            continue
        signal = p.pd_hat >= threshold
        if p.is_pre_crisis:
            n_pre_crisis_months += 1
            if signal:
                n_pre_crisis_signals += 1
                # Find which crisis this observation belongs to.
                default_year = p.year + max(1, p.months_to_default // MONTHS_PER_YEAR)
                key = (p.iso3, default_year)
                crises.setdefault(key, {'signal_years': set(), 'first_signal': None})
                crises[key]['signal_years'].add(p.year)
                if crises[key]['first_signal'] is None or p.year < crises[key]['first_signal']:
                    crises[key]['first_signal'] = p.year
            else:
                # Track the crisis so we can compute recall correctly.
                default_year = p.year + max(1, p.months_to_default // MONTHS_PER_YEAR)
                key = (p.iso3, default_year)
                crises.setdefault(key, {'signal_years': set(), 'first_signal': None})
        else:
            # tranquil (also skips rows within the excluded 0-lo_excl window).
            n_tranquil_months += 1
            if signal:
                n_false_alarms += 1

    n_crises = len(crises)
    n_crises_signalled = sum(1 for v in crises.values() if v['signal_years'])

    lead_times = []
    persistence = []
    for (iso3, default_year), c in crises.items():
        if not c['signal_years']:
            continue
        # Lead time: months between the FIRST signal and the default onset.
        first_year = min(c['signal_years'])
        lead_months = MONTHS_PER_YEAR * (default_year - first_year)
        # Clamp to horizon so a "3Y signal that lands 4 years early" doesn't
        # inflate lead time; also floor at 12 so the 1Y horizon reports
        # sensible numbers.
        lo_excl_local, hi_eval_local = _horizon_windows(horizon_years)
        lead_months = min(lead_months, hi_eval_local)
        lead_times.append(lead_months)
        # Persistence: months (years * 12) of signal within pre-crisis window.
        persistence.append(MONTHS_PER_YEAR * len(c['signal_years']))

    avg_lead = sum(lead_times) / len(lead_times) if lead_times else 0.0
    avg_persist = sum(persistence) / len(persistence) if persistence else 0.0

    metrics = SignalMetrics(
        threshold=threshold,
        n_crises=n_crises, n_crises_signalled=n_crises_signalled,
        n_pre_crisis_months=n_pre_crisis_months,
        n_pre_crisis_signals=n_pre_crisis_signals,
        n_tranquil_months=n_tranquil_months,
        n_false_alarms=n_false_alarms,
    )
    return metrics, avg_lead, avg_persist


def sensitivity_table(predictions: List[Prediction],
                      horizon_years: int,
                      thresholds: Iterable[float] = (0.20, 0.30, 0.40, 0.50, 0.60)) -> List[Dict]:
    """Tellimer's Table 3 layout: one row per threshold, showing the
    trade-off between recall / false alarms / precision / lead time /
    persistence at that operational threshold."""
    total = sum(1 for p in predictions if not p.is_in_default)
    n_pre = sum(1 for p in predictions if p.is_pre_crisis and not p.is_in_default)
    base_rate = (n_pre / total) if total else 0.0

    rows: List[Dict] = []
    for t in thresholds:
        metrics, lead, persist = apply_threshold(predictions, t, horizon_years)
        rows.append(metrics.to_dict(lead, persist, base_rate))
    return rows


# ── OOS prediction generation ───────────────────────────────────────────


def _build_panel_for_horizon(horizon_years: int, years_back: int = 25):
    """Reuse fit.build_training_panel to construct the standardized
    feature panel + labels + iso_years. Returns (X, y, features, meta)."""
    from backend.credit_default import fit as cd_fit
    return cd_fit.build_training_panel(years_back, horizon_years, label_mode='state')


def _fit_and_predict_groupkfold(X, y, features, meta, n_splits: int = 5):
    """Country-level GroupKFold: fit the stacked model on each fold's
    train, return OOS predicted probabilities aligned to iso_years."""
    import numpy as np
    from sklearn.model_selection import GroupKFold
    from sklearn.ensemble import GradientBoostingClassifier
    from backend.credit_default import fit as cd_fit

    groups = meta['iso_years']['iso3'].values
    n_countries = len(set(groups))
    n_folds = min(n_splits, max(2, n_countries))
    gkf = GroupKFold(n_splits=n_folds)

    proba_all = np.full(len(X), np.nan)
    y_arr = y.values
    X_arr = X.values

    sign_vec = np.array([
        +1.0 if SCAFFOLD_HIGHER_IS_WORSE.get(f, True) else -1.0
        for f in features
    ])
    n_pos = int(y_arr.sum())
    n_neg = int(len(y_arr) - n_pos)
    scale_pos_weight = n_neg / max(1, n_pos)

    for tr, te in gkf.split(X_arr, y_arr, groups=groups):
        y_tr = y_arr[tr]
        if len(set(y_tr)) < 2 or int(y_tr.sum()) < 1:
            continue
        try:
            beta_fold, b_fold, _ = cd_fit._fit_logit_sign_constrained(
                X_arr[tr], y_tr, sign_vec,
            )
        except Exception:  # noqa: BLE001
            continue
        init_fold = cd_fit._LogitInitEstimator(coefs=beta_fold, intercept=b_fold)
        try:
            model_fold = GradientBoostingClassifier(
                n_estimators=300, max_depth=3, learning_rate=0.05,
                subsample=0.8, max_features='sqrt',
                random_state=42, min_samples_leaf=20, init=init_fold,
            )
            sw = np.where(y_tr == 1, scale_pos_weight, 1.0)
            model_fold.fit(X_arr[tr], y_tr, sample_weight=sw)
            proba_all[te] = model_fold.predict_proba(X_arr[te])[:, 1]
        except Exception as e:  # noqa: BLE001
            print(f'[backtest] groupkfold fold failed: {e}')
            continue
    return proba_all


def _fit_and_predict_walkforward(X, y, features, meta, min_train_years: int = 10):
    """Expanding-window (walk-forward): for each cutoff year, fit on
    rows with year <= cutoff, predict rows with year == cutoff + 1.
    Returns an array of OOS PDs aligned to iso_years, NaN for rows that
    aren't in any fold's test slice."""
    import numpy as np
    from sklearn.ensemble import GradientBoostingClassifier
    from backend.credit_default import fit as cd_fit

    iso_years = meta['iso_years']
    years = sorted(int(y) for y in iso_years['year'].unique())
    if len(years) < min_train_years + 1:
        return np.full(len(X), np.nan)

    proba_all = np.full(len(X), np.nan)
    y_arr = y.values
    X_arr = X.values

    sign_vec = np.array([
        +1.0 if SCAFFOLD_HIGHER_IS_WORSE.get(f, True) else -1.0
        for f in features
    ])

    for i, cutoff in enumerate(years[min_train_years - 1:-1]):
        tr = iso_years['year'].astype(int) <= cutoff
        te = iso_years['year'].astype(int) == (cutoff + 1)
        tr_idx = tr.values
        te_idx = te.values
        y_tr = y_arr[tr_idx]
        if len(set(y_tr)) < 2 or int(y_tr.sum()) < 1 or int(te_idx.sum()) < 1:
            continue

        n_pos = int(y_tr.sum())
        n_neg = int(len(y_tr) - n_pos)
        scale_pos_weight = n_neg / max(1, n_pos)

        try:
            beta_fold, b_fold, _ = cd_fit._fit_logit_sign_constrained(
                X_arr[tr_idx], y_tr, sign_vec,
            )
        except Exception:  # noqa: BLE001
            continue
        init_fold = cd_fit._LogitInitEstimator(coefs=beta_fold, intercept=b_fold)
        try:
            model_fold = GradientBoostingClassifier(
                n_estimators=300, max_depth=3, learning_rate=0.05,
                subsample=0.8, max_features='sqrt',
                random_state=42, min_samples_leaf=20, init=init_fold,
            )
            sw = np.where(y_tr == 1, scale_pos_weight, 1.0)
            model_fold.fit(X_arr[tr_idx], y_tr, sample_weight=sw)
            proba_all[te_idx] = model_fold.predict_proba(X_arr[te_idx])[:, 1]
        except Exception as e:  # noqa: BLE001
            print(f'[backtest] walkforward cutoff={cutoff} failed: {e}')
            continue
    return proba_all


# ── Public entry ────────────────────────────────────────────────────────


def backtest_horizon(horizon_years: int = 1,
                     method: str = 'groupkfold',
                     thresholds: Iterable[float] = (0.20, 0.30, 0.40, 0.50, 0.60),
                     years_back: int = 25) -> Dict:
    """Full backtest for one horizon. Returns a dict with the
    sensitivity table + fold metadata + AUC/Brier summary.

    ``method``:
      - ``groupkfold`` (default) — country-level 5-fold OOS. Fast.
      - ``walkforward``           — expanding-window OOS by cutoff year.
                                    Tighter on temporal leakage; slower.
    """
    import numpy as np
    from sklearn.metrics import roc_auc_score, brier_score_loss

    X, y, features, meta = _build_panel_for_horizon(horizon_years, years_back)

    # For 3Y and 5Y horizons, apply the discounted-hazard transform to
    # the fitted 1Y PD instead of retraining a separate model — that's
    # what the deployed rating_model does, and it's the right thing to
    # backtest against operationally.
    if horizon_years == 1:
        proba = _predict(X, y, features, meta, method)
    else:
        # Fit at 1Y, then transform each row's OOS 1Y PD to the target
        # horizon so recall / false alarms are computed on the SAME PD
        # trajectories the dashboard actually shows.
        X1, y1, features1, meta1 = _build_panel_for_horizon(1, years_back)
        proba_1y = _predict(X1, y1, features1, meta1, method)
        # Align (iso3, year) → 1Y PD, then transform.
        iso1 = meta1['iso_years'][['iso3', 'year']].astype({'iso3': str, 'year': int})
        pd_map = {}
        for i, (iso3, year) in enumerate(zip(iso1['iso3'], iso1['year'])):
            if proba_1y[i] == proba_1y[i]:  # not NaN
                pd_map[(iso3, int(year))] = proba_1y[i]

        alpha = ALPHA_3Y if horizon_years == 3 else ALPHA_5Y
        iso_h = meta['iso_years'][['iso3', 'year']].astype({'iso3': str, 'year': int})
        proba = np.full(len(iso_h), np.nan)
        for i, (iso3, year) in enumerate(zip(iso_h['iso3'], iso_h['year'])):
            p1 = pd_map.get((iso3, int(year)))
            if p1 is None or p1 != p1:
                continue
            proba[i] = 1.0 - math.pow(max(1e-9, 1.0 - float(p1)), alpha)

    # Discard rows with no OOS prediction.
    iso_years = meta['iso_years']
    valid = ~(proba != proba)  # not NaN
    iso3_arr = iso_years['iso3'].values[valid]
    year_arr = iso_years['year'].astype(int).values[valid]
    proba_v = proba[valid]

    # Bag headline AUC / Brier on the OOS set.
    y_v = y.values[valid]
    auc = float(roc_auc_score(y_v, proba_v)) if 0 < y_v.sum() < len(y_v) else None
    brier = float(brier_score_loss(y_v, proba_v)) if len(y_v) else None

    # Event-time labelling + sensitivity table.
    predictions = label_predictions(
        zip(iso3_arr.tolist(), year_arr.tolist()), proba_v.tolist(),
        horizon_years=horizon_years,
    )
    table = sensitivity_table(predictions, horizon_years, thresholds)

    return {
        'horizon_years': horizon_years,
        'method': method,
        'n_obs_oos': int(valid.sum()),
        'n_countries_oos': int(len(set(iso3_arr))),
        'auc_oos': auc,
        'brier_oos': brier,
        'unconditional_pd': round(float(y_v.mean()) if len(y_v) else 0.0, 4),
        'sensitivity_table': table,
    }


def _predict(X, y, features, meta, method: str):
    if method == 'walkforward':
        return _fit_and_predict_walkforward(X, y, features, meta)
    return _fit_and_predict_groupkfold(X, y, features, meta)
