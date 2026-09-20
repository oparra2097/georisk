"""
EM rates panel config — LATAM top 6 + Asia top 6.

Single source of truth for the /em-rates page and the fitted Taylor
rule per country. Each entry names the central bank, the policy rate,
the country's inflation target, a neutral real rate estimate (from
published IMF / BIS papers, refresh annually), and the data-source
tickers used by ``em_policy_rates.py`` and ``em_yields.py``.

**Neutral real rate sources** — these are point estimates from
publicly available IMF / central-bank staff papers, current as of
2026. They enter the Taylor rule via the constant
``neutral_nominal = neutral_r + target_cpi`` when the OLS fit is
under-identified (< 60 obs or unstable ρ). Otherwise the constant is
estimated. Refresh yearly.

**IMF IFS series codes** — see IMF Data Portal. Two most-used per
country: policy rate (``FPOLM_PA`` variant) and CPI index
(``PCPI_IX``). Some EM central banks have non-standard series names;
see the ``ifs_policy_note`` field.

**Yahoo 10Y tickers** — best-effort. Some (BR10YT=X, IN10YT=X) work,
some (COP, PEN, ARG) don't. FRED OECD long-term rate series
(``IRLTLT01<ISO2>M156N``) is the fallback where Yahoo returns nothing.

**Central bank meeting dates** live in ``em_meetings.py`` — refresh
yearly against each CB's published calendar.
"""

from __future__ import annotations

# ── LATAM top 6 ────────────────────────────────────────────────────────

_LATAM = {
    'BRA': {
        'name': 'Brazil', 'iso2': 'BR', 'ccy': 'BRL',
        'cb_name': 'BCB', 'cb_url': 'https://www.bcb.gov.br',
        'rate_name': 'SELIC target',
        'target_cpi': 3.0,        # 3.0% ±1.5% (2026 target)
        'neutral_r': 4.5,         # BCB / IMF WP 2024 estimate
        'yahoo_10y': 'BR10YT=X',
        'fred_10y': 'IRLTLT01BRM156N',
        'ifs_policy': 'FIPR_PA', 'ifs_cpi': 'PCPI_IX',
    },
    'MEX': {
        'name': 'Mexico', 'iso2': 'MX', 'ccy': 'MXN',
        'cb_name': 'Banxico', 'cb_url': 'https://www.banxico.org.mx',
        'rate_name': 'Target overnight rate',
        'target_cpi': 3.0,        # 3% ±1% (permanent)
        'neutral_r': 2.5,
        'yahoo_10y': 'MX10YT=X',
        'fred_10y': 'IRLTLT01MXM156N',
        'ifs_policy': 'FIPR_PA', 'ifs_cpi': 'PCPI_IX',
    },
    'COL': {
        'name': 'Colombia', 'iso2': 'CO', 'ccy': 'COP',
        'cb_name': 'BanRep', 'cb_url': 'https://www.banrep.gov.co',
        'rate_name': 'Monetary policy rate',
        'target_cpi': 3.0,        # 3% ±1%
        'neutral_r': 2.5,
        'yahoo_10y': 'CO10YT=X',
        'fred_10y': 'IRLTLT01COM156N',
        'ifs_policy': 'FIPR_PA', 'ifs_cpi': 'PCPI_IX',
    },
    'CHL': {
        'name': 'Chile', 'iso2': 'CL', 'ccy': 'CLP',
        'cb_name': 'BCCh', 'cb_url': 'https://www.bcentral.cl',
        'rate_name': 'TPM (Tasa de Política Monetaria)',
        'target_cpi': 3.0,        # 3% ±1%
        'neutral_r': 1.5,         # BCCh estimate 2024
        'yahoo_10y': 'CL10YT=X',
        'fred_10y': 'IRLTLT01CLM156N',
        'ifs_policy': 'FIPR_PA', 'ifs_cpi': 'PCPI_IX',
    },
    'PER': {
        'name': 'Peru', 'iso2': 'PE', 'ccy': 'PEN',
        'cb_name': 'BCRP', 'cb_url': 'https://www.bcrp.gob.pe',
        'rate_name': 'Reference rate',
        'target_cpi': 2.0,        # 2% ±1%
        'neutral_r': 1.5,
        'yahoo_10y': 'PE10YT=X',
        'fred_10y': None,          # OECD doesn't publish PE
        'ifs_policy': 'FIPR_PA', 'ifs_cpi': 'PCPI_IX',
    },
    'ARG': {
        'name': 'Argentina', 'iso2': 'AR', 'ccy': 'ARS',
        'cb_name': 'BCRA', 'cb_url': 'https://www.bcra.gob.ar',
        'rate_name': 'LELIQ / Monetary policy rate',
        'target_cpi': None,       # No formal target as of 2026; regime shift
        'neutral_r': 6.0,         # Very high given persistent inflation
        'yahoo_10y': 'AR10YT=X',
        'fred_10y': None,
        'ifs_policy': 'FIPR_PA', 'ifs_cpi': 'PCPI_IX',
        'note': 'no formal inflation target; use realized-CPI anchor',
    },
}

# ── Asia top 6 ─────────────────────────────────────────────────────────

_ASIA = {
    'CHN': {
        'name': 'China', 'iso2': 'CN', 'ccy': 'CNY',
        'cb_name': 'PBoC', 'cb_url': 'http://www.pbc.gov.cn',
        'rate_name': '7-day reverse repo rate',
        'target_cpi': 3.0,        # informal ceiling, not formal
        'neutral_r': 1.0,
        'yahoo_10y': 'CN10YT=X',
        'fred_10y': 'IRLTLT01CNM156N',
        'ifs_policy': 'FIPR_PA', 'ifs_cpi': 'PCPI_IX',
    },
    'IND': {
        'name': 'India', 'iso2': 'IN', 'ccy': 'INR',
        'cb_name': 'RBI', 'cb_url': 'https://www.rbi.org.in',
        'rate_name': 'Repo rate',
        'target_cpi': 4.0,        # 4% ±2%
        'neutral_r': 1.5,
        'yahoo_10y': 'IN10YT=X',
        'fred_10y': 'IRLTLT01INM156N',
        'ifs_policy': 'FIPR_PA', 'ifs_cpi': 'PCPI_IX',
    },
    'KOR': {
        'name': 'South Korea', 'iso2': 'KR', 'ccy': 'KRW',
        'cb_name': 'BOK', 'cb_url': 'https://www.bok.or.kr',
        'rate_name': 'Base rate',
        'target_cpi': 2.0,
        'neutral_r': 0.5,
        'yahoo_10y': 'KR10YT=X',
        'fred_10y': 'IRLTLT01KRM156N',
        'ifs_policy': 'FIPR_PA', 'ifs_cpi': 'PCPI_IX',
    },
    'IDN': {
        'name': 'Indonesia', 'iso2': 'ID', 'ccy': 'IDR',
        'cb_name': 'BI', 'cb_url': 'https://www.bi.go.id',
        'rate_name': 'BI-Rate',
        'target_cpi': 2.5,        # 2.5% ±1%
        'neutral_r': 2.0,
        'yahoo_10y': 'ID10YT=X',
        'fred_10y': None,
        'ifs_policy': 'FIPR_PA', 'ifs_cpi': 'PCPI_IX',
    },
    'THA': {
        'name': 'Thailand', 'iso2': 'TH', 'ccy': 'THB',
        'cb_name': 'BOT', 'cb_url': 'https://www.bot.or.th',
        'rate_name': 'Policy rate',
        'target_cpi': 2.0,        # 1-3% range
        'neutral_r': 1.0,
        'yahoo_10y': 'TH10YT=X',
        'fred_10y': None,
        'ifs_policy': 'FIPR_PA', 'ifs_cpi': 'PCPI_IX',
    },
    'PHL': {
        'name': 'Philippines', 'iso2': 'PH', 'ccy': 'PHP',
        'cb_name': 'BSP', 'cb_url': 'https://www.bsp.gov.ph',
        'rate_name': 'Overnight reverse repurchase rate',
        'target_cpi': 3.0,        # 2-4%
        'neutral_r': 2.0,
        'yahoo_10y': 'PH10YT=X',
        'fred_10y': None,
        'ifs_policy': 'FIPR_PA', 'ifs_cpi': 'PCPI_IX',
    },
}


COUNTRIES: dict[str, dict] = {**_LATAM, **_ASIA}
LATAM_ISO3 = list(_LATAM.keys())
ASIA_ISO3 = list(_ASIA.keys())
ALL_ISO3 = LATAM_ISO3 + ASIA_ISO3


def get_country(iso3: str) -> dict | None:
    """Return the config dict for one ISO3, or None if not covered."""
    return COUNTRIES.get(iso3.upper())


def region_of(iso3: str) -> str | None:
    if iso3 in LATAM_ISO3:
        return 'LATAM'
    if iso3 in ASIA_ISO3:
        return 'Asia'
    return None
