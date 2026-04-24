"""
Per-country configuration: rollout priority, scoring weight overrides,
display names, data-source preferences.
"""

# Rollout order — controls /api/country-risk/countries response and UI sort.
PRIORITY_ORDER = [
    'US', 'GB',              # Phase 1 & 2 primary
    'DE', 'FR', 'IT', 'ES',  # EU Big-4 (Phase 2)
    'EU',                    # EU-27 aggregate (Phase 2)
    'MX', 'CO', 'BR', 'CL', 'AR',  # LatAm (Phase 4)
]

DISPLAY_NAMES = {
    'US': 'United States',
    'GB': 'United Kingdom',
    'DE': 'Germany',
    'FR': 'France',
    'IT': 'Italy',
    'ES': 'Spain',
    'EU': 'European Union (GDP-wtd)',
    'MX': 'Mexico',
    'CO': 'Colombia',
    'BR': 'Brazil',
    'CL': 'Chile',
    'AR': 'Argentina',
}

# Default sub-score weights. Must sum to 1.0.
DEFAULT_WEIGHTS = {
    'structural': 0.30,
    'macro':      0.40,
    'labor':      0.30,
}

# Per-country overrides. Must sum to 1.0 when all three keys are present.
COUNTRY_WEIGHTS = {
    # Argentina: INDEC labor data is noisy; downweight labor, upweight macro.
    'AR': {'structural': 0.30, 'macro': 0.55, 'labor': 0.15},
}


# Source preference for youth unemployment: first available wins.
# Phase 1 only exercises 'fred'. Other paths are wired in later phases.
YOUTH_UNEMP_SOURCES = {
    'US': ['fred'],
    'GB': ['ons', 'ilo'],
    'DE': ['eurostat', 'ilo'],
    'FR': ['eurostat', 'ilo'],
    'IT': ['eurostat', 'ilo'],
    'ES': ['eurostat', 'ilo'],
    'MX': ['ilo'],
    'CO': ['ilo'],
    'BR': ['ilo'],
    'CL': ['ilo'],
    'AR': ['ilo'],
}

# FRED series IDs for US labor data.
FRED_SERIES = {
    'youth_unemp_rate': 'LNS14024887',   # Unemployment rate, 16-24, SA, monthly
    'total_unemp_rate': 'UNRATE',        # Total civilian unemployment, SA, monthly
}


def get_weights(country_code: str):
    """Return scoring weights for a country, falling back to defaults."""
    return COUNTRY_WEIGHTS.get(country_code, DEFAULT_WEIGHTS)


def is_supported(country_code: str) -> bool:
    return country_code.upper() in PRIORITY_ORDER
