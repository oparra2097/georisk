"""
Active Conflict Registry.

Provides floor scores for countries with known ongoing armed conflicts.
Tiers are based on the Wikipedia "List of ongoing armed conflicts" classification:
  - MAJOR_WAR:      10,000+ fatalities/year, active large-scale combat
  - WAR:            1,000-9,999 fatalities/year, sustained armed conflict
  - MINOR_CONFLICT: 100-999 fatalities/year, active but limited
  - SKIRMISHES:     <100 fatalities/year, low-intensity

Floor scores represent the MINIMUM a country should score on relevant indicators,
regardless of what GDELT returns on any given day. They are NOT ceilings --
the news-reactive system can push scores higher.

Updated manually. Check quarterly against ACLED/Wikipedia.
"""

# ─── TIER DEFINITIONS ────────────────────────────────────────────────
# Each tier provides floor scores per indicator + a composite floor.
# economic_sanctions intentionally excluded (news-reactive only).

CONFLICT_TIERS = {
    'MAJOR_WAR': {
        'military_conflict': 85.0,
        'political_stability': 70.0,
        'terrorism': 50.0,
        'protests_civil_unrest': 45.0,
        'diplomatic_tensions': 55.0,
        'composite_floor': 75.0,
    },
    'WAR': {
        'military_conflict': 70.0,
        'political_stability': 60.0,
        'terrorism': 40.0,
        'protests_civil_unrest': 40.0,
        'diplomatic_tensions': 45.0,
        'composite_floor': 60.0,
    },
    'MINOR_CONFLICT': {
        'military_conflict': 50.0,
        'political_stability': 45.0,
        'terrorism': 30.0,
        'protests_civil_unrest': 30.0,
        'diplomatic_tensions': 30.0,
        'composite_floor': 45.0,
    },
    'SKIRMISHES': {
        'military_conflict': 35.0,
        'political_stability': 30.0,
        'terrorism': 20.0,
        'protests_civil_unrest': 20.0,
        'diplomatic_tensions': 20.0,
        'composite_floor': 30.0,
    },
}


# ─── ACTIVE CONFLICTS REGISTRY ──────────────────────────────────────
# country_code -> {tier, conflict, parties, since, updated}

ACTIVE_CONFLICTS = {
    # --- MAJOR WARS (10,000+ deaths/year, active large-scale combat) ---
    'UA': {
        'tier': 'MAJOR_WAR',
        'conflict': 'Russo-Ukrainian War',
        'parties': ['Russia', 'Ukraine'],
        'since': '2022-02-24',
        'updated': '2025-01-01',
    },
    'PS': {
        'tier': 'MAJOR_WAR',
        'conflict': 'Israel-Gaza War',
        'parties': ['Israel', 'Hamas/Palestinian factions'],
        'since': '2023-10-07',
        'updated': '2025-01-01',
    },
    'SD': {
        'tier': 'MAJOR_WAR',
        'conflict': 'Sudanese Civil War',
        'parties': ['SAF', 'RSF'],
        'since': '2023-04-15',
        'updated': '2025-01-01',
    },
    'MM': {
        'tier': 'MAJOR_WAR',
        'conflict': 'Myanmar Civil War',
        'parties': ['Military junta (Tatmadaw)', 'NUG/EAOs'],
        'since': '2021-02-01',
        'updated': '2025-01-01',
    },

    # --- WARS (1,000-9,999 deaths/year, sustained armed conflict) ---
    'SY': {
        'tier': 'WAR',
        'conflict': 'Syrian Civil War (ongoing)',
        'parties': ['Multiple factions'],
        'since': '2011-03-15',
        'updated': '2025-01-01',
    },
    'YE': {
        'tier': 'WAR',
        'conflict': 'Yemeni Civil War / Houthi conflict',
        'parties': ['Houthis', 'Saudi coalition', 'STC'],
        'since': '2014-09-01',
        'updated': '2025-01-01',
    },
    'IL': {
        'tier': 'WAR',
        'conflict': 'Israel-Gaza War (belligerent)',
        'parties': ['IDF', 'Hamas'],
        'since': '2023-10-07',
        'updated': '2025-01-01',
    },
    'RU': {
        'tier': 'WAR',
        'conflict': 'Russo-Ukrainian War (belligerent)',
        'parties': ['Russia'],
        'since': '2022-02-24',
        'updated': '2025-01-01',
    },
    'IR': {
        'tier': 'WAR',
        'conflict': 'Israel-Iran conflict / proxy network escalation',
        'parties': ['IRGC', 'Israel', 'Hezbollah', 'Houthis', 'US'],
        'since': '2024-04-13',
        'updated': '2026-04-01',
    },
    'LB': {
        'tier': 'WAR',
        'conflict': 'Israel-Hezbollah conflict',
        'parties': ['Hezbollah', 'IDF'],
        'since': '2023-10-08',
        'updated': '2026-04-01',
    },

    # --- MINOR CONFLICTS (100-999 deaths/year) ---
    'SO': {
        'tier': 'MINOR_CONFLICT',
        'conflict': 'Somali Civil War / Al-Shabaab insurgency',
        'parties': ['Federal govt', 'Al-Shabaab'],
        'since': '2009-01-01',
        'updated': '2025-01-01',
    },
    'ET': {
        'tier': 'MINOR_CONFLICT',
        'conflict': 'Ethiopian internal conflicts',
        'parties': ['Federal govt', 'Regional forces'],
        'since': '2020-11-04',
        'updated': '2025-01-01',
    },
    'CD': {
        'tier': 'MINOR_CONFLICT',
        'conflict': 'Eastern Congo conflict (M23)',
        'parties': ['DRC army', 'M23', 'ADF'],
        'since': '2022-01-01',
        'updated': '2025-01-01',
    },
    'ML': {
        'tier': 'MINOR_CONFLICT',
        'conflict': 'Mali insurgency',
        'parties': ['Junta', 'JNIM', 'Tuareg'],
        'since': '2012-01-01',
        'updated': '2025-01-01',
    },
    'BF': {
        'tier': 'MINOR_CONFLICT',
        'conflict': 'Burkina Faso insurgency',
        'parties': ['Junta', 'JNIM/ISGS'],
        'since': '2015-01-01',
        'updated': '2025-01-01',
    },
    'NG': {
        'tier': 'MINOR_CONFLICT',
        'conflict': 'Boko Haram / banditry',
        'parties': ['Federal govt', 'Boko Haram', 'ISWAP', 'bandits'],
        'since': '2009-01-01',
        'updated': '2025-01-01',
    },
    'SS': {
        'tier': 'MINOR_CONFLICT',
        'conflict': 'South Sudan civil conflict',
        'parties': ['Multiple factions'],
        'since': '2013-12-15',
        'updated': '2025-01-01',
    },
    'MZ': {
        'tier': 'MINOR_CONFLICT',
        'conflict': 'Cabo Delgado insurgency',
        'parties': ['Mozambique govt', 'IS-Mozambique', 'SAMIM'],
        'since': '2017-10-05',
        'updated': '2026-04-01',
    },
    'MX': {
        'tier': 'MINOR_CONFLICT',
        'conflict': 'Mexican drug war',
        'parties': ['Federal govt', 'CJNG', 'Sinaloa cartel', 'others'],
        'since': '2006-12-11',
        'updated': '2026-04-01',
    },
    'CO': {
        'tier': 'MINOR_CONFLICT',
        'conflict': 'ELN / FARC dissident insurgency',
        'parties': ['Colombian govt', 'ELN', 'FARC-EP dissidents', 'Clan del Golfo'],
        'since': '1964-05-27',
        'updated': '2026-04-01',
    },
    'CM': {
        'tier': 'MINOR_CONFLICT',
        'conflict': 'Anglophone Crisis / Boko Haram spillover',
        'parties': ['Cameroon govt', 'Ambazonia forces', 'Boko Haram'],
        'since': '2017-10-01',
        'updated': '2026-04-01',
    },

    # --- SKIRMISHES (<100 deaths/year, low-intensity) ---
    'IQ': {
        'tier': 'SKIRMISHES',
        'conflict': 'Iraqi insurgency remnants',
        'parties': ['Iraqi govt', 'ISIS remnants'],
        'since': '2017-01-01',
        'updated': '2025-01-01',
    },
    'PK': {
        'tier': 'SKIRMISHES',
        'conflict': 'TTP insurgency',
        'parties': ['Pakistan army', 'TTP'],
        'since': '2004-01-01',
        'updated': '2025-01-01',
    },
    'LY': {
        'tier': 'SKIRMISHES',
        'conflict': 'Libyan instability',
        'parties': ['GNA', 'LNA'],
        'since': '2014-01-01',
        'updated': '2025-01-01',
    },
}


def get_conflict_floors(country_code):
    """
    Get floor scores for a country based on active conflict status.
    Returns dict of {indicator_name: floor_value, 'composite_floor': value}
    or None if no active conflict registered.
    """
    entry = ACTIVE_CONFLICTS.get(country_code)
    if not entry:
        return None
    tier = entry['tier']
    return CONFLICT_TIERS.get(tier)


def get_conflict_info(country_code):
    """Return conflict metadata for a country, or None."""
    return ACTIVE_CONFLICTS.get(country_code)


def is_active_conflict(country_code):
    """Check if a country has a registered active conflict."""
    return country_code in ACTIVE_CONFLICTS
