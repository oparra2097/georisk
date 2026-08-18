"""Seed the portfolio: the three complexes, plus the Nest unit roster.

Seeding is idempotent — it only inserts what is missing, and it never
overwrites pricing or unit edits made later in the manager UI. Rents are
deliberately seeded at $0 ("unpriced"): the unit mix is known, the rent roll
is not, so pricing is set in the manager UI before a unit can be leased.
"""

from __future__ import annotations

import logging

from backend.rent.db import get_db

logger = logging.getLogger(__name__)

PROPERTIES = [
    {
        'slug': 'nest',
        'name': 'Nest',
        'city': '',
        'state': 'TX',
        'amenities': 'In-unit washer/dryer, Trash included',
        'notes': '64 units: 5 x 1bd/1ba, 59 x 2bd/2ba (~1,080 sqft).',
    },
    {
        'slug': 'old-port-isabel',
        'name': 'Old Port Isabel',
        'city': 'Port Isabel',
        'state': 'TX',
        'amenities': '',
        'notes': 'Unit roster pending — add units in the manager portal.',
    },
    {
        'slug': 'vermillion',
        'name': 'Vermillion',
        'city': '',
        'state': 'TX',
        'amenities': '',
        'notes': 'Unit roster pending — add units in the manager portal.',
    },
]

# Nest: four floors of sixteen. The five 1bd/1ba units are 101-105; every
# other unit is a 2bd/2ba at roughly 1,080 sqft.
NEST_FLOORS = (1, 2, 3, 4)
NEST_UNITS_PER_FLOOR = 16
NEST_ONE_BED_LABELS = {'101', '102', '103', '104', '105'}
NEST_TWO_BED_SQFT = 1080


def nest_unit_roster() -> list[dict]:
    roster = []
    for floor in NEST_FLOORS:
        for n in range(1, NEST_UNITS_PER_FLOOR + 1):
            label = f'{floor}{n:02d}'
            one_bed = label in NEST_ONE_BED_LABELS
            roster.append({
                'label': label,
                'bedrooms': 1 if one_bed else 2,
                'bathrooms': 1.0 if one_bed else 2.0,
                'sqft': None if one_bed else NEST_TWO_BED_SQFT,
            })
    return roster


def seed_portfolio() -> dict:
    """Insert any missing properties and the Nest unit roster."""
    conn = get_db()
    created = {'properties': 0, 'units': 0}
    try:
        for prop in PROPERTIES:
            cur = conn.execute(
                'INSERT INTO properties (slug, name, city, state, amenities, notes) '
                'VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(slug) DO NOTHING',
                (prop['slug'], prop['name'], prop['city'], prop['state'],
                 prop['amenities'], prop['notes']))
            created['properties'] += cur.rowcount if cur.rowcount > 0 else 0

        row = conn.execute("SELECT id FROM properties WHERE slug = 'nest'").fetchone()
        if row:
            nest_id = row['id']
            for unit in nest_unit_roster():
                cur = conn.execute(
                    'INSERT INTO units (property_id, label, bedrooms, bathrooms, sqft, '
                    'market_rent_cents, status) VALUES (?, ?, ?, ?, ?, 0, ?) '
                    'ON CONFLICT(property_id, label) DO NOTHING',
                    (nest_id, unit['label'], unit['bedrooms'], unit['bathrooms'],
                     unit['sqft'], 'vacant'))
                created['units'] += cur.rowcount if cur.rowcount > 0 else 0
        conn.commit()
    finally:
        conn.close()

    if created['properties'] or created['units']:
        logger.info("Rent seed: +%d properties, +%d units",
                    created['properties'], created['units'])
    return created
