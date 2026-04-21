"""
One-time (or occasional) build of static/data/country_aliases.json.

Sources:
  1. static/data/country_codes.json — official names + ISO codes
  2. GeoNames cities15000.zip — top cities per country by population
  3. static/data/country_aliases_overrides.json — manual additions /
     removals / disambiguation rules

Output schema (country_aliases.json):
{
  "BR": {
    "name": "Brazil",
    "aliases": ["Brazil", "Brazilian", "Brasil", ...],
    "cities": ["Brasilia", "Sao Paulo", "Rio de Janeiro"],
    "exclude_unless": {"Token": ["required_context_word", ...]}
  },
  ...
}

Run: python scripts/build_country_aliases.py
"""

import io
import json
import os
import sys
import zipfile
from collections import defaultdict

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, 'static', 'data')
CODES_PATH = os.path.join(DATA_DIR, 'country_codes.json')
OVERRIDES_PATH = os.path.join(DATA_DIR, 'country_aliases_overrides.json')
OUT_PATH = os.path.join(DATA_DIR, 'country_aliases.json')

GEONAMES_URL = 'https://download.geonames.org/export/dump/cities15000.zip'
TOP_CITIES_PER_COUNTRY = 3


def load_codes():
    with open(CODES_PATH) as f:
        data = json.load(f)
    return {c['alpha-2']: c['name'] for c in data if c.get('alpha-2')}


def load_overrides():
    if not os.path.exists(OVERRIDES_PATH):
        return {}
    with open(OVERRIDES_PATH) as f:
        return json.load(f)


def fetch_geonames_cities():
    """Download cities15000 and return {alpha2: [(name, population), ...]} sorted desc."""
    print(f'Downloading {GEONAMES_URL} ...', file=sys.stderr)
    resp = requests.get(GEONAMES_URL, timeout=60)
    resp.raise_for_status()
    zf = zipfile.ZipFile(io.BytesIO(resp.content))
    fname = 'cities15000.txt'
    cities_by_country = defaultdict(list)
    with zf.open(fname) as f:
        for raw in io.TextIOWrapper(f, encoding='utf-8'):
            parts = raw.rstrip('\n').split('\t')
            # Columns per GeoNames docs:
            # 0: geonameid, 1: name, 2: asciiname, ..., 8: country_code,
            # 14: population
            if len(parts) < 15:
                continue
            asciiname = parts[2]
            country_code = parts[8]
            try:
                pop = int(parts[14] or 0)
            except ValueError:
                pop = 0
            if not country_code or not asciiname:
                continue
            cities_by_country[country_code].append((asciiname, pop))
    for cc in cities_by_country:
        cities_by_country[cc].sort(key=lambda t: -t[1])
    return cities_by_country


def build_alias_set(name, cities, override):
    aliases = set()
    aliases.add(name)

    # GeoNames top cities
    for city_name, _pop in cities[:TOP_CITIES_PER_COUNTRY]:
        aliases.add(city_name)

    # Overrides: additions
    for token in (override or {}).get('add', []):
        aliases.add(token)

    # Overrides: removals
    for token in (override or {}).get('remove', []):
        # case-insensitive match
        aliases = {a for a in aliases if a.lower() != token.lower()}

    # Drop empty / very short tokens that cause false positives
    aliases = {a.strip() for a in aliases if a and len(a.strip()) >= 3}
    return sorted(aliases, key=lambda s: (-len(s), s.lower()))


def main():
    codes = load_codes()
    overrides = load_overrides()

    try:
        cities_by_country = fetch_geonames_cities()
    except Exception as exc:
        print(f'WARN: GeoNames fetch failed ({exc}); continuing without cities',
              file=sys.stderr)
        cities_by_country = {}

    out = {}
    for alpha2, name in codes.items():
        override = overrides.get(alpha2, {})
        cities = cities_by_country.get(alpha2, [])
        aliases = build_alias_set(name, cities, override)
        entry = {
            'name': name,
            'aliases': aliases,
            'cities': [c for c, _ in cities[:TOP_CITIES_PER_COUNTRY]],
        }
        if override.get('exclude_unless'):
            entry['exclude_unless'] = override['exclude_unless']
        out[alpha2] = entry

    with open(OUT_PATH, 'w') as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    print(f'Wrote {len(out)} countries to {OUT_PATH}', file=sys.stderr)
    print(f'Example BR: {out.get("BR")}', file=sys.stderr)
    print(f'Example SS: {out.get("SS")}', file=sys.stderr)


if __name__ == '__main__':
    main()
