import json
import os

_base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_data_dir = os.path.join(_base_dir, 'static', 'data')

_countries = []
_fips_to_iso = {}
_alpha2_to_numeric = {}
_numeric_to_alpha2 = {}
_alpha2_to_name = {}
_alpha2_to_fips = {}
_loaded = False


def _load():
    global _countries, _fips_to_iso, _alpha2_to_numeric, _numeric_to_alpha2
    global _alpha2_to_name, _alpha2_to_fips, _loaded

    if _loaded:
        return

    with open(os.path.join(_data_dir, 'country_codes.json'), 'r') as f:
        _countries = json.load(f)

    with open(os.path.join(_data_dir, 'fips_to_iso.json'), 'r') as f:
        _fips_to_iso = json.load(f)

    for c in _countries:
        a2 = c.get('alpha-2', '')
        num = c.get('country-code', '')
        name = c.get('name', '')
        if a2:
            _alpha2_to_numeric[a2] = num
            _alpha2_to_name[a2] = name
        if num and num != '-99':
            _numeric_to_alpha2[num] = a2

    for fips, iso in _fips_to_iso.items():
        _alpha2_to_fips[iso] = fips

    _loaded = True


def iso_alpha2_to_numeric(alpha2: str) -> str:
    _load()
    return _alpha2_to_numeric.get(alpha2.upper(), '')


def numeric_to_iso_alpha2(numeric: str) -> str:
    _load()
    key = str(int(numeric)) if numeric.isdigit() else numeric
    padded = numeric.zfill(3)
    return _numeric_to_alpha2.get(padded, _numeric_to_alpha2.get(key, ''))


def iso_alpha2_to_fips(alpha2: str) -> str:
    _load()
    return _alpha2_to_fips.get(alpha2.upper(), alpha2.upper())


def fips_to_iso_alpha2(fips: str) -> str:
    _load()
    return _fips_to_iso.get(fips.upper(), '')


def iso_alpha2_to_name(alpha2: str) -> str:
    _load()
    return _alpha2_to_name.get(alpha2.upper(), alpha2)


def get_all_country_codes():
    _load()
    return list(_alpha2_to_name.keys())


def get_all_countries():
    _load()
    return list(_countries)
