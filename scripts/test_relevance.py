"""
Quick sanity test for backend/scoring/relevance.py.

Runs a set of labelled (title, country_code, expect_keep) cases through
`is_relevant` and prints pass/fail plus the decision reason. Designed
specifically to probe the ambiguous cases (BR, SS, KR/KP, CD/CG, Guinea
variants, SD vs SS, etc.).

Run: python scripts/test_relevance.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.scoring.relevance import (  # noqa: E402
    is_relevant, filter_articles_for_country,
)

# (title, description, country_alpha2, expect_keep)
CASES = [
    # --- Brazil: should KEEP ---
    ('Lula signs climate pact at COP summit in Brasilia', '',
     'BR', True),
    ('Brazil central bank hikes rates amid inflation concerns', '',
     'BR', True),
    ('Petrobras announces offshore drilling plan', '',
     'BR', True),
    ('Rio de Janeiro police clash with protesters', '',
     'BR', True),
    # --- Brazil: should DROP (only tangential mentions) ---
    ('Cristiano Ronaldo scores against Brazil in friendly', '',
     'BR', False),   # mention but about Portugal/football, not country risk
    ('Amazon founder Bezos visits Seattle', '',
     'BR', False),   # no Brazil alias at all

    # --- South Sudan: should KEEP, even when Sudan is also mentioned ---
    ('Kiir meets Machar in Juba as peace deal stalls', '',
     'SS', True),
    ('South Sudanese refugees flee into Uganda', '',
     'SS', True),
    ('SPLM-IO accuses government forces of ceasefire violations', '',
     'SS', True),
    # --- South Sudan: should DROP (it is about Sudan) ---
    ('Sudan RSF shells Khartoum as civil war escalates', '',
     'SS', False),
    ('Darfur violence displaces thousands', '',
     'SS', False),

    # --- Sudan (SD): should KEEP ---
    ('Khartoum airstrikes kill civilians as Sudan war enters second year',
     '', 'SD', True),
    ('RSF and Sudanese army reach fragile truce in Darfur', '',
     'SD', True),
    # --- Sudan: should DROP (about South Sudan) ---
    ('South Sudan president Kiir reshuffles cabinet', '',
     'SD', False),

    # --- Korea disambiguation ---
    ('North Korea tests ballistic missile over Sea of Japan', '',
     'KP', True),
    ('North Korea tests ballistic missile over Sea of Japan', '',
     'KR', False),
    ('Yoon impeached by South Korean parliament', '',
     'KR', True),
    ('Yoon impeached by South Korean parliament', '',
     'KP', False),

    # --- Congo disambiguation ---
    ('DRC accuses Rwanda of backing M23 rebels in Goma', '',
     'CD', True),
    ('DRC accuses Rwanda of backing M23 rebels in Goma', '',
     'CG', False),
    ('Republic of Congo president visits Brazzaville', '',
     'CG', True),

    # --- Guinea disambiguation ---
    ('Papua New Guinea volcano displaces thousands', '',
     'PG', True),
    ('Papua New Guinea volcano displaces thousands', '',
     'GN', False),
    ('Conakry protests over fuel prices in Guinea', '',
     'GN', True),
    ('Conakry protests over fuel prices in Guinea', '',
     'GQ', False),

    # --- Georgia ambiguity ---
    ('Tbilisi protests rock Georgian capital over EU bill',
     'Saakashvili supporters clash with police', 'GE', True),
    ('Atlanta Georgia school shooting leaves three dead', '',
     'GE', False),

    # --- Taiwan ---
    ('Taiwan scrambles jets after PRC incursion near Taipei', '',
     'TW', True),
    ('Thailand floods displace thousands', '',
     'TW', False),
]


def run():
    passed = 0
    failed = 0
    for title, desc, cc, expect in CASES:
        article = {'title': title, 'description': desc}
        keep, reason = is_relevant(article, cc)
        ok = keep == expect
        status = 'PASS' if ok else 'FAIL'
        if ok:
            passed += 1
        else:
            failed += 1
        print(f'{status} {cc} keep={keep} expected={expect} '
              f'reason={reason!r} :: {title}')
    print()
    print(f'{passed} passed, {failed} failed, {len(CASES)} total')
    return 0 if failed == 0 else 1


if __name__ == '__main__':
    sys.exit(run())
