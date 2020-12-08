"""
    Activity manual run utils
"""
from datetime import datetime, timedelta
from keys import KeyChain

print('Version 1')  # interactive import version control


def rollup_manual_run(yy_mm_dd: tuple, delta_hour: int, base1s: str = 'vgunf'):
    from lib.pg_starter import PGStarter
    from lib.datarollup import CounterLinesRoll
    starter = PGStarter(KeyChain.PG_STARTER_KEY)
    a = CounterLinesRoll(starter)
    a['from'] = datetime(2000+yy_mm_dd[0], yy_mm_dd[1], yy_mm_dd[2])
    a['to'] = a['from'] + timedelta(hours=delta_hour)
    a['base1s'] = base1s
    a.apply()
