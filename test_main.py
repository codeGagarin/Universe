from lib.pg_starter import PGStarter
from keys import KeyChain

starter = PGStarter(KeyChain.PG_STARTER_KEY)
starter.track_schedule()
