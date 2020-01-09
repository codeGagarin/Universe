from activity import *

ldr = Loader(KeyChain.LOADER_KEY)
ldr.get_PG_connector = lambda: PGConnector(KeyChain.PG_KEY)
ldr.get_IS_connector = lambda: ISConnector(KeyChain.IS_KEY)
ldr.register(Email)
ldr.register(LoaderStateReporter)
ldr.register(ISActualizer)
ldr.register(ISSync)
ldr.track_schedule()
