from activity import *

ldr = Loader(KeyChain.LOADER_KEY)
ldr.register(Email)
ldr.register(LoaderStateReporter)
# ldr.register(FakeEmail)
ldr.track_schedule()