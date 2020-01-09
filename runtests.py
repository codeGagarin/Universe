from connector import *
from activity import *
from unittest import TestSuite
import sys


def load_tests(loader, tests, pattern):
    suite = TestSuite()
    for test_class in (TestIntraConnector, TestPGConnector, TestLoader):
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite

if __name__ == '__main__':
    if len(sys.argv) <= 1:
        unittest.main(verbosity=2)
    elif sys.argv[1] == 'regular':
        ldr = Loader(KeyChain.TEST_LOADER_KEY)
        ldr.register(Email)
        ldr.register(LoaderStateReporter)
        ldr.register(FakeEmail)
        ldr.track_schedule()
