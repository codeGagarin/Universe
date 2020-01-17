from activity import TestLoader
from connector import TestISConnector
from connector import TestPGConnector
from report import TestReports

from unittest import TestSuite
import sys


def load_tests(loader, tests, pattern):
    suite = TestSuite()
    for test_class in (TestISConnector, TestPGConnector, TestLoader, TestReports):
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
        ldr.register(ISActualizer)
        ldr.register(ISSync)
        ldr.track_schedule()
