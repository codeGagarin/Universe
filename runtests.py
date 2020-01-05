from connector import TestIntraConnector, TestPGConnector
from activity import TestLoader
from unittest import TestSuite

def load_tests(loader, tests, pattern):
    suite = TestSuite()
    for test_class in (TestIntraConnector, TestPGConnector, TestLoader):
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite

if __name__ == '__main__':
    unittest.main(verbosity=2)