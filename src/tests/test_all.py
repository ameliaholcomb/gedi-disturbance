import unittest

from src.tests import test_radd_overlay
from src.tests import test_jrc_overlay
from src.tests import test_jrc_intensity_overlay

suites = []
suites.append(test_radd_overlay.suite)
suites.append(test_jrc_overlay.suite)
suites.append(test_jrc_intensity_overlay.suite)

suite = unittest.TestSuite(suites)

if __name__ == "__main__":  # pragma: no cover
    unittest.TextTestRunner(verbosity=2).run(suite)
