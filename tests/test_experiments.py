import unittest

from experiments.benchmark import percentile
from experiments.model_check import explore


class ExperimentTests(unittest.TestCase):
    def test_percentile_interpolation(self):
        self.assertEqual(percentile([1, 2, 3, 4], .5), 2.5)
        self.assertEqual(percentile([7], .99), 7)

    def test_fencing_model_and_negative_control(self):
        self.assertTrue(explore(True)['safe'])
        broken = explore(False)
        self.assertFalse(broken['safe'])
        self.assertIn('deadline expires', broken['counterexample'])
        unfenced = explore(False, check_expiry=True)
        self.assertFalse(unfenced['safe'])
        self.assertIn('claim token-2', unfenced['counterexample'])


if __name__ == '__main__':
    unittest.main()
