import unittest

from experiments.faults import scenario


class FaultExperimentTests(unittest.TestCase):
    def test_effect_commit_before_crash_is_duplicated_without_unique_key(self):
        result = scenario(False)
        self.assertEqual(result['side_effects'], 2)
        self.assertEqual(result['executions'], 2)

    def test_external_unique_key_survives_crash_and_retry(self):
        result = scenario(True)
        self.assertEqual(result['side_effects'], 1)
        self.assertEqual(result['final_state'], 'done')
