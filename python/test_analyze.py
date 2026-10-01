import unittest

from analyze import detect


class DetectTests(unittest.TestCase):
    def test_sustained_increase(self):
        self.assertEqual(detect([40] * 20 + [220] * 5), (40.0, 220.0))

    def test_single_spike_is_ignored(self):
        self.assertIsNone(detect([40] * 20 + [40, 40, 500, 40, 40]))

    def test_needs_baseline(self):
        self.assertIsNone(detect([40] * 24))


if __name__ == "__main__":
    unittest.main()
