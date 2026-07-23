import os
import sys
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "speed"))
from consumer import SlidingWindow, hotspot_flags


class TestWindow(unittest.TestCase):
    def test_eviction(self):
        w = SlidingWindow(window_s=1, bucket_s=1)
        w.add("a")
        time.sleep(1.2)
        w.add("b")
        top = w.top(5)
        regions = [r[0] for r in top]
        self.assertIn("b", regions)
        self.assertNotIn("a", regions)

    def test_topn(self):
        w = SlidingWindow(window_s=60, bucket_s=10)
        for _ in range(5):
            w.add("x", 3.0)
        for _ in range(2):
            w.add("y", 4.0)
        w.add("z", 5.0)
        top = w.top(2)
        self.assertEqual(len(top), 2)
        self.assertEqual(top[0][0], "x")
        self.assertEqual(top[0][1], 5)
        self.assertEqual(top[1][0], "y")

    def test_hotspot(self):
        top = [("quiet", 1, 2.0), ("spike", 50, 4.0), ("mid", 2, 3.0)]
        flags = hotspot_flags(top)
        self.assertTrue(flags.get("spike"))
        self.assertFalse(flags.get("quiet"))


if __name__ == "__main__":
    unittest.main()
