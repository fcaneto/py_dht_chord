import unittest

from ..util import Interval

class IsKeyInIntervalTest(unittest.TestCase):

    def test_regular(self):
        interval = Interval(start=2, end=5, circle_size=10)
        self.assertTrue(5 in interval)
        self.assertFalse(1 in interval)
        self.assertFalse(6 in interval)

    def test_boundaries(self):
        interval = Interval(start=3, end=5, circle_size=10)
        self.assertTrue(3 in interval)
        self.assertTrue(5 in interval)

    def test_cyclic(self):
        interval = Interval(start=7, end=5, circle_size=10)
        self.assertTrue(7 in interval)
        self.assertTrue(5 in interval)
        self.assertTrue(10 in interval)
        self.assertTrue(1 in interval)
        self.assertFalse(6 in interval)

    def test_whole_circle(self):
        interval = Interval(start=1, end=1, circle_size=10)
        self.assertTrue(1 in interval)
        self.assertTrue(5 in interval)

    def test_closed_on_left(self):
        interval = Interval(start=2, end=3, circle_size=10, closed_on_left=True, closed_on_right=False)
        self.assertTrue(2 in interval)
        self.assertFalse(3 in interval)

    def test_closed_on_right(self):
        interval = Interval(start=2, end=3, circle_size=10, closed_on_left=False, closed_on_right=True)
        self.assertFalse(2 in interval)
        self.assertTrue(3 in interval)

