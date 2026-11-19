"""
Unit tests for the reducer module.

This module verifies that the reduce_sum() function correctly aggregates
a list of values associated with a key in the MapReduce pipeline.
"""

import unittest
# Import the reducer function to be tested
from src.reducer import reduce_sum


class TestReduceSum(unittest.TestCase):
    """
    Test suite for reduce_sum() function.

    The reducer takes:
        key    : a string (e.g., PassengerId or route)
        values : list of integers

    It returns:
        (key, sum(values))
    """

    def test_single_value(self):
        """
        Test reducer with a single value.
        Expected: value remains unchanged after summation.
        """
        # Input: one value → output should be the same
        self.assertEqual(reduce_sum("P001", [1]), ("P001", 1))

    def test_multiple_values(self):
        """
        Test reducer with multiple values.
        Expected: all values are summed correctly.
        """
        # Input: multiple 1s → output should be total count
        self.assertEqual(reduce_sum("P001", [1, 1, 1, 1, 1]), ("P001", 5))

    def test_pre_aggregated_values(self):
        """
        Test reducer with already aggregated values.
        (e.g., after combiner step)
        Expected: values are still summed correctly.
        """
        # Input: values already combined --> reducer should still sum them
        self.assertEqual(reduce_sum("P001", [3, 7]), ("P001", 10))

    def test_empty_values(self):
        """
        Test reducer with an empty list.
        Expected: sum([]) = 0 (no values to aggregate).
        """
        # Edge case: no values → output should be 0
        self.assertEqual(reduce_sum("P001", []), ("P001", 0))


# Entry point to run all unit tests
if __name__ == "__main__":
    unittest.main()