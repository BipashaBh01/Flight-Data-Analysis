"""
Unit tests for the combiner module.

Verifies that the combiner correctly performs local pre-aggregation
of (key, value) pairs within a single mapper's output before the
shuffle phase, reducing the volume of intermediate data.
"""

import unittest
from src.combiner import combine


class TestCombine(unittest.TestCase):
    """
    Test suite for combine() function.

    The combiner takes a list of (key, value) tuples from one mapper
    and locally sums values per key, returning a compacted list.
    """

    def test_aggregates_duplicate_keys(self):
        """
        Duplicate keys within the same chunk should be summed.
        """
        pairs = [("P001", 1), ("P002", 1), ("P001", 1), ("P001", 1)]
        result = dict(combine(pairs))

        # P001 appears 3 times → combined value should be 3
        self.assertEqual(result["P001"], 3)
        self.assertEqual(result["P002"], 1)

    def test_single_key(self):
        """
        A single-key input should pass through unchanged.
        """
        pairs = [("P005", 1)]
        result = dict(combine(pairs))

        self.assertEqual(result["P005"], 1)
        self.assertEqual(len(result), 1)

    def test_no_duplicates(self):
        """
        When all keys are unique, no aggregation occurs and the
        output length matches the input length.
        """
        pairs = [("A", 1), ("B", 1), ("C", 1)]
        result = dict(combine(pairs))

        self.assertEqual(len(result), 3)
        self.assertTrue(all(v == 1 for v in result.values()))

    def test_empty_input(self):
        """
        An empty input should return an empty list.
        """
        result = combine([])
        self.assertEqual(result, [])

    def test_preserves_total_count(self):
        """
        The sum of all values after combining must equal the sum
        of all values before combining (no data lost or gained).
        """
        pairs = [("X", 1), ("Y", 1), ("X", 1), ("Y", 1), ("X", 1)]
        input_total = sum(v for _, v in pairs)
        output_total = sum(v for _, v in combine(pairs))

        self.assertEqual(input_total, output_total)

    def test_returns_list_of_tuples(self):
        """
        Output format must be a list of (key, value) tuples — the same
        type the shuffle step expects from raw mapper output.
        """
        pairs = [("K", 1), ("K", 1)]
        result = combine(pairs)

        self.assertIsInstance(result, list)
        for item in result:
            self.assertIsInstance(item, tuple)
            self.assertEqual(len(item), 2)


if __name__ == "__main__":
    unittest.main()
