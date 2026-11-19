"""
Unit tests for the partitioner module.

Verifies that the hash-based partitioner correctly distributes keys
across reducer buckets without data loss or duplication.
"""

import unittest
from src.partitioner import hash_partition


class TestHashPartition(unittest.TestCase):
    """
    Test suite for hash_partition() function.

    The partitioner assigns each key to exactly one bucket using a
    deterministic hash function, ensuring even reducer load distribution.
    """

    def test_all_keys_preserved(self):
        """
        Every key from the input must appear in exactly one partition.
        No keys should be lost or duplicated.
        """
        grouped = {"A": [1, 1], "B": [1], "C": [1, 1, 1], "D": [1]}
        partitions = hash_partition(grouped, num_partitions=2)

        # Collect all keys across all partitions
        all_keys = set()
        for part in partitions:
            all_keys.update(part.keys())

        self.assertEqual(all_keys, {"A", "B", "C", "D"})

    def test_no_key_duplication(self):
        """
        A key must not appear in more than one partition.
        """
        grouped = {"X": [1], "Y": [2], "Z": [3]}
        partitions = hash_partition(grouped, num_partitions=3)

        seen = []
        for part in partitions:
            for key in part:
                self.assertNotIn(key, seen, f"Key '{key}' found in multiple partitions")
                seen.append(key)

    def test_correct_number_of_partitions(self):
        """
        The output must contain exactly num_partitions buckets.
        """
        grouped = {"A": [1], "B": [1]}
        partitions = hash_partition(grouped, num_partitions=5)

        self.assertEqual(len(partitions), 5)

    def test_single_partition(self):
        """
        With num_partitions=1, all keys land in the same (only) bucket.
        """
        grouped = {"K1": [1], "K2": [2], "K3": [3]}
        partitions = hash_partition(grouped, num_partitions=1)

        self.assertEqual(len(partitions), 1)
        self.assertEqual(len(partitions[0]), 3)

    def test_values_preserved(self):
        """
        Values associated with each key must be identical after partitioning.
        """
        grouped = {"P001": [1, 1, 1], "P002": [3, 7]}
        partitions = hash_partition(grouped, num_partitions=2)

        # Find where each key ended up and verify its values
        for part in partitions:
            if "P001" in part:
                self.assertEqual(part["P001"], [1, 1, 1])
            if "P002" in part:
                self.assertEqual(part["P002"], [3, 7])

    def test_deterministic_assignment(self):
        """
        The same key must always land in the same partition across
        multiple calls (determinism is required for correctness).
        """
        grouped = {"stable_key": [1, 2, 3]}
        result1 = hash_partition(grouped, num_partitions=4)
        result2 = hash_partition(grouped, num_partitions=4)

        # Find which bucket got the key in each run
        bucket1 = next(i for i, p in enumerate(result1) if "stable_key" in p)
        bucket2 = next(i for i, p in enumerate(result2) if "stable_key" in p)

        self.assertEqual(bucket1, bucket2)

    def test_empty_input(self):
        """
        Empty input should produce empty partitions.
        """
        partitions = hash_partition({}, num_partitions=3)

        self.assertEqual(len(partitions), 3)
        self.assertTrue(all(len(p) == 0 for p in partitions))


if __name__ == "__main__":
    unittest.main()
