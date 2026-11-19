"""
Partitioner: Deterministic key-to-reducer assignment.

Controls which reducer receives which keys, ensuring an even distribution
of work across reducer workers.  In a distributed MapReduce cluster this
determines the network destination; here it pre-groups keys into buckets
so the reduce phase can parallelise more effectively.
"""

from typing import List, Tuple, Dict

from .config import DEFAULT_NUM_WORKERS


def hash_partition(
    grouped: Dict[str, List[int]],
    num_partitions: int = DEFAULT_NUM_WORKERS,
) -> List[Dict[str, List[int]]]:
    """
    Partition grouped keys into *num_partitions* buckets using a hash function.

    Each bucket is an independent dictionary that can be handed to a separate
    reducer worker, mimicking Hadoop's HashPartitioner.

    The built-in hash() is deterministic within a single Python process,
    so the same key always lands in the same partition — a requirement for
    correct reduce-side aggregation.

    Args:
        grouped        : Output of shuffle_and_sort — {key: [values]}.
        num_partitions : Number of reducer buckets (typically == num_workers).

    Returns:
        A list of *num_partitions* dictionaries, each containing a
        disjoint subset of the keys.
    """
    # Pre-allocate empty dicts for every partition slot
    partitions: List[Dict[str, List[int]]] = [
        {} for _ in range(num_partitions)
    ]

    for key, values in grouped.items():
        # hash(key) % num_partitions gives a deterministic bucket index;
        # abs() guards against negative hashes on some Python implementations
        bucket = abs(hash(key)) % num_partitions
        partitions[bucket][key] = values

    return partitions
