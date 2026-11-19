"""
Reducer: Final aggregation logic.
"""

from typing import List, Tuple


def reduce_sum(key: str, values: List[int]) -> Tuple[str, int]:
    """
    Reduce phase: sum all values associated with a key.

    This is the canonical word-count reducer, adapted here to count
    flights per passenger or occurrences per route.

    Args:
        key    : The grouped key (e.g., PassengerId or route string).
        values : All intermediate values emitted for this key.

    Returns:
        A (key, total) tuple.
    """
    # sum(values) collapses [1, 1, 1, …] into a single integer
    return (key, sum(values))
