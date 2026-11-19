"""
Shuffler / Sorter: Grouping and sorting phase.
"""

import logging
from collections import defaultdict
from typing import List, Tuple, Dict

log = logging.getLogger(__name__)


def shuffle_and_sort(
    all_pairs: List[Tuple[str, int]]
) -> Dict[str, List[int]]:
    """
    Shuffle and Sort phase: group all intermediate pairs by key.

    In a real distributed MapReduce, this step moves all values for the
    same key to the same reducer node and sorts by key so that reducers
    receive data in a deterministic order.  Here we emulate this using
    an in-memory defaultdict followed by sorting.

    Args:
        all_pairs : Flattened list of (key, value) pairs from all mappers.

    Returns:
        An ordered dictionary mapping each key to a sorted list of values.
    """
    
    log.info("SHUFFLE/SORT: grouping %d intermediate pairs …", len(all_pairs))
    
    # defaultdict(list) automatically creates an empty list for each new key,
    # so we never need to check "if key not in grouped" before appending
    grouped: Dict[str, List[int]] = defaultdict(list)
    
    for key, val in all_pairs:
        grouped[key].append(val)     # collect all values under the same key
    
    # sorted() returns items in lexicographic key order, mirroring Hadoop's sort phase.
    # dict() wraps the result so callers get a plain dict, not a defaultdict
    sorted_groups = dict(sorted(grouped.items()))
    
    log.info("SHUFFLE/SORT: %d unique keys after grouping", len(sorted_groups))
    return sorted_groups
