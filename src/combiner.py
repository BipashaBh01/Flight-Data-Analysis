"""
Combiner: Local pre-aggregation to reduce shuffle data volume.
"""

from collections import defaultdict
from typing import List, Tuple, Dict


def combine(pairs: List[Tuple[str, int]]) -> List[Tuple[str, int]]:
    """
    Combiner: locally aggregate key-value pairs within a single chunk.

    The combiner acts as a mini-reducer applied before the shuffle step,
    dramatically reducing the volume of data that must be transferred and
    sorted across workers.

    Args:
        pairs : Raw (key, value) pairs from one map worker.

    Returns:
        A compacted list of (key, aggregated_value) tuples.
    """

    # defaultdict(int) initialises missing keys to 0, removing the need for
    # an explicit "if key not in local" check before incrementing
    local: Dict[str, int] = defaultdict(int)
    
    for key, val in pairs:
        local[key] += val          # accumulate counts for this key within the chunk
    
    # Convert back to a plain list of tuples so the return type matches
    # what the shuffle step expects from all mappers
    return list(local.items())
