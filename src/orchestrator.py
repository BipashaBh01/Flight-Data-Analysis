"""
Orchestrator: Full MapReduce pipeline execution.
"""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Dict, Callable

from .combiner import combine
from .shuffle import shuffle_and_sort
from .reducer import reduce_sum
from .fault_tolerance import fault_tolerant_worker
from .config import DEFAULT_NUM_WORKERS

log = logging.getLogger(__name__)


def run_mapreduce(
    chunks: List[List[Dict]],
    mapper: Callable,
    reducer: Callable = reduce_sum,
    use_combiner: bool = True,
    num_workers: int = DEFAULT_NUM_WORKERS,
) -> Dict[str, int]:
    """
    Orchestrate the full MapReduce pipeline using a thread pool.
 
    Pipeline stages:
        1. Map           : each chunk processed by a mapper thread
        2. Combine       : optional local aggregation per chunk
        3. Shuffle/Sort  : group all pairs by key
        4. Reduce        : aggregate grouped values in parallel
 
    Args:
        chunks       : Pre-split data chunks (from splitter).
        mapper       : Map function: chunk -> [(key, value), …]
        reducer      : Reduce function: (key, [values]) -> (key, total)
        use_combiner : Whether to apply the combiner after each map.
        num_workers  : Thread pool size for map and reduce phases.
 
    Returns:
        A dictionary mapping each key to its final aggregated value.
    """
    # ========== MAP PHASE ==================================================
    log.info("MAP PHASE: launching %d workers …", num_workers)
 
    all_pairs: List[Tuple[str, int]] = []      # shared list that every map thread appends to
    lock = threading.Lock()                    # mutex so concurrent .extend() calls don't interleave
 
    def map_worker(chunk):
        """Inner closure: runs one mapper on one chunk inside the thread pool."""
        # fault_tolerant_worker wraps the mapper call; returns None if all retries fail
        result = fault_tolerant_worker(mapper, chunk)
 
        if result is None:
            return                             # mapper permanently failed; skip this chunk gracefully
 
        if use_combiner:
            # Apply the combiner before adding to the shared list to reduce
            # the number of items that enter the shuffle step
            result = combine(result)
 
        # Lock is required because list.extend() is not atomic;
        # without it, two threads could corrupt the list during a resize
        with lock:
            all_pairs.extend(result)
 
    # as_completed() context: the pool is shut down cleanly when the 'with' block exits,
    # waiting for all submitted tasks to finish before proceeding
    with ThreadPoolExecutor(max_workers=num_workers) as pool:
        # Submit one map_worker future per chunk; futures run concurrently on the thread pool
        futures = [pool.submit(map_worker, chunk) for chunk in chunks]
 
        for f in as_completed(futures):
            # Calling f.result() re-raises any exception that escaped map_worker,
            # making errors visible in the main thread rather than silently swallowed
            f.result()
 
    log.info("MAP PHASE: %d intermediate pairs collected", len(all_pairs))
 
    # ================ SHUFFLE / SORT =====================================================
    # Single-threaded; grouping is inherently sequential and fast enough
    # that parallelising it would add synchronisation overhead without benefit
    grouped = shuffle_and_sort(all_pairs)
 
    # =============== REDUCE PHASE =======================================================
    log.info("REDUCE PHASE: reducing %d keys …", len(grouped))
    results: Dict[str, int] = {}               # final output: {key: total_count}
 
    with ThreadPoolExecutor(max_workers=num_workers) as pool:
        # Dict comprehension maps each future back to its key so we can retrieve
        # results in the order they complete rather than submission order
        future_to_key = {
            pool.submit(fault_tolerant_worker, reducer, k, v): k
            for k, v in grouped.items()        # one reduce task per unique key
        }
 
        for future in as_completed(future_to_key):
            output = future.result()           # (key, total) tuple, or None on permanent failure
 
            if output:                         # guard against None from a failed reduce worker
                key, total = output
                results[key] = total           # CPython dict writes from threads are safe due to the GIL
 
    log.info("REDUCE PHASE: complete — %d unique results", len(results))
    return results
