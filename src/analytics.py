"""
Query helpers: top_passengers_by_flights(), top_routes()
"""

from typing import List, Tuple, Dict


def top_passengers_by_flights(
    results: Dict[str, int], top_n: int = 10
) -> List[Tuple[str, int]]:
    """
    Return the top-N passengers ranked by total flight count.
 
    Args:
        results : Output of the flight-count MapReduce job.
        top_n   : How many top entries to return.
 
    Returns:
        Sorted list of (PassengerId, flight_count) tuples, descending.
    """
    # key=lambda x: x[1] sorts by the count (index 1), not the passenger ID (index 0)
    # reverse=True gives descending order so index 0 is the highest-flight passenger
    # [:top_n] slices the sorted list to return only the requested number of results
    return sorted(results.items(), key=lambda x: x[1], reverse=True)[:top_n]
 
 
def top_routes(
    results: Dict[str, int], top_n: int = 10
) -> List[Tuple[str, int]]:
    """
    Return the top-N busiest routes ranked by flight count.
 
    Args:
        results : Output of the route MapReduce job.
        top_n   : How many top routes to return.
 
    Returns:
        Sorted list of (route, count) tuples, descending.
    """
    # Identical sort logic to top_passengers_by_flights; separated into its
    # own function for clarity and to allow independent top_n values per job
    return sorted(results.items(), key=lambda x: x[1], reverse=True)[:top_n]
