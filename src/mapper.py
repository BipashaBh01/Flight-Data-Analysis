"""
Mapper: Emits (key, value) pairs per flight record.
"""

import logging
from typing import List, Tuple, Dict

log = logging.getLogger(__name__)


def map_flight_count(chunk: List[Dict]) -> List[Tuple[str, int]]:
    """
    Map phase: emit one (PassengerId, 1) pair per flight record.

    Each call processes a single chunk and returns a flat list of
    intermediate key-value pairs suitable for the shuffle step.

    Args:
        chunk : A list of row-dictionaries from the splitter.

    Returns:
        A list of (passenger_id, 1) tuples.
    """
    pairs: List[Tuple[str, int]] = []               # local output list; never shared across threads
    skipped = 0                                      # counter for rows dropped due to missing IDs
    
    for row in chunk:
        # .get() with a default of "" avoids a KeyError if the column is missing entirely
        pid = row.get("PassengerId", "").strip()
        
        if pid:                           # skip rows where PassengerId is empty or whitespace-only
            pairs.append((pid, 1))        # value is always 1; the reducer will sum these later
        else:
            skipped += 1

    if skipped:
        log.warning("map_flight_count: skipped %d rows with missing PassengerId", skipped)

    log.debug("map_flight_count: emitted %d pairs from %d rows", len(pairs), len(chunk))
    return pairs


def map_flight_routes(chunk: List[Dict]) -> List[Tuple[str, int]]:
    """
    Extension mapper: emit (route, 1) pairs to count flights per route.

    A route is represented as 'FromAirport->ToAirport'.

    Args:
        chunk : A list of row-dictionaries.

    Returns:
        A list of (route_string, 1) tuples.
    """
    pairs: List[Tuple[str, int]] = []
    skipped = 0                                      # counter for rows with incomplete airport data

    for row in chunk:
        frm = row.get("FromAirport", "").strip()       # origin IATA code
        to  = row.get("ToAirport",   "").strip()       # destination IATA code
        
        # Only emit if BOTH airports are present; partial rows are silently dropped
        if frm and to:
            # f-string builds a human-readable directional route key
            pairs.append((f"{frm}->{to}", 1))
        else:
            skipped += 1

    if skipped:
        log.warning("map_flight_routes: skipped %d rows with missing airport data", skipped)

    log.debug("map_flight_routes: emitted %d pairs from %d rows", len(pairs), len(chunk))
    return pairs
