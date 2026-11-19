"""
Main entry point: runs both MapReduce jobs and prints output.
"""

import os
import sys
import time
import logging

from .config import DEFAULT_NUM_CHUNKS
from .splitter import split_data
from .mapper import map_flight_count, map_flight_routes
from .orchestrator import run_mapreduce
from .analytics import top_passengers_by_flights, top_routes

log = logging.getLogger(__name__)


def main(csv_path: str = "data/Task_A_passenger_flight_data.csv"):
    """
    Run all MapReduce jobs and print a formatted results summary.
 
    Args:
        csv_path : Path to the passenger/flight CSV dataset.
    """
    if not os.path.exists(csv_path):                    # fail with a clear message
        log.error("Dataset not found: %s", csv_path)
        return {}
 
    print("\n" + "=" * 60)
    print("  MapReduce Flight Data Analysis")
    print("=" * 60)
 
    # ========== JOB 1: Passenger flight counts ===================================
    t0 = time.perf_counter()                   # high-resolution wall-clock start time for Job 1
 
    # Split once; the same chunks list is reused for Job 2 to avoid re-reading the CSV
    chunks = split_data(csv_path, num_chunks=DEFAULT_NUM_CHUNKS)
 
    flight_results = run_mapreduce(chunks, map_flight_count, use_combiner=True)
 
    elapsed_job1 = time.perf_counter() - t0   # wall-clock seconds for the entire Job 1 pipeline
 
    top10 = top_passengers_by_flights(flight_results, top_n=10)
 
    # top10[0][1] is the count of the highest-ranked passenger (list is already sorted descending)
    max_flights = top10[0][1] if top10 else 0
 
    # A list comprehension finds ALL passengers tied at max_flights,
    # handling ties correctly rather than returning only the first match
    winners = [(pid, cnt) for pid, cnt in flight_results.items()
               if cnt == max_flights]
 
    print(f"\n{'─'*60}")
    print("  JOB 1 — Passenger(s) with the highest number of flights")
    print(f"{'─'*60}")
    print(f"  Max flights recorded : {max_flights}")
    print(f"  Passenger(s)         : {[w[0] for w in winners]}")
    print(f"\n  Top 10 passengers:")
 
    # enumerate(..., 1) starts the rank counter at 1 instead of the default 0
    for rank, (pid, cnt) in enumerate(top10, 1):
        # :>2 right-aligns rank in a 2-char field; :<20 left-aligns passenger ID in 20 chars
        print(f"    {rank:>2}. {pid:<20}  {cnt} flights")
 
    print(f"\n  [Job 1 completed in {elapsed_job1:.3f}s]")
 
    # =========== JOB 2: Busiest routes (extension) ================================================
    t1 = time.perf_counter()                   # separate timer for Job 2
 
    # Re-use the same chunks; only the mapper changes — demonstrating framework generality
    route_results = run_mapreduce(chunks, map_flight_routes, use_combiner=True)
 
    elapsed_job2 = time.perf_counter() - t1
 
    top_r = top_routes(route_results, top_n=10)
 
    print(f"\n{'─'*60}")
    print("  JOB 2 — Top 10 busiest routes  [Extension]")
    print(f"{'─'*60}")
    for rank, (route, cnt) in enumerate(top_r, 1):
        print(f"    {rank:>2}. {route:<30}  {cnt} flights")
    print(f"\n  [Job 2 completed in {elapsed_job2:.3f}s]")
    print("\n" + "=" * 60 + "\n")
 
    # Return a dict so callers (e.g. the report generator) can consume results programmatically
    return {
        "flight_results": flight_results,
        "route_results": route_results,
        "winners": winners,
        "max_flights": max_flights,
        "top10_passengers": top10,
        "top10_routes": top_r,
    }
 
 
if __name__ == "__main__":
    # sys.argv[1] lets the user pass a custom CSV path on the command line;
    # falling back to the default filename keeps zero-argument invocation working
    path = sys.argv[1] if len(sys.argv) > 1 else "data/Task_A_passenger_flight_data.csv"
    main(path)
