"""
Integration tests for the full MapReduce pipeline.

This test suite verifies the correctness of the entire system:
Mapper --> Combiner --> Shuffle --> Reducer --> Orchestrator --> Analytics.

It ensures that:
- Individual components work together correctly
- End-to-end MapReduce execution produces correct results
- Aggregation and ranking functions behave as expected
"""

import unittest

# Core MapReduce pipeline components
from src.splitter import split_data
from src.mapper import map_flight_count, map_flight_routes
from src.combiner import combine
from src.shuffle import shuffle_and_sort
from src.reducer import reduce_sum
from src.orchestrator import run_mapreduce
from src.analytics import top_passengers_by_flights, top_routes


# ---------------------------------------------------------------------------
# SAMPLE DATASET (IN-MEMORY FIXTURE)
# ---------------------------------------------------------------------------
# This dataset simulates a small flight database.
# It is used for deterministic testing of the full pipeline.

SAMPLE_ROWS = [
    {"PassengerId": "P001", "FlightID": "F1", "FromAirport": "LHR", "ToAirport": "JFK",
     "DepartureTime": "2024-01-01 08:00", "TotalFlightTime": "480"},

    {"PassengerId": "P002", "FlightID": "F2", "FromAirport": "CDG", "ToAirport": "SFO",
     "DepartureTime": "2024-01-02 10:00", "TotalFlightTime": "660"},

    {"PassengerId": "P001", "FlightID": "F3", "FromAirport": "JFK", "ToAirport": "LHR",
     "DepartureTime": "2024-01-03 14:00", "TotalFlightTime": "450"},

    {"PassengerId": "P003", "FlightID": "F4", "FromAirport": "LHR", "ToAirport": "JFK",
     "DepartureTime": "2024-01-04 09:00", "TotalFlightTime": "470"},

    {"PassengerId": "P001", "FlightID": "F5", "FromAirport": "LHR", "ToAirport": "CDG",
     "DepartureTime": "2024-01-05 11:00", "TotalFlightTime": "90"},
]


# ---------------------------------------------------------------------------
# COMBINER TESTS
# ---------------------------------------------------------------------------

class TestCombiner(unittest.TestCase):
    """
    Tests the combiner stage.

    The combiner performs local aggregation:
        ("P001", 1), ("P001", 1) → ("P001", 2)
    """

    def test_combine_aggregates(self):
        """
        Ensure that duplicate keys are aggregated correctly.
        """
        pairs = [("P001", 1), ("P002", 1), ("P001", 1)]

        # Convert list of tuples into dictionary for easier assertion
        result = dict(combine(pairs))

        # P001 appears twice --> should be aggregated to 2
        self.assertEqual(result["P001"], 2)

        # P002 appears once --> remains 1
        self.assertEqual(result["P002"], 1)


# ---------------------------------------------------------------------------
# SHUFFLE & SORT TESTS
# ---------------------------------------------------------------------------

class TestShuffleAndSort(unittest.TestCase):
    """
    Tests the shuffle and sort phase.

    Responsibilities:
    - Group values by key
    - Sort keys in deterministic order
    """

    def test_grouping(self):
        """
        Ensure values are grouped correctly by key.
        """
        pairs = [("P001", 1), ("P002", 1), ("P001", 1)]
        grouped = shuffle_and_sort(pairs)

        # Values for P001 should be grouped together
        self.assertEqual(grouped["P001"], [1, 1])

        # P002 should have a single value
        self.assertEqual(grouped["P002"], [1])

    def test_sorted_keys(self):
        """
        Ensure output dictionary keys are sorted lexicographically.
        """
        pairs = [("B", 1), ("A", 1), ("C", 1)]
        grouped = shuffle_and_sort(pairs)

        # Keys should be sorted alphabetically
        self.assertEqual(list(grouped.keys()), ["A", "B", "C"])


# ---------------------------------------------------------------------------
# END-TO-END PIPELINE TESTS
# ---------------------------------------------------------------------------

class TestPipeline(unittest.TestCase):
    """
    Full integration tests for the MapReduce pipeline.

    This verifies:
    - Mapper correctness
    - Combiner + shuffle correctness
    - Reducer correctness
    - Orchestrator coordination
    """

    def test_flight_count_pipeline(self):
        """
        Test full pipeline for passenger flight counts.
        """
        # Split dataset into chunks (simulating distributed input)
        chunks = [SAMPLE_ROWS[:3], SAMPLE_ROWS[3:]]

        # Run full MapReduce pipeline
        results = run_mapreduce(
            chunks,
            map_flight_count,
            use_combiner=True,
            num_workers=2
        )

        # Verify aggregated results
        self.assertEqual(results["P001"], 3)
        self.assertEqual(results["P002"], 1)
        self.assertEqual(results["P003"], 1)

    def test_route_pipeline(self):
        """
        Test full pipeline for flight route counting.
        """
        chunks = [SAMPLE_ROWS]

        results = run_mapreduce(
            chunks,
            map_flight_routes,
            use_combiner=True,
            num_workers=1
        )

        # Verify route counts
        self.assertEqual(results["LHR->JFK"], 2)
        self.assertEqual(results["CDG->SFO"], 1)

    def test_top_passengers(self):
        """
        Test ranking of passengers by flight count.
        """
        results = {"P001": 5, "P002": 3, "P003": 8}

        top = top_passengers_by_flights(results, top_n=2)

        # Highest count should come first
        self.assertEqual(top[0], ("P003", 8))
        self.assertEqual(top[1], ("P001", 5))

    def test_top_routes(self):
        """
        Test ranking of routes by frequency.
        """
        results = {"LHR->JFK": 10, "CDG->SFO": 5, "JFK->LHR": 8}

        top = top_routes(results, top_n=2)

        # Ensure correct descending order
        self.assertEqual(top[0], ("LHR->JFK", 10))
        self.assertEqual(top[1], ("JFK->LHR", 8))


# ---------------------------------------------------------------------------
# TEST RUNNER ENTRY POINT
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Executes all unit + integration tests when file is run directly
    unittest.main()