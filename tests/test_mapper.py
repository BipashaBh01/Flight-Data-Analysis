"""
Unit tests for the mapper module.

This file tests whether the mapper functions correctly transform
input flight data into (key, value) pairs for MapReduce.
"""

import unittest
# Import the mapper functions to be tested
from src.mapper import map_flight_count, map_flight_routes


class TestMapFlightCount(unittest.TestCase):
    """
    Test suite for map_flight_count function.

    This mapper emits (PassengerId, 1) for each valid flight record.
    """

    def test_basic_emission(self):
        """
        Test that each passenger generates one (PassengerId, 1) pair.
        """
        # Sample input chunk (simulating a small part of dataset)
        chunk = [
            {"PassengerId": "P001", "FlightID": "F1", "FromAirport": "LHR", "ToAirport": "JFK",
             "DepartureTime": "2024-01-01 08:00", "TotalFlightTime": "480"},
            {"PassengerId": "P002", "FlightID": "F2", "FromAirport": "CDG", "ToAirport": "SFO",
             "DepartureTime": "2024-01-02 10:00", "TotalFlightTime": "660"},
        ]

        # Run mapper
        result = map_flight_count(chunk)

        # Expect one count per passenger
        self.assertEqual(result, [("P001", 1), ("P002", 1)])

    def test_duplicate_passenger(self):
        """
        Test that duplicate passengers still emit separate (key,1) pairs.
        (Aggregation happens later in reducer, not mapper)
        """
        chunk = [
            {"PassengerId": "P001", "FlightID": "F1", "FromAirport": "LHR", "ToAirport": "JFK",
             "DepartureTime": "2024-01-01 08:00", "TotalFlightTime": "480"},
            {"PassengerId": "P001", "FlightID": "F3", "FromAirport": "JFK", "ToAirport": "LHR",
             "DepartureTime": "2024-01-03 14:00", "TotalFlightTime": "450"},
        ]

        result = map_flight_count(chunk)

        # Should emit two records (not aggregated yet)
        self.assertEqual(len(result), 2)

        # Each value should still be 1
        self.assertTrue(all(v == 1 for _, v in result))

    def test_empty_passenger_id_skipped(self):
        """
        Test that records with missing PassengerId are ignored.
        """
        chunk = [
            {"PassengerId": "", "FlightID": "F1", "FromAirport": "LHR", "ToAirport": "JFK",
             "DepartureTime": "2024-01-01 08:00", "TotalFlightTime": "480"},
        ]

        result = map_flight_count(chunk)

        # Invalid records should not produce output
        self.assertEqual(result, [])

    def test_empty_chunk(self):
        """
        Test that an empty input returns an empty output.
        """
        self.assertEqual(map_flight_count([]), [])


class TestMapFlightRoutes(unittest.TestCase):
    """
    Test suite for map_flight_routes function.

    This mapper emits (route, 1), where route = "FromAirport->ToAirport".
    """

    def test_route_emission(self):
        """
        Test correct route formatting and emission.
        """
        chunk = [
            {"PassengerId": "P001", "FlightID": "F1", "FromAirport": "LHR", "ToAirport": "JFK",
             "DepartureTime": "2024-01-01 08:00", "TotalFlightTime": "480"},
        ]

        result = map_flight_routes(chunk)

        # Expect route formatted as "FROM->TO"
        self.assertEqual(result, [("LHR->JFK", 1)])

    def test_missing_airport_skipped(self):
        """
        Test that records with missing airport info are ignored.
        """
        chunk = [
            {"PassengerId": "P001", "FlightID": "F1", "FromAirport": "LHR", "ToAirport": "",
             "DepartureTime": "2024-01-01 08:00", "TotalFlightTime": "480"},
        ]

        result = map_flight_routes(chunk)

        # Invalid routes should not be emitted
        self.assertEqual(result, [])


# Entry point to run all tests
if __name__ == "__main__":
    unittest.main()