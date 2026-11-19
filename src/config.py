"""
Configuration settings for the MapReduce Flight Analysis framework.
Centralises chunk size, worker count, logging setup, and CSV field definitions.
"""

import logging

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,                                   # show INFO and above (DEBUG is suppressed)
    format="%(asctime)s [%(levelname)s] %(message)s",     # timestamp + level prefix on every line
    datefmt="%H:%M:%S",                                   # compact HH:MM:SS timestamp
)
log = logging.getLogger(__name__)                         # module-scoped logger; __name__ = 'mapreduce_flights'

# ---------------------------------------------------------------------------
# Default pipeline parameters
# ---------------------------------------------------------------------------
DEFAULT_NUM_CHUNKS = 4
DEFAULT_NUM_WORKERS = 4
DEFAULT_RETRIES = 3
DEFAULT_RETRY_DELAY = 0.5

# ---------------------------------------------------------------------------
# CSV field definitions
# ---------------------------------------------------------------------------
FIELDNAMES = ["PassengerId", "FlightID", "FromAirport",
              "ToAirport", "DepartureTime", "TotalFlightTime"]
HAS_HEADER = False
