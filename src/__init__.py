"""
MapReduce Flight Analysis — src package.
"""

from .splitter import split_data
from .mapper import map_flight_count, map_flight_routes
from .combiner import combine
from .shuffle import shuffle_and_sort
from .reducer import reduce_sum
from .fault_tolerance import fault_tolerant_worker
from .orchestrator import run_mapreduce
from .analytics import top_passengers_by_flights, top_routes
from .main import main
