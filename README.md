# MapReduce Flight Data Analysis

A Python-based MapReduce framework designed to analyse passenger-flight data. The system reads a CSV dataset to identify the most frequent flyers and the busiest flight routes using a parallel processing pipeline inspired by the Hadoop MapReduce model.

## Problem Statement

Given a large CSV dataset of passenger flight records, the aim is to determine:

1. **Job 1** — The passenger(s) with highest number of flights taken; also to rank the top 10 passengers with most flights.
2. **Job 2 (Extension)** — The flight routes that are used most often, ranked by frequency.

The solution implements a complete MapReduce pipeline: Split &#8594; Map &#8594; Combine &#8594; Shuffle/Sort &#8594; Partition &#8594; Reduce &mdash; with multi-threaded execution, fault tolerance, and reusable architecture.

---

## Features

| Feature | Description |
|---|---|
| **Modular Pipeline** | Each MapReduce stage (split, map, combine, shuffle, partition, reduce) is an independent, testable module. |
| **Multi-threaded Execution** | Map and reduce phases run concurrently via `ThreadPoolExecutor`. |
| **Combiner (Local Pre-aggregation)** | Reduces shuffle data volume by aggregating within each chunk before the shuffle step. |
| **Hash Partitioner** | Deterministically assigns keys to reducer buckets for even load distribution, mirroring Hadoop's `HashPartitioner`. |
| **Fault Tolerance** | Workers are wrapped in a retry mechanism with exponential back-off. |
| **Two MapReduce Jobs** | Demonstrates framework reuse - same pipeline, different mappers. |
| **Centralised Configuration** | Chunk count, worker count, retries, and CSV field defined in a single `config.py`. |
| **Comprehensive Testing** | 37 unit and integration tests covering every pipeline stage. |

---

## Project Structure

```
.
├── src/
│   ├── __init__.py                            # Package exports
│   ├── config.py                              # Centralised settings (chunks, workers, CSV fields)
│   ├── splitter.py                            # Splits CSV into chunks for parallel processing
│   ├── mapper.py                              # Map functions: flight count and route mappers
│   ├── combiner.py                            # Local pre-aggregation (mini-reducer per chunk)
│   ├── shuffle.py                             # Groups intermediate pairs by key, sorts lexicographically
│   ├── partitioner.py                         # Hash-based key-to-reducer bucket assignment
│   ├── reducer.py                             # Final aggregation: sums all values per key
│   ├── fault_tolerance.py                     # Retry wrapper with exponential back-off
│   ├── orchestrator.py                        # Coordinates the full MapReduce pipeline
│   ├── analytics.py                           # Query helpers: top passengers, top routes
│   └── main.py                                # Entry point: runs both jobs, prints results
├── tests/
│   ├── __init__.py                            # Test package marker
│   ├── test_mapper.py                         # Unit tests for map functions
│   ├── test_reducer.py                        # Unit tests for the reducer
│   ├── test_combiner.py                       # Unit tests for the combiner
│   ├── test_partitioner.py                    # Unit tests for the hash partitioner
│   ├── test_fault_tolerance.py                # Unit tests for retry/back-off logic
│   └── test_pipeline.py                       # Integration tests (combine, shuffle, end-to-end pipeline)
├── data/
│   └── Task_A_passenger_flight_data.csv
├── README.md
└── requirements.txt                           # Project dependencies
```

---

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      CSV Dataset                                │
└────────────────────────┬────────────────────────────────────────┘
                         │
                    ┌────▼────┐
                    │ SPLITTER │  Divides rows into N chunks
                    └────┬────┘
                         │
          ┌──────────────┼──────────────┐
          │              │              │
     ┌────▼────┐   ┌────▼────┐   ┌────▼────┐
     │ MAPPER  │   │ MAPPER  │   │ MAPPER  │   Parallel threads
     │ Worker 1│   │ Worker 2│   │ Worker N│   emit (key, 1) pairs
     └────┬────┘   └────┬────┘   └────┬────┘
          │              │              │
     ┌────▼────┐   ┌────▼────┐   ┌────▼────┐
     │COMBINER │   │COMBINER │   │COMBINER │   Optional local
     │ (local) │   │ (local) │   │ (local) │   pre-aggregation
     └────┬────┘   └────┬────┘   └────┬────┘
          │              │              │
          └──────────────┼──────────────┘
                         │
                  ┌──────▼──────┐
                  │SHUFFLE/SORT │  Group all pairs by key
                  └──────┬──────┘
                         │
                  ┌──────▼──────┐
                  │ PARTITIONER │  Hash keys into reducer buckets
                  └──────┬──────┘
                         │
          ┌──────────────┼──────────────┐
          │              │              │
     ┌────▼────┐   ┌────▼────┐   ┌────▼────┐
     │ REDUCER │   │ REDUCER │   │ REDUCER │   Parallel threads
     │Bucket 1 │   │Bucket 2 │   │Bucket N │   sum values per key
     └────┬────┘   └────┬────┘   └────┬────┘
          │              │              │
          └──────────────┼──────────────┘
                         │
                  ┌──────▼──────┐
                  │   RESULTS   │  Final aggregated output
                  └─────────────┘
```

---

## Installation & Usage

### Prerequisites

- Python 3.8+

### Running the Analysis

```bash
# From project root, two ways to run:

# 1. Using the default dataset (this will automatically uses Task_A_passenger_flight_data.csv)
python -m src.main

# 2. Specifying the dataset (use a custom dataset if needed)
python -m src.main data/Task_A_passenger_flight_data.csv
```

### Running Tests

```bash
# All tests (37 total)

# Using unittest
python -m unittest discover -s tests -v

# Using pytest
pytest

# Individual test files
python -m unittest tests.test_mapper
python -m unittest tests.test_reducer
python -m unittest tests.test_combiner
python -m unittest tests.test_partitioner
python -m unittest tests.test_fault_tolerance
python -m unittest tests.test_pipeline
```

---

## How It Works

### 1. Splitting (`splitter.py`)
Reads the CSV and splits it into approximately equal chunks. Each chunk is a list of dictionaries (one per row), enabling parallel processing without shared state. The encoding `utf-8-sig` handles Excel-style encoding issues.

### 2. Mapping (`mapper.py`)
Two mapper functions are provided:
- **`map_flight_count`** — emits `(PassengerId, 1)` for each valid record.
- **`map_flight_routes`** — emits `(FromAirport->ToAirport, 1)` for each valid record.

Both mappers log warnings when rows are skipped due to missing fields, and emit debug-level counts of processed records.

### 3. Combining (`combiner.py`)
An optional mini-reducer applied per chunk *before* the shuffle step. It locally aggregates duplicate keys, reducing the volume of data that must be transferred and sorted.

### 4. Shuffle & Sort (`shuffle.py`)
Groups all intermediate key-value pairs by key using a `defaultdict`, then sorts keys lexicographically — mirroring Hadoop's shuffle phase.

### 5. Partitioning (`partitioner.py`)
A hash-based partitioner that deterministically assigns each key to a reducer bucket using `hash(key) % num_partitions`. This ensures even load distribution across reducers and mirrors Hadoop's `HashPartitioner`.

### 6. Reducing (`reducer.py`)
Sums all values for each unique key, producing the final count. Works identically on raw mapper output and pre-aggregated combiner output since `sum()` is associative and commutative.

### 7. Orchestration (`orchestrator.py`)
Coordinates the full pipeline using `ThreadPoolExecutor` for both map and reduce phases. A threading lock protects the shared intermediate-pairs list during the map phase. All tuneable parameters are drawn from `config.py`.

### 8. Fault Tolerance (`fault_tolerance.py`)
Adds retry logic with exponential backoff for failed tasks, similar to Hadoop’s retry mechanism. Supports flexible function handling.

---

## Configuration

All tuneable parameters are centralised in `src/config.py`:

| Parameter | Default | Description |
|---|---|---|
| `DEFAULT_NUM_CHUNKS` | 4 | Number of data chunks for splitting |
| `DEFAULT_NUM_WORKERS` | 4 | Thread pool size for map & reduce |
| `DEFAULT_RETRIES` | 3 | Max retry attempts per worker |
| `DEFAULT_RETRY_DELAY` | 0.5 | Initial retry delay in seconds |
| `HAS_HEADER` | False | Whether the CSV has a header row |

---

## Sample Output

```
============================================================
  MapReduce Flight Data Analysis
============================================================

────────────────────────────────────────────────────────────
  JOB 1 — Passenger(s) with the highest number of flights
────────────────────────────────────────────────────────────
  Max flights recorded : 25
  Passenger(s)         : ['UES9151GS5']

  Top 10 passengers:
     1. UES9151GS5            25 flights
     2. XEO2189OQ4            24 flights
     3. TVA9549HJ7            23 flights
     ...

  [Job 1 completed in 0.042s]

────────────────────────────────────────────────────────────
  JOB 2 — Top 10 busiest routes  [Extension]
────────────────────────────────────────────────────────────
     1. SYD->LHR                    12 flights
     2. LHR->CDG                    11 flights
     ...

  [Job 2 completed in 0.038s]

============================================================
```

---

## Testing

The project includes **37 tests** across 6 test files:

| Test File | Tests | Coverage |
|---|---|---|
| `test_mapper.py` | 6 | Basic emission, duplicates, empty IDs, empty chunks, route formatting, missing airports |
| `test_reducer.py` | 4 | Single value, multiple values, pre-aggregated values, empty list |
| `test_combiner.py` | 6 | Aggregation, single key, no duplicates, empty input, total preservation, output type |
| `test_partitioner.py` | 7 | Key preservation, no duplication, partition count, single-partition, value integrity, determinism, empty input |
| `test_fault_tolerance.py` | 7 | First-try success, retry-then-success, permanent failure, arg forwarding, exponential back-off, zero-retry edges, zero-retry success |
| `test_pipeline.py` | 7 | Combiner integration, shuffle grouping, sorting, full flight-count pipeline, route pipeline, top passengers, top routes |

---

