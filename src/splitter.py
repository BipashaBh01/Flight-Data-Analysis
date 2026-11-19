"""
Splitter: Divides the dataset into chunks for parallel processing.
"""

import csv
import logging
from typing import List, Dict

from .config import FIELDNAMES, HAS_HEADER

log = logging.getLogger(__name__)


def split_data(filepath: str, num_chunks: int = 4) -> List[List[Dict]]:
    """
    Read the CSV dataset and divide rows into approximately equal chunks.

    Each chunk is a list of row-dictionaries produced by csv.DictReader.
    Splitting enables parallel map workers to process independent subsets
    of the data without shared state.

    Args:
        filepath   : Path to the CSV file.
        num_chunks : Desired number of parallel chunks (default 4).

    Returns:
        A list of chunks, where each chunk is a list of row dicts.
    """
    log.info("SPLITTER: reading '%s' …", filepath)
    rows: List[Dict] = []                                         # accumulates every row as a plain dict

    # Read CSV file
    with open(filepath, newline="", encoding="utf-8-sig") as fh:
        # newline=""   – lets csv module handle \r\n line endings on all platforms
        # utf-8-sig    – silently strips the UTF-8 BOM that Excel often prepends to CSV exports

        # Supplying fieldnames explicitly (row 1 is treated as data, not a header)
        reader = csv.DictReader(fh, fieldnames=FIELDNAMES)

        if HAS_HEADER:
            next(reader)                       # discard the real header row so it doesn't become a data record

        for row in reader:
            # Strip values: some CSVs have spaces around commas that pollute the data
            rows.append({k.strip(): v.strip() for k, v in row.items()})

    total = len(rows)

    # max(1, ...) ensures chunk_size is never 0 when the file has fewer rows than num_chunks,
    # which would otherwise cause an infinite list of empty slices
    chunk_size = max(1, total // num_chunks)
    
    # List comprehension walks the row list in steps of chunk_size;
    # the last slice naturally absorbs any remainder rows (integer division truncates)
    chunks = [rows[i: i + chunk_size] for i in range(0, total, chunk_size)]
    
    log.info(
        "SPLITTER: %d rows split into %d chunks (~%d rows each)",
        total, len(chunks), chunk_size,
    )
    return chunks
