"""
Fault-tolerance wrapper for worker functions.
"""

import time
import logging
from typing import Callable, Optional, Any

from .config import DEFAULT_RETRIES, DEFAULT_RETRY_DELAY

log = logging.getLogger(__name__)


def fault_tolerant_worker(
    fn: Callable,
    *args,
    retries: int = DEFAULT_RETRIES,
    delay: float = DEFAULT_RETRY_DELAY,
) -> Optional[Any]:
    """
    Wrap any worker function with retry-on-failure fault tolerance.

    If the worker raises an exception it is retried up to `retries`
    times with an exponential back-off.  This mimics the task-attempt
    mechanism used by Hadoop to handle transient failures in a cluster.

    Args:
        fn      : The worker callable to protect.
        *args   : Positional arguments forwarded to fn.
        retries : Maximum number of retry attempts.
        delay   : Initial wait between retries (doubles each attempt).

    Returns:
        The return value of fn(*args), or None if all retries fail.
    """
    attempt = 0
    wait = delay                       # initial sleep duration before the first retry
    # getattr() handles callables that lack __name__ (e.g. lambdas, functools.partial)
    fn_name = getattr(fn, "__name__", repr(fn))

    # loop runs at most retries+1 times (1 try + N retries)
    while attempt <= retries:
        try:
            return fn(*args)           # forward all positional args to the wrapped function
        except Exception as exc:       # catch any exception so transient errors don't kill the pipeline  # noqa: BLE001
            attempt += 1
            log.warning(
                "Worker %s failed (attempt %d/%d): %s",
                fn_name, attempt, retries, exc,
            )
            if attempt > retries:
                # All retry budget exhausted; log a permanent failure and signal caller via None
                log.error("Worker %s permanently failed.", fn_name)
                return None
            
            time.sleep(wait)    # back off before retrying to avoid hammering a failing resource
            wait *= 2           # exponential back-off: 0.5s -> 1s -> 2s ->...
