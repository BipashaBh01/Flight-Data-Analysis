"""
Unit tests for the fault_tolerance module.

Verifies that the fault_tolerant_worker() wrapper correctly retries
failing functions, applies exponential back-off, and returns None
when all retry attempts are exhausted.
"""

import unittest
from unittest.mock import MagicMock, patch
from src.fault_tolerance import fault_tolerant_worker


class TestFaultTolerantWorker(unittest.TestCase):
    """
    Test suite for fault_tolerant_worker() function.

    The wrapper catches exceptions, retries up to N times with
    exponential back-off, and returns None on permanent failure.
    """

    def test_success_on_first_attempt(self):
        """
        A function that succeeds immediately should return its value
        without triggering any retries.
        """
        fn = MagicMock(return_value=42)
        result = fault_tolerant_worker(fn, "arg1", retries=3, delay=0.01)

        self.assertEqual(result, 42)
        fn.assert_called_once_with("arg1")

    def test_success_after_retry(self):
        """
        A function that fails once then succeeds should return its
        value after the retry.
        """
        fn = MagicMock(side_effect=[ValueError("transient"), 99])
        result = fault_tolerant_worker(fn, retries=3, delay=0.01)

        self.assertEqual(result, 99)
        self.assertEqual(fn.call_count, 2)

    def test_returns_none_after_all_retries_exhausted(self):
        """
        If the function fails on every attempt (initial + retries),
        the wrapper should return None rather than raising.
        """
        fn = MagicMock(side_effect=RuntimeError("permanent"))
        result = fault_tolerant_worker(fn, retries=2, delay=0.01)

        self.assertIsNone(result)
        # 1 initial attempt + 2 retries = 3 total calls
        self.assertEqual(fn.call_count, 3)

    def test_forwards_positional_args(self):
        """
        All positional arguments should be forwarded to the wrapped function.
        """
        fn = MagicMock(return_value="ok")
        fault_tolerant_worker(fn, "a", "b", "c", retries=1, delay=0.01)

        fn.assert_called_once_with("a", "b", "c")

    @patch("src.fault_tolerance.time.sleep")
    def test_exponential_backoff(self, mock_sleep):
        """
        Sleep durations should double on each successive retry,
        implementing exponential back-off (delay, delay*2, delay*4, …).
        """
        fn = MagicMock(side_effect=[
            ValueError("fail 1"),
            ValueError("fail 2"),
            ValueError("fail 3"),
            ValueError("fail 4"),
        ])
        fault_tolerant_worker(fn, retries=3, delay=1.0)

        # After 3 retries with delay=1.0: sleeps of 1.0, 2.0, 4.0
        sleep_calls = [call.args[0] for call in mock_sleep.call_args_list]
        self.assertEqual(sleep_calls, [1.0, 2.0, 4.0])

    def test_zero_retries_still_runs_once(self):
        """
        With retries=0 the function should execute exactly once.
        If it fails, return None immediately.
        """
        fn = MagicMock(side_effect=RuntimeError("boom"))
        result = fault_tolerant_worker(fn, retries=0, delay=0.01)

        self.assertIsNone(result)
        fn.assert_called_once()

    def test_zero_retries_success(self):
        """
        With retries=0 and no failure, normal return value is produced.
        """
        fn = MagicMock(return_value="hello")
        result = fault_tolerant_worker(fn, retries=0, delay=0.01)

        self.assertEqual(result, "hello")


if __name__ == "__main__":
    unittest.main()
