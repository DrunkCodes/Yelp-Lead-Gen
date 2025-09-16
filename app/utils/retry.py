"""
Asynchronous retry utilities with exponential backoff and jitter.
"""

import asyncio
import functools
import logging
import random
import time
from typing import Any, Callable, Coroutine, Optional, Type, TypeVar, Union, cast

# Type variables for better type hinting
T = TypeVar('T')
ExceptionHandler = Callable[[Exception, int], Coroutine[Any, Any, None]]
ExceptionTypes = Union[Type[Exception], tuple[Type[Exception], ...]]

logger = logging.getLogger(__name__)


async def retry_async(
    fn: Callable[..., Coroutine[Any, Any, T]],
    *args: Any,
    max_tries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    jitter: bool = True,
    exceptions: ExceptionTypes = Exception,
    on_exception: Optional[ExceptionHandler] = None,
    **kwargs: Any
) -> T:
    """
    Retry an async function with exponential backoff and jitter.
    
    Args:
        fn: The async function to retry
        *args: Positional arguments to pass to the function
        max_tries: Maximum number of retry attempts (default: 3)
        base_delay: Initial delay between retries in seconds (default: 1.0)
        max_delay: Maximum delay between retries in seconds (default: 30.0)
        jitter: Whether to add random jitter to the delay (default: True)
        exceptions: Exception or tuple of exceptions to catch and retry on
        on_exception: Optional callback function to call when an exception occurs
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        The result of the function if successful
        
    Raises:
        The last exception encountered if all retries fail
    """
    last_exception = None
    
    for attempt in range(1, max_tries + 1):
        try:
            return await fn(*args, **kwargs)
        except exceptions as e:
            last_exception = e
            
            # If this was the last attempt, re-raise the exception
            if attempt == max_tries:
                logger.warning(f"Final retry attempt ({attempt}/{max_tries}) failed: {e}")
                raise
            
            # Calculate delay with exponential backoff
            delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
            
            # Add jitter if enabled (±25% of delay)
            if jitter:
                delay = delay * (0.75 + random.random() * 0.5)
            
            # Call the exception handler if provided
            if on_exception:
                try:
                    await on_exception(e, attempt)
                except Exception as callback_error:
                    logger.warning(f"Exception handler failed: {callback_error}")
            
            logger.info(f"Retry attempt {attempt}/{max_tries} failed: {e}. Retrying in {delay:.2f}s")
            await asyncio.sleep(delay)
    
    # This should never happen due to the re-raise above, but just in case
    assert last_exception is not None
    raise last_exception


def with_retry(
    max_tries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    jitter: bool = True,
    exceptions: ExceptionTypes = Exception,
    on_exception: Optional[ExceptionHandler] = None
) -> Callable[[Callable[..., Coroutine[Any, Any, T]]], Callable[..., Coroutine[Any, Any, T]]]:
    """
    Decorator to retry an async function with exponential backoff and jitter.
    
    Args:
        max_tries: Maximum number of retry attempts (default: 3)
        base_delay: Initial delay between retries in seconds (default: 1.0)
        max_delay: Maximum delay between retries in seconds (default: 30.0)
        jitter: Whether to add random jitter to the delay (default: True)
        exceptions: Exception or tuple of exceptions to catch and retry on
        on_exception: Optional callback function to call when an exception occurs
        
    Returns:
        A decorator function that wraps the original async function with retry logic
    """
    def decorator(
        fn: Callable[..., Coroutine[Any, Any, T]]
    ) -> Callable[..., Coroutine[Any, Any, T]]:
        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            return await retry_async(
                fn,
                *args,
                max_tries=max_tries,
                base_delay=base_delay,
                max_delay=max_delay,
                jitter=jitter,
                exceptions=exceptions,
                on_exception=on_exception,
                **kwargs
            )
        return wrapper
    return decorator


class RetryContext:
    """
    Context class to track retry state across multiple retry attempts.
    Useful for implementing more complex retry logic or tracking metrics.
    """
    def __init__(self) -> None:
        self.attempts: int = 0
        self.start_time: float = time.time()
        self.last_exception: Optional[Exception] = None
        self.last_delay: float = 0
        self.total_delay: float = 0
    
    @property
    def elapsed(self) -> float:
        """Total elapsed time since first attempt in seconds."""
        return time.time() - self.start_time
    
    def record_attempt(self, exception: Optional[Exception], delay: float) -> None:
        """Record information about a retry attempt."""
        self.attempts += 1
        self.last_exception = exception
        self.last_delay = delay
        self.total_delay += delay
    
    def __str__(self) -> str:
        return (
            f"RetryContext(attempts={self.attempts}, "
            f"elapsed={self.elapsed:.2f}s, "
            f"total_delay={self.total_delay:.2f}s)"
        )


async def retry_with_context(
    fn: Callable[..., Coroutine[Any, Any, T]],
    *args: Any,
    context: Optional[RetryContext] = None,
    max_tries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    jitter: bool = True,
    exceptions: ExceptionTypes = Exception,
    on_exception: Optional[ExceptionHandler] = None,
    **kwargs: Any
) -> T:
    """
    Retry an async function with a retry context for more detailed tracking.
    
    Args:
        fn: The async function to retry
        *args: Positional arguments to pass to the function
        context: Optional RetryContext to track retry state (created if None)
        max_tries: Maximum number of retry attempts (default: 3)
        base_delay: Initial delay between retries in seconds (default: 1.0)
        max_delay: Maximum delay between retries in seconds (default: 30.0)
        jitter: Whether to add random jitter to the delay (default: True)
        exceptions: Exception or tuple of exceptions to catch and retry on
        on_exception: Optional callback function to call when an exception occurs
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        The result of the function if successful
        
    Raises:
        The last exception encountered if all retries fail
    """
    ctx = context or RetryContext()
    
    for attempt in range(1, max_tries + 1):
        try:
            return await fn(*args, **kwargs)
        except exceptions as e:
            # If this was the last attempt, re-raise the exception
            if attempt == max_tries:
                logger.warning(f"Final retry attempt ({attempt}/{max_tries}) failed: {e}")
                ctx.record_attempt(e, 0)
                raise
            
            # Calculate delay with exponential backoff
            delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
            
            # Add jitter if enabled (±25% of delay)
            if jitter:
                delay = delay * (0.75 + random.random() * 0.5)
            
            # Call the exception handler if provided
            if on_exception:
                try:
                    await on_exception(e, attempt)
                except Exception as callback_error:
                    logger.warning(f"Exception handler failed: {callback_error}")
            
            logger.info(f"Retry attempt {attempt}/{max_tries} failed: {e}. Retrying in {delay:.2f}s")
            ctx.record_attempt(e, delay)
            await asyncio.sleep(delay)
    
    # This should never happen due to the re-raise above, but just in case
    raise RuntimeError("Unexpected end of retry loop")
