import asyncio
import functools
import inspect
import logging
import random
import time
from collections.abc import Callable
from typing import ParamSpec, TypeVar

P = ParamSpec("P")
T = TypeVar("T")


def with_retry(
    max_retries: int = 3,
    base_delay: float = 0.1,
    max_delay: float = 2.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Decorator for functions (sync or async) to add retry logic with exponential backoff.

    Parameters
    ----------
    max_retries : int, optional
        Maximum number of retry attempts. Default: 3
    base_delay : float, optional
        Initial delay between retries in seconds. Default: 0.1
    max_delay : float, optional
        Maximum delay between retries in seconds. Default: 2.0
    exponential_base : float, optional
        Base for exponential backoff calculation. Default: 2.0
    jitter : bool, optional
        Add random jitter to delays to prevent thundering herd. Default: True
    exceptions : tuple[type[Exception], ...], optional
        Tuple of exception types to catch and retry on. Default: (Exception,)

    Returns
    -------
    Callable
        Decorated function (sync or async) with retry logic

    Examples
    --------
    >>> @with_retry(max_retries=3, base_delay=0.1)
    ... async def fetch_data():
    ...     return await some_async_operation()

    >>> @with_retry(max_retries=3, base_delay=0.1)
    ... def fetch_data_sync():
    ...     return some_sync_operation()

    """
    logger = logging.getLogger(__name__)

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        is_async = inspect.iscoroutinefunction(func)

        if is_async:

            @functools.wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
                last_exception: Exception | None = None

                for attempt in range(max_retries + 1):
                    try:
                        return await func(*args, **kwargs)
                    except exceptions as e:
                        last_exception = e

                        if attempt == max_retries:
                            logger.error(
                                "Operation failed after %d retries: %s",
                                max_retries,
                                str(e),
                            )
                            raise

                        delay = min(base_delay * (exponential_base**attempt), max_delay)

                        # Add jitter to prevent thundering herd
                        if jitter:
                            delay = delay * (0.5 + random.random())

                        logger.warning(
                            "Operation failed (attempt %d/%d), retrying in %.2fs: %s",
                            attempt + 1,
                            max_retries + 1,
                            delay,
                            str(e),
                        )

                        await asyncio.sleep(delay)

                # This should never be reached, but just in case
                if last_exception:
                    raise last_exception
                raise RuntimeError("Unexpected retry loop exit")

            return async_wrapper  # type: ignore[return-value]

        @functools.wraps(func)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            last_exception: Exception | None = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e

                    if attempt == max_retries:
                        logger.error(
                            "Operation failed after %d retries: %s",
                            max_retries,
                            str(e),
                        )
                        raise
                    delay = min(base_delay * (exponential_base**attempt), max_delay)

                    # Add jitter to prevent thundering herd
                    if jitter:
                        delay = delay * (0.5 + random.random())

                    logger.warning(
                        "Operation failed (attempt %d/%d), retrying in %.2fs: %s",
                        attempt + 1,
                        max_retries + 1,
                        delay,
                        str(e),
                    )

                    time.sleep(delay)

            # This should never be reached, but just in case
            if last_exception:
                raise last_exception
            raise RuntimeError("Unexpected retry loop exit")

        return sync_wrapper

    return decorator
