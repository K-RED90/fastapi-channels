"""Utilities for batch processing, concurrency control, and streaming iterators.

This module provides helper functions and classes for efficient batch processing
of large datasets, concurrency limiting, and memory-efficient streaming operations.
"""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Iterator
from dataclasses import dataclass, field
from itertools import islice
from typing import Any, Generic, TypeVar

T = TypeVar("T")
R = TypeVar("R")


def batch_items(
    items: Iterable[T], batch_size: int, strict: bool = False
) -> Iterator[tuple[T, ...]]:
    """Split an iterable into batches of specified size.

    Parameters
    ----------
    items : Iterable[T]
        Items to split into batches
    batch_size : int
        Maximum number of items per batch

    Yields
    ------
    tuple[T, ...]
        Batch of items (last batch may be smaller)

    Examples
    --------
    >>> list(batch_items([1, 2, 3, 4, 5], batch_size=2))
    [(1, 2), (3, 4), (5,)]

    """
    if batch_size < 1:
        raise ValueError("n must be at least one")
    iterator = iter(items)
    while batch := tuple(islice(iterator, batch_size)):
        if strict and len(batch) != batch_size:
            raise ValueError("batch_items(): incomplete batch")
        yield batch


class Semaphore:
    """Wrapper around asyncio.Semaphore with context manager support.

    Provides a simple interface for limiting concurrent operations.

    Parameters
    ----------
    max_concurrent : int
        Maximum number of concurrent operations allowed

    Examples
    --------
    >>> sem = Semaphore(max_concurrent=10)
    >>> async with sem:
    ...     await perform_operation()

    """

    def __init__(self, max_concurrent: int):
        self._semaphore = asyncio.Semaphore(max_concurrent)

    async def __aenter__(self) -> "Semaphore":
        await self._semaphore.acquire()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._semaphore.release()

    async def acquire(self) -> None:
        """Acquire the semaphore."""
        await self._semaphore.acquire()

    def release(self) -> None:
        """Release the semaphore."""
        self._semaphore.release()


async def run_with_concurrency_limit(
    tasks: Iterable[Awaitable[T]],
    max_concurrent: int,
    return_exceptions: bool = True,
) -> list[T | BaseException]:
    """Run async tasks with a concurrency limit.

    Parameters
    ----------
    tasks : Iterable[Awaitable[T]]
        Async tasks to run
    max_concurrent : int
        Maximum number of concurrent tasks
    return_exceptions : bool, optional
        If True, exceptions are returned as results. Default: True

    Returns
    -------
    list[T | BaseException]
        Results from all tasks (may include exceptions)

    Examples
    --------
    >>> tasks = [fetch(url) for url in urls]
    >>> results = await run_with_concurrency_limit(tasks, max_concurrent=10)

    """
    semaphore = Semaphore(max_concurrent)

    async def run_with_sem(task: Awaitable[T]) -> T:
        async with semaphore:
            return await task

    wrapped_tasks = [run_with_sem(task) for task in tasks]
    return await asyncio.gather(*wrapped_tasks, return_exceptions=return_exceptions)


async def process_in_batches(
    items: Iterable[T],
    processor: Callable[[T], Awaitable[R]],
    batch_size: int = 100,
    max_concurrent: int = 10,
    return_exceptions: bool = True,
) -> list[R | BaseException]:
    """Process items in batches with concurrency control.

    Splits items into batches and processes each batch concurrently,
    with a limit on the number of concurrent operations.

    Parameters
    ----------
    items : Iterable[T]
        Items to process
    processor : Callable[[T], Awaitable[R]]
        Async function to process each item
    batch_size : int, optional
        Number of items per batch. Default: 100
    max_concurrent : int, optional
        Maximum concurrent operations within each batch. Default: 10
    return_exceptions : bool, optional
        If True, exceptions are returned as results. Default: True

    Returns
    -------
    list[R | BaseException]
        Results from all items (may include exceptions)

    Examples
    --------
    >>> async def send_message(channel):
    ...     await backend.publish(channel, {"type": "ping"})
    ...
    >>> results = await process_in_batches(
    ...     channels,
    ...     send_message,
    ...     batch_size=50,
    ...     max_concurrent=10
    ... )

    """
    results: list[R | BaseException] = []

    for batch in batch_items(items, batch_size):
        tasks = [processor(item) for item in batch]
        batch_results = await run_with_concurrency_limit(
            tasks, max_concurrent=max_concurrent, return_exceptions=return_exceptions
        )
        results.extend(batch_results)

    return results


@dataclass
class BatchProcessor(Generic[T, R]):
    """Configurable batch processor with progress tracking.

    Provides a reusable interface for processing items in batches
    with configurable concurrency, error handling, and progress callbacks.

    Parameters
    ----------
    processor : Callable[[T], Awaitable[R]]
        Async function to process each item
    batch_size : int, optional
        Number of items per batch. Default: 100
    max_concurrent : int, optional
        Maximum concurrent operations. Default: 10
    on_batch_complete : Callable[[int, int], Awaitable[None]] | None, optional
        Callback after each batch completes (batch_number, total_batches). Default: None
    on_error : Callable[[T, Exception], Awaitable[None]] | None, optional
        Callback when processing an item fails. Default: None
    continue_on_error : bool, optional
        Continue processing if an item fails. Default: True

    Examples
    --------
    >>> async def send_to_user(user_id):
    ...     await notify_user(user_id, message)
    ...
    >>> processor = BatchProcessor(
    ...     processor=send_to_user,
    ...     batch_size=50,
    ...     max_concurrent=5,
    ... )
    >>> results = await processor.process(user_ids)

    """

    processor: Callable[[T], Awaitable[R]]
    batch_size: int = 100
    max_concurrent: int = 10
    on_batch_complete: Callable[[int, int], Awaitable[None]] | None = None
    on_error: Callable[[T, Exception], Awaitable[None]] | None = None
    continue_on_error: bool = True
    _processed_count: int = field(default=0, init=False)
    _error_count: int = field(default=0, init=False)

    @property
    def processed_count(self) -> int:
        """Number of items successfully processed."""
        return self._processed_count

    @property
    def error_count(self) -> int:
        """Number of items that failed processing."""
        return self._error_count

    def reset_counts(self) -> None:
        """Reset processing statistics."""
        self._processed_count = 0
        self._error_count = 0

    async def process(self, items: Iterable[T]) -> list[R | None]:
        """Process all items in batches.

        Parameters
        ----------
        items : Iterable[T]
            Items to process

        Returns
        -------
        list[R | None]
            Results from processing (None for failed items if continue_on_error)

        """
        self.reset_counts()
        items_list = list(items)
        batches = list(batch_items(items_list, self.batch_size))
        total_batches = len(batches)
        results: list[R | None] = []

        semaphore = Semaphore(self.max_concurrent)

        async def process_item(item: T) -> R | None:
            async with semaphore:
                try:
                    result = await self.processor(item)
                    self._processed_count += 1
                    return result
                except Exception as e:
                    self._error_count += 1
                    if self.on_error:
                        await self.on_error(item, e)
                    if not self.continue_on_error:
                        raise
                    return None

        for batch_num, batch in enumerate(batches, 1):
            tasks = [process_item(item) for item in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=False)
            results.extend(batch_results)

            if self.on_batch_complete:
                await self.on_batch_complete(batch_num, total_batches)

        return results

    async def process_stream(self, items: AsyncIterator[list[T]]) -> AsyncIterator[list[R | None]]:
        """Process items from an async stream.

        Parameters
        ----------
        items : AsyncIterator[list[T]]
            Async iterator yielding batches of items

        Yields
        ------
        list[R | None]
            Results from each batch

        """
        self.reset_counts()
        semaphore = Semaphore(self.max_concurrent)

        async def process_item(item: T) -> R | None:
            async with semaphore:
                try:
                    result = await self.processor(item)
                    self._processed_count += 1
                    return result
                except Exception as e:
                    self._error_count += 1
                    if self.on_error:
                        await self.on_error(item, e)
                    if not self.continue_on_error:
                        raise
                    return None

        async for batch in items:
            tasks = [process_item(item) for item in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=False)
            yield batch_results
