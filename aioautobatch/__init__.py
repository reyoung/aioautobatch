import typing
import dataclasses
import asyncio
import inspect

__all__ = ["autobatch"]

Ts = typing.TypeVarTuple("Ts")
R = typing.TypeVar("R")


class Fn(typing.Protocol[*Ts, R]):
    async def __call__(self, *args: typing.Unpack[Ts]) -> asyncio.Future[R]: ...


BatchFn: typing.TypeAlias = (
    typing.Callable[
        [list[tuple[typing.Unpack[Ts]]]],
        typing.Awaitable[list[R]],
    ]
    | typing.Callable[
        [list[tuple[typing.Unpack[Ts]]], list[asyncio.Future[R]]],
        typing.Awaitable[None],
    ]
)


@dataclasses.dataclass(frozen=True)
class _Job(typing.Generic[*Ts, R]):
    args: tuple[typing.Unpack[Ts]]
    future: asyncio.Future[R]
    created_at: float = dataclasses.field(
        default_factory=lambda: asyncio.get_running_loop().time()
    )


class _AutoBatcher(typing.Generic[*Ts, R]):
    def __init__(
        self,
        batch_fn: BatchFn[*Ts, R],
        start_delay: float,
        batch_size: int | None = None,
        max_delay: float = 1.0,
        max_concurrent_batches: int | None = None,
        on_exception: typing.Callable[[BaseException], None] | None = None,
    ) -> None:
        self._loop_task: None | asyncio.Task[None] = None
        self._batch_fn: BatchFn[*Ts, R] = batch_fn
        self._start_delay = start_delay
        self._batch_size = batch_size
        self._max_delay = max_delay
        self._batch_semaphore: asyncio.Semaphore | None = None
        if max_concurrent_batches is not None:
            self._batch_semaphore = asyncio.Semaphore(max_concurrent_batches)
        self._job_queue: asyncio.Queue[_Job[*Ts, R]] = asyncio.Queue()
        self._on_exception = on_exception
        self._accepts_future_list = _accepts_future_list(batch_fn)

    async def __call__(self, *args: typing.Unpack[Ts]) -> asyncio.Future[R]:
        job: _Job[*Ts, R] = _Job(
            args=args, future=asyncio.get_running_loop().create_future()
        )
        await self._job_queue.put(job)

        if self._loop_task is None:
            self._loop_task = asyncio.create_task(self._loop())

        return job.future

    async def _loop(self) -> None:
        try:
            await self._loop_main()
        finally:
            self._loop_task = None

    async def _loop_main(self) -> None:
        first = True
        while True:
            if self._job_queue.empty():
                # No jobs, wait for one
                return

            jobs: list[_Job[*Ts, R]] = []
            first_job = await self._job_queue.get()
            jobs.append(first_job)
            delay = self._start_delay if first else self._max_delay
            first = False
            await self._fetch_batch(jobs, delay)
            await self._process_batch(jobs)

    async def _process_batch(self, jobs: list[_Job[*Ts, R]]) -> None:
        if self._batch_semaphore is not None:
            await self._batch_semaphore.acquire()

        task = asyncio.create_task(self._execute_batch(jobs))
        task.add_done_callback(self._on_batch_done)

    async def _execute_batch(self, jobs: list[_Job[*Ts, R]]) -> None:
        args_list: list[tuple[typing.Unpack[Ts]]] = []
        futs: list[asyncio.Future[R]] = []
        for job in jobs:
            if job.future.cancelled():
                continue
            args_list.append(job.args)
            futs.append(job.future)
        try:
            if self._accepts_future_list:
                res_list = None
                await self._batch_fn(args_list, futs)
            else:
                res_list = await self._batch_fn(args_list)
        except Exception as e:
            for fut in futs:
                if not fut.cancelled() and not fut.done():
                    fut.set_exception(e)
            raise
        else:
            self._apply_results(futs, res_list)

    def _on_batch_done(self, task: asyncio.Task[None]) -> None:
        if self._batch_semaphore is not None:
            self._batch_semaphore.release()
        
        if task.cancelled() and self._on_exception is not None:
            # cancelled tasks do not have exceptions, so we create a generic one
            self._on_exception(asyncio.CancelledError())
            return
        
        exc = task.exception()
        if exc is not None and self._on_exception is not None:
            self._on_exception(exc)

    async def _fetch_batch(self, jobs: list[_Job[*Ts, R]], delay: float) -> None:
        begin = jobs[0].created_at

        while self._batch_size is None or len(jobs) < self._batch_size:
            now = asyncio.get_running_loop().time()
            elapsed = now - begin
            to_wait = delay - elapsed
            if to_wait <= 0:
                break

            try:
                job = await asyncio.wait_for(self._job_queue.get(), timeout=to_wait)
            except asyncio.TimeoutError:
                break

            jobs.append(job)

    @staticmethod
    def _apply_results(
        futs: list[asyncio.Future[R]], results: typing.Iterable[R] | None
    ) -> None:
        if results is None:
            return

        for fut, res in zip(futs, results):
            if fut.cancelled() or fut.done():
                continue
            fut.set_result(res)


def _accepts_future_list(batch_fn: typing.Callable[..., typing.Any]) -> bool:
    try:
        sig = inspect.signature(batch_fn)
    except (TypeError, ValueError):
        return False

    try:
        sig.bind_partial([], [])
    except TypeError:
        return False
    else:
        return True


def autobatch(
    batch_fn: BatchFn[*Ts, R],
    start_delay: float = 0,
    batch_size: int | None = None,
    max_delay: float = 1.0,
    max_concurrent_batches: int | None = None,
    on_exception: typing.Callable[[Exception], None] | None = None,
) -> Fn[*Ts, R]:
    return _AutoBatcher[*Ts, R](
        batch_fn,
        start_delay=start_delay,
        batch_size=batch_size,
        max_delay=max_delay,
        max_concurrent_batches=max_concurrent_batches,
        on_exception=on_exception,
    )
