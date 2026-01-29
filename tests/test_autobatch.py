import asyncio
import unittest

from aioautobatch import autobatch


class AutoBatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_batches_within_start_delay(self) -> None:
        calls: list[list[tuple[int, int]]] = []

        async def batch_fn(args_list: list[tuple[int, int]]) -> list[int]:
            calls.append(list(args_list))
            return [a + b for a, b in args_list]

        fn = autobatch(batch_fn, start_delay=0.05)

        futures = await asyncio.gather(*(fn(i, i + 1) for i in range(3)))
        results = await asyncio.gather(*futures)

        self.assertEqual(results, [1, 3, 5])
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0], [(0, 1), (1, 2), (2, 3)])

    async def test_respects_batch_size(self) -> None:
        calls: list[list[tuple[int]]] = []

        async def batch_fn(args_list: list[tuple[int]]) -> list[int]:
            calls.append(list(args_list))
            return [args[0] for args in args_list]

        fn = autobatch(batch_fn, start_delay=0.05, batch_size=2, max_delay=0.01)

        futures = [await fn(i) for i in range(3)]
        results = await asyncio.gather(*futures)

        self.assertEqual(results, [0, 1, 2])
        self.assertEqual(len(calls), 2)
        self.assertEqual([len(batch) for batch in calls], [2, 1])

    async def test_limits_concurrent_batches(self) -> None:
        current = 0
        max_seen = 0

        async def batch_fn(args_list: list[tuple[int]]) -> list[int]:
            nonlocal current, max_seen
            current += 1
            max_seen = max(max_seen, current)
            await asyncio.sleep(0.05)
            current -= 1
            return [args[0] for args in args_list]

        fn = autobatch(batch_fn, batch_size=1, max_concurrent_batches=1)

        futures = await asyncio.gather(*(fn(i) for i in range(3)))
        results = await asyncio.gather(*futures)

        self.assertEqual(results, [0, 1, 2])
        self.assertEqual(max_seen, 1)

    async def test_exception_path_notifies_and_sets_future(self) -> None:
        caught: list[Exception] = []

        async def batch_fn(args_list: list[tuple[int]]) -> list[int]:
            raise ValueError("boom")

        def on_exception(exc: Exception) -> None:
            caught.append(exc)

        fn = autobatch(
            batch_fn, start_delay=0.02, batch_size=10, on_exception=on_exception
        )

        futures = await asyncio.gather(fn(1), fn(2))

        for fut in futures:
            with self.assertRaises(ValueError):
                await fut

        await asyncio.sleep(0)
        self.assertEqual(len(caught), 1)
        self.assertIsInstance(caught[0], ValueError)
