# aioautobatch

Lightweight asyncio helper that batches many small calls into fewer async batch calls. It wraps your batch function and returns an awaitable that resolves with the corresponding result.

## Install

```bash
pip install .
```

Python 3.11+ is required; the project targets Python 3.14 in `pyproject.toml`.

## Usage

```python
import asyncio
from aioautobatch import autobatch


async def batch_add(args_list: list[tuple[int, int]]) -> list[int]:
    # Called once per batch with all pending arguments.
    return [a + b for a, b in args_list]


async def main():
    add = autobatch(batch_add, start_delay=0.05, batch_size=5)

    # Each call returns a future tied to its position in the batch.
    futures = [await add(i, i + 1) for i in range(8)]
    results = await asyncio.gather(*futures)
    print(results)  # [1, 3, 5, 7, 9, 11, 13, 15]


asyncio.run(main())
```

### Key parameters

- `start_delay`: initial window (seconds) to collect the first batch.
- `batch_size`: maximum items per batch; `None` means unbounded.
- `max_delay`: max wait between batches after the first batch starts.
- `max_concurrent_batches`: cap concurrent in-flight batches.
- `on_exception`: optional callback invoked when a batch raises.

## Tests

```bash
python3 -m unittest discover -s tests
```

Unit tests cover batching behavior, batch sizing, concurrency limits, and exception propagation.
