import dataclasses
from dataclasses import dataclass
from typing import Any, Callable, Dict, Generic, List, Optional, TypeVar
from vine import promise  # type: ignore

from .exceptions import WrongNumberOfResultsReturned


T = TypeVar("T")
K = TypeVar("K")


@dataclass
class LoaderTask(Generic[K, T]):
    key: K
    future: promise


@dataclass
class Batch(Generic[K, T]):
    tasks: List[LoaderTask] = dataclasses.field(default_factory=list)
    dispatched: bool = False

    def add_task(self, key: Any, future: promise):
        task = LoaderTask[K, T](key, future)
        self.tasks.append(task)

    def __len__(self) -> int:
        return len(self.tasks)


class SyncDataLoader(Generic[K, T]):
    queue: List[LoaderTask]
    batches: List[Batch[K, T]]
    cache: bool = False
    cache_map: Dict[K, promise]

    def __init__(
        self,
        load_fn: Any,
        max_batch_size: Optional[int] = None,
        cache: bool = True,
    ):
        self.load_fn = load_fn
        self.max_batch_size = max_batch_size

        self.cache = cache
        self.batches = []
        self.queue = []

        if self.cache:
            self.cache_map = {}

    def load(self, key: K):  # -> promise[T]:
        if self.cache:
            future = self.cache_map.get(key)

            if future:
                return future

        future = promise()

        if self.cache:
            self.cache_map[key] = future

        batch = get_current_batch(self)
        batch.add_task(key, future)

        return future

    @property
    def is_ready(self):
        return all([batch.dispatched for batch in self.batches])


def should_create_new_batch(loader: SyncDataLoader, batch: Batch) -> bool:
    if (
        batch.dispatched
        or loader.max_batch_size
        and len(batch) >= loader.max_batch_size
    ):
        return True

    return False


def get_current_batch(loader: SyncDataLoader) -> Batch:
    latest_batch: Optional[Batch]
    try:
        latest_batch = loader.batches[-1]
    except IndexError:
        latest_batch = None

    if latest_batch and not should_create_new_batch(loader, latest_batch):
        return latest_batch

    loader.batches.append(Batch())

    return loader.batches[-1]


def dispatch(loader: SyncDataLoader):
    for batch in loader.batches:
        if not batch.dispatched:
            dispatch_batch(loader, batch)


def dispatch_batch(loader: SyncDataLoader, batch: Batch) -> None:
    batch.dispatched = True

    keys = [task.key for task in batch.tasks]

    # TODO: check if load_fn return an awaitable and it is a list

    try:
        values = loader.load_fn(keys)
        values = list(values)

        if len(values) != len(batch):
            raise WrongNumberOfResultsReturned(
                expected=len(batch), received=len(values)
            )

        for task, value in zip(batch.tasks, values):
            if isinstance(value, BaseException):
                task.future.throw(value)
            else:
                task.future(value)
    except Exception as e:
        for task in batch.tasks:
            task.future.throw(e)
