import pymongo

from taskiq import TaskiqResult, compat
from taskiq.abc import AsyncResultBackend
from taskiq.exceptions import ResultBackendError, ResultGetError, TaskiqError

class NoResultError(ResultGetError, TaskiqError):
    pass


class AsyncMongoResultBackend[T](AsyncResultBackend[T]):
    _mongo: pymongo.AsyncMongoClient
    _database: str
    _collection: str

    def __init__(
            self,
            mongo: pymongo.AsyncMongoClient | None = None,
            url: str | None = None,
            database: str | None = None,
            collection: str | None = None,
    ):
        if mongo is not None:
            self._mongo = mongo
        elif url is not None:
            self._mongo = pymongo.AsyncMongoClient(url)
        else:
            raise ValueError("Must provide either mongo client or url")

        self._database = database or "taskiq"
        self._collection = collection or "taskiq-results"

    async def startup(self) -> None:
        await self._mongo.aconnect()

    async def shutdown(self) -> None:
        await self._mongo.close()

    async def set_result(self, task_id: str, result: TaskiqResult[T]) -> None:
        col = self._mongo[self._database][self._collection]
        i = await col.find_one({"task_id": task_id})
        if i is None:
            await col.insert_one({"task_id": task_id, "result": compat.model_dump(result)})

    async def is_result_ready(self, task_id: str) -> bool:
        col = self._mongo[self._database][self._collection]
        i = await col.find_one({"task_id": task_id})
        return i is not None

    async def get_result(self, task_id: str, with_logs: bool = False) -> TaskiqResult[T]:
        col = self._mongo[self._database][self._collection]
        i = await col.find_one({"task_id": task_id})
        if i is None:
            raise NoResultError

        task_result = compat.model_validate(
            TaskiqResult[T],
            dict(i),
        )

        if not with_logs:
            task_result.log = None

        return task_result