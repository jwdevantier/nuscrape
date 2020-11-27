import asyncio
from abc import abstractmethod, ABC
from enum import Enum
import time
from queue import Queue
import typing as t

import aiohttp
from pydantic import BaseModel


class ProxySettings(BaseModel):
    url: str
    basic_auth: t.Optional[aiohttp.BasicAuth]


class HttpMethod(Enum):
    GET = 1
    HEAD = 2
    POST = 3
    PUT = 4
    DELETE = 5
    CONNECT = 6
    OPTIONS = 7
    TRACE = 8
    PATCH = 9


@t.runtime_checkable
class SpiderParseFn(t.Protocol):
    def __call__(self, rq: "Request", **kwargs) -> t.AsyncIterator[t.Any]:
        ...


@t.runtime_checkable
class SpiderParseErrFn(t.Protocol):
    def __call__(self, rq: "Request", err: Exception):
        ...


# TODO: adjust as is necessary / convenient for aiohttp use
class Request(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    # Describe the request itself. These fields are used to determine equality
    method: HttpMethod
    url: str
    headers: t.Optional[t.Dict[str, str]] = None
    params: t.Optional[t.Dict[str, str]] = None
    body: t.Optional[str]

    # function to parse the request, if `None`, default parse method is used
    callback: t.Optional[SpiderParseFn] = None
    # arguments to pass along to the function parsing the request
    callback_kwargs: t.Optional[t.Dict[str, t.Any]] = None
    # in case of error - call this callback if provided
    errback: t.Optional[SpiderParseErrFn] = None

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__)
            and self.method == other.method
            and self.url == other.url
            and self.headers == other.headers
            and self.params == other.params
            and self.body == other.body
        )

    def __hash__(self):
        return hash(
            hash(self.method)
            + hash(self.url)
            + hash(self.headers)
            + hash(self.params)
            + hash(self.body)
        )


class Spider(ABC):
    def shutdown(self) -> None:
        """Called when scraper is shut down."""
        pass

    async def rq_delay_hook(self):
        """Called to determine how long to wait between requests."""
        return

    @abstractmethod
    async def start_requests(self) -> t.AsyncIterator[Request]:
        yield None

    @abstractmethod
    async def parse(self) -> t.AsyncIterator[t.Any]:
        yield None


class SpiderSchedulingPolicy(ABC):
    @abstractmethod
    def schedule_next(self, time_start: int, time_end: int) -> int:
        """Determine how many seconds to wait before (approximate) next run."""
        ...


class FixedSchedule(SpiderSchedulingPolicy):
    """Fixed schedule - run every `seconds` seconds - regardless of runtime."""

    def __init__(self, seconds: int):
        self._seconds = seconds

    def schedule_next(self, time_start: int, time_end: int) -> int:
        return max(0, time_end - time_start)


# TODO: change implementation to introduce a pipeline of PipelineStep's
@t.runtime_checkable
class PipelineStep(t.Protocol):
    def __call__(self, item: t.Any) -> t.Any:
        ...


# TODO: may need a way to examine and stop following certain requests
# TODO: probably want to support a generic error-handler - retry X times, log or so.
class SpiderRunner(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    spider: Spider
    scheduler: SpiderSchedulingPolicy
    proxy: t.Optional[ProxySettings]
    on_item: PipelineStep

    def __init__(self, **kwargs):
        super(SpiderRunner, self).__init__(**kwargs)
        self.__task: t.Optional[asyncio.Task] = None

    async def __handle_request(self, rq: Request) -> t.AsyncIterator[t.Any]:
        proxy_settings = (
            self.proxy.dict() if self.proxy else {}
        )  # TODO: cache somewhere
        async with aiohttp.ClientSession() as session:
            await self.spider.rq_delay_hook()
            async with getattr(session, rq.method.name.lower())(
                url=rq.url,
                headers=rq.headers,
                params=rq.params,
                body=rq.body,
                **proxy_settings
            ) as rsp:
                cb = rq.callback or self.spider.parse
                async for result in cb(rsp, **rq.callback_kwargs):
                    yield result
                # TODO: rq errback -- when should it be used...?

    async def __run_loop(self):
        """Periodically schedule the spider to run until stopped."""
        try:
            while True:
                start = int(time.time())
                pending = Queue()
                visited_rq_hashes: t.Set[int] = set()
                async for rq in self.spider.start_requests():
                    pending.put(rq)
                    while not pending.empty():
                        rq: Request = pending.get()
                        rq_hash = hash(rq)
                        if rq_hash in visited_rq_hashes:
                            continue
                        visited_rq_hashes.add(rq_hash)
                        async for result in self.__handle_request(rq):
                            if isinstance(result, Request):
                                pending.put(result)
                            else:
                                # TODO: revamp to use a pipeline
                                self.on_item(result)
                end = int(time.time())
                await asyncio.sleep(self.scheduler.schedule_next(start, end))
        except asyncio.CancelledError:
            # TODO: study up on cancelling -- want to mark cancels from inside
            #       the spider as errors - but stopping on external cancels.
            self.spider.shutdown()

    async def start(self):
        """Start spider."""
        if self.__task:
            return
        self.__task = asyncio.create_task(self.__run_loop())

    async def stop(self):
        """Stop spider."""
        self.__task.cancel()
        self.__task = None
