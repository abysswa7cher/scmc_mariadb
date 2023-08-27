import aiohttp, asyncio, time

class ThrottledClientSession(aiohttp.ClientSession):
    """
    Rate-throttled client session class inherited from aiohttp.ClientSession)

    USAGE:
        replace `session = aiohttp.ClientSession()`
        with `session = ThrottledClientSession(rate_limit=15)`

    see https://stackoverflow.com/a/60357775/107049
    """

    MIN_SLEEP = 0.1

    def __init__(self, rate_limit: float = None, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.rate_limit = rate_limit
        self._fillerTask = None
        self._queue = None
        self._start_time = time.time()
        if rate_limit is not None:
            if rate_limit <= 0:
                raise ValueError('rate_limit must be positive')
            self._queue = asyncio.Queue(min(2, int(rate_limit) + 1))
            self._fillerTask = asyncio.create_task(self._filler(rate_limit))

    #def _get_sleep(self) -> Optional[float]:
    def _get_sleep(self) -> None | float:
        if self.rate_limit is not None:
            return max(1 / self.rate_limit, self.MIN_SLEEP)
        return None

    async def close(self) -> None:
        """Close rate-limiter's "bucket filler" task"""
        if self._fillerTask is not None:
            self._fillerTask.cancel()
        try:
            await asyncio.wait_for(self._fillerTask, timeout=0.5)
        except asyncio.TimeoutError as err:
            print(str(err))
        await super().close()

    async def _filler(self, rate_limit: float = 1):
        """Filler task to fill the leaky bucket algo"""
        try:
            if self._queue is None:
                return
            self.rate_limit = rate_limit
            sleep = self._get_sleep()
            updated_at = time.monotonic()
            fraction = 0
            extra_increment = 0
            for i in range(0, self._queue.maxsize):
                self._queue.put_nowait(i)
            while True:
                if not self._queue.full():
                    now = time.monotonic()
                    increment = rate_limit * (now - updated_at)
                    fraction += increment % 1
                    extra_increment = fraction // 1
                    items_2_add = int(min(self._queue.maxsize - self._queue.qsize(), int(increment) + extra_increment))
                    fraction = fraction % 1
                    for i in range(0, items_2_add):
                        self._queue.put_nowait(i)
                    updated_at = now
                await asyncio.sleep(sleep)
        except asyncio.CancelledError:
            print('Cancelled')
        except Exception as err:
            print(str(err))

    async def _allow(self) -> None:
        if self._queue is not None:
            # debug
            # if self._start_time == None:
            #    self._start_time = time.time()
            await self._queue.get()
            self._queue.task_done()
        return None

    async def _request(self, *args, **kwargs)  -> aiohttp.ClientResponse:
        """Throttled _request()"""
        await self._allow()
        return await super()._request(*args, **kwargs)