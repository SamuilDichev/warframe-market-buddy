import asyncpg


class AsyncDatabaseConnection:
    def __init__(self, **db_kwargs):
        self.connection = None
        self._db_kwargs = db_kwargs

    async def __aenter__(self):
        self.connection = await asyncpg.connect(**self._db_kwargs)
        return self.connection

    async def __aexit__(self, *_):
        if self.connection is not None:
            await self.connection.close()
