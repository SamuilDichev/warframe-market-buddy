import asyncpg


class AsyncDatabaseConnection:
    def __init__(self, host, port, user, database):
        self.connection = None
        self.host = host
        self.port = port
        self.user = user
        self.database = database

    async def __aenter__(self):
        self.connection = await asyncpg.connect(host=self.host, port=self.port, user=self.user, database=self.database)
        return self.connection

    async def __aexit__(self, *_):
        await self.connection.close()
