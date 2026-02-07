import asyncpg
from typing import Optional
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://postgres:postgrespassword@localhost:5432/mydb"
)

class Database:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
        self.connection_string = DATABASE_URL

    async def connect(self):
        try:
            self.pool = await asyncpg.create_pool(
                self.connection_string,
                min_size=1,
                max_size=10,
                timeout=30,
                command_timeout=5
            )
            async with self.pool.acquire() as conn:
                version = await conn.fetchval("SELECT version()")
                print(f"Connected to PostgreSQL: {version.split(',')[0]}")
        except Exception as e:
            print(f"Failed to connect to database: {e}")
            self.pool = None
            raise

    async def disconnect(self):
        if self.pool:
            await self.pool.close()
            self.pool = None
            print("Database disconnected")

    def is_connected(self):
        return self.pool is not None

db = Database()