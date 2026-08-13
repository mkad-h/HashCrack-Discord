import aiosqlite
import uuid
import time
from enum import Enum
from typing import Optional, List, Dict, Any
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "jobs.db"


class JobStatus(str, Enum):
    QUEUED = "queued"
    STARTING = "starting"
    RUNNING = "running"
    CRACKED = "cracked"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


class JobManager:
    def __init__(self, db_path: str = str(DB_PATH)):
        self.db_path = db_path

    async def init_db(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    channel_id INTEGER NOT NULL,
                    message_id INTEGER,
                    hash_value TEXT NOT NULL,
                    mode INTEGER NOT NULL,
                    wordlist TEXT NOT NULL,
                    rules TEXT,
                    status TEXT NOT NULL,
                    instance_id TEXT,
                    ssh_host TEXT,
                    ssh_port INTEGER,
                    result TEXT,
                    error TEXT,
                    progress TEXT,
                    notified INTEGER DEFAULT 0,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    started_at REAL,
                    finished_at REAL
                )
            """)
            # Pool de instancias calientes (reutilizables)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS warm_instances (
                    instance_id TEXT PRIMARY KEY,
                    ssh_host TEXT NOT NULL,
                    ssh_port INTEGER NOT NULL,
                    wordlist_path TEXT,
                    status TEXT NOT NULL,
                    last_used_at REAL NOT NULL,
                    created_at REAL NOT NULL
                )
            """)
            # Migración suave si la DB ya existía sin columnas nuevas
            for col, typedef in [("progress", "TEXT"), ("notified", "INTEGER DEFAULT 0")]:
                try:
                    await db.execute(f"ALTER TABLE jobs ADD COLUMN {col} {typedef}")
                except Exception:
                    pass
            await db.commit()

    async def create_job(
        self,
        user_id: int,
        channel_id: int,
        hash_value: str,
        mode: int,
        wordlist: str,
        rules: Optional[str] = None,
        message_id: Optional[int] = None,
    ) -> str:
        job_id = str(uuid.uuid4())[:8]
        now = time.time()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO jobs (
                    job_id, user_id, channel_id, message_id, hash_value, mode,
                    wordlist, rules, status, progress, notified, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
                """,
                (
                    job_id, user_id, channel_id, message_id, hash_value, mode,
                    wordlist, rules, JobStatus.QUEUED.value, "En cola...", now, now
                ),
            )
            await db.commit()
        return job_id

    async def update_job(self, job_id: str, **kwargs):
        if not kwargs:
            return
        kwargs["updated_at"] = time.time()
        cols = ", ".join(f"{k} = ?" for k in kwargs)
        values = list(kwargs.values()) + [job_id]
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(f"UPDATE jobs SET {cols} WHERE job_id = ?", values)
            await db.commit()

    async def claim_notified(self, job_id: str) -> bool:
        """Marca notified=1 solo si aún era 0. True = esta corrida 'ganó' el aviso."""
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                "UPDATE jobs SET notified = 1, updated_at = ? WHERE job_id = ? AND COALESCE(notified, 0) = 0",
                (time.time(), job_id),
            )
            await db.commit()
            return cur.rowcount > 0

    async def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def get_active_jobs(self) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """
                SELECT * FROM jobs
                WHERE status IN (?, ?, ?)
                ORDER BY created_at ASC
                """,
                (JobStatus.QUEUED.value, JobStatus.STARTING.value, JobStatus.RUNNING.value),
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(r) for r in rows]

    async def get_jobs_needing_message_update(self) -> List[Dict[str, Any]]:
        """Jobs cuyo mensaje de Discord hay que editar."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """
                SELECT * FROM jobs
                WHERE message_id IS NOT NULL
                  AND channel_id IS NOT NULL
                  AND (
                    status IN ('queued', 'starting', 'running')
                    OR (
                      status IN ('cracked', 'failed', 'cancelled', 'timeout')
                      AND COALESCE(notified, 0) = 0
                    )
                  )
                ORDER BY updated_at DESC
                """
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(r) for r in rows]

    async def get_user_jobs(self, user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """
                SELECT * FROM jobs WHERE user_id = ?
                ORDER BY created_at DESC LIMIT ?
                """,
                (user_id, limit),
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(r) for r in rows]

    async def count_running(self) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT COUNT(*) FROM jobs
                WHERE status IN (?, ?)
                """,
                (JobStatus.STARTING.value, JobStatus.RUNNING.value),
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0

    # ---- Warm instance pool ----

    async def save_warm_instance(
        self, instance_id: str, ssh_host: str, ssh_port: int, wordlist_path: str = ""
    ):
        now = time.time()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO warm_instances (instance_id, ssh_host, ssh_port, wordlist_path, status, last_used_at, created_at)
                VALUES (?, ?, ?, ?, 'idle', ?, ?)
                ON CONFLICT(instance_id) DO UPDATE SET
                    ssh_host=excluded.ssh_host,
                    ssh_port=excluded.ssh_port,
                    wordlist_path=excluded.wordlist_path,
                    status='idle',
                    last_used_at=excluded.last_used_at
                """,
                (instance_id, ssh_host, ssh_port, wordlist_path or "", now, now),
            )
            await db.commit()

    async def claim_warm_instance(self) -> Optional[Dict[str, Any]]:
        """Toma una instancia idle del pool (si hay)."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM warm_instances WHERE status = 'idle' ORDER BY last_used_at DESC LIMIT 1"
            ) as cur:
                row = await cur.fetchone()
            if not row:
                return None
            inst = dict(row)
            await db.execute(
                "UPDATE warm_instances SET status = 'busy', last_used_at = ? WHERE instance_id = ?",
                (time.time(), inst["instance_id"]),
            )
            await db.commit()
            return inst

    async def release_warm_instance(self, instance_id: str, wordlist_path: str = ""):
        now = time.time()
        async with aiosqlite.connect(self.db_path) as db:
            if wordlist_path:
                await db.execute(
                    "UPDATE warm_instances SET status='idle', last_used_at=?, wordlist_path=? WHERE instance_id=?",
                    (now, wordlist_path, instance_id),
                )
            else:
                await db.execute(
                    "UPDATE warm_instances SET status='idle', last_used_at=? WHERE instance_id=?",
                    (now, instance_id),
                )
            await db.commit()

    async def remove_warm_instance(self, instance_id: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM warm_instances WHERE instance_id = ?", (instance_id,))
            await db.commit()

    async def get_idle_warm_instances(self, older_than_seconds: float) -> List[Dict[str, Any]]:
        cutoff = time.time() - older_than_seconds
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM warm_instances WHERE status='idle' AND last_used_at < ?",
                (cutoff,),
            ) as cur:
                return [dict(r) for r in await cur.fetchall()]

    async def list_warm_instances(self) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM warm_instances ORDER BY last_used_at DESC") as cur:
                return [dict(r) for r in await cur.fetchall()]
