"""
Job Logger - Tracks job executions in the database
Used by cronjobs to log their execution status for monitoring
"""

import asyncio
import json
import logging
import os
import asyncpg
from datetime import datetime
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class JobLogger:
    """
    Helper class to log job executions to the database.

    Usage:
        job_logger = JobLogger()
        await job_logger.connect()

        execution_id = await job_logger.start_job("my-job-name")
        try:
            # Do your job work here
            records = process_data()
            await job_logger.complete_job(execution_id, records_processed=len(records))
        except Exception as e:
            await job_logger.complete_job(execution_id, status='failed', error_message=str(e))
        finally:
            await job_logger.close()
    """

    def __init__(self, pool: asyncpg.Pool = None):
        """Initialize with optional existing pool"""
        self.pool = pool
        self._own_pool = False  # Track if we created the pool ourselves

    async def connect(self):
        """Create database connection pool if not provided"""
        if self.pool is None:
            self.pool = await asyncpg.create_pool(
                host=os.getenv('DB_HOST', '15.235.115.4'),
                port=int(os.getenv('DB_PORT', '5433')),
                user=os.getenv('DB_USER', 'smallcaps_admin'),
                password=os.getenv('DB_PASSWORD', 'SmallCaps2025'),
                database=os.getenv('DB_NAME', 'smallcaps_prod'),
                min_size=1,
                max_size=3
            )
            self._own_pool = True
            logger.info("JobLogger connected to database")

    async def close(self):
        """Close database connection pool if we own it"""
        if self._own_pool and self.pool:
            await self.pool.close()
            logger.info("JobLogger disconnected from database")

    async def start_job(self, job_name: str, metadata: Dict[str, Any] = None) -> Optional[int]:
        """
        Log job start and return execution ID

        Args:
            job_name: Unique identifier for the job (e.g., 'float-scraper')
            metadata: Optional dict of additional data to store

        Returns:
            execution_id: ID to use when completing the job, or None on error
        """
        if not self.pool:
            await self.connect()

        try:
            query = """
                INSERT INTO job_executions (job_name, started_at, status, metadata)
                VALUES ($1, NOW(), 'running', $2::jsonb)
                RETURNING id
            """
            metadata_json = json.dumps(metadata or {})
            async with self.pool.acquire() as conn:
                result = await conn.fetchrow(query, job_name, metadata_json)
                if result:
                    logger.info(f"📝 Job '{job_name}' started (execution_id: {result['id']})")
                    return result['id']
            return None
        except Exception as e:
            logger.error(f"Failed to log job start: {e}")
            return None

    async def complete_job(
        self,
        execution_id: int,
        status: str = 'success',
        records_processed: int = None,
        error_message: str = None
    ):
        """
        Log job completion

        Args:
            execution_id: ID returned from start_job()
            status: 'success', 'failed', or 'partial'
            records_processed: Number of records processed
            error_message: Error description if failed
        """
        if not execution_id:
            return

        try:
            query = """
                UPDATE job_executions
                SET
                    completed_at = NOW(),
                    status = $2,
                    records_processed = $3,
                    error_message = $4,
                    duration_seconds = EXTRACT(EPOCH FROM (NOW() - started_at))::INTEGER
                WHERE id = $1
            """
            async with self.pool.acquire() as conn:
                await conn.execute(query, execution_id, status, records_processed, error_message)
            logger.info(f"📝 Job execution {execution_id} completed: status={status}, records={records_processed}")
        except Exception as e:
            logger.error(f"Failed to log job completion: {e}")


async def run_with_logging(job_name: str, job_func, *args, **kwargs):
    """
    Convenience wrapper to run a job function with automatic logging

    Usage:
        async def my_job():
            # Do work
            return 100  # records processed

        await run_with_logging("my-job", my_job)
    """
    job_logger = JobLogger()
    await job_logger.connect()

    execution_id = await job_logger.start_job(job_name)

    try:
        result = await job_func(*args, **kwargs)
        records = result if isinstance(result, int) else None
        await job_logger.complete_job(execution_id, records_processed=records)
        return result
    except Exception as e:
        await job_logger.complete_job(execution_id, status='failed', error_message=str(e))
        raise
    finally:
        await job_logger.close()
