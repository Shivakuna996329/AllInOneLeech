import json
import asyncio
from pathlib import Path

RECOVERY_FILE = Path("recovery.json")
_lock = asyncio.Lock()


def load_jobs():
    if not RECOVERY_FILE.exists():
        return []

    try:
        data = json.loads(RECOVERY_FILE.read_text())
        return data if isinstance(data, list) else []
    except (json.JSONDecodeError, OSError):
        return []


async def save_job(job):
    async with _lock:
        jobs = load_jobs()
        jobs = [
            item for item in jobs
            if item.get("id") != job.get("id")
        ]
        jobs.append(job)

        temp = RECOVERY_FILE.with_suffix(".tmp")
        temp.write_text(json.dumps(jobs, indent=2))
        temp.replace(RECOVERY_FILE)


async def remove_job(job_id):
    async with _lock:
        jobs = load_jobs()
        jobs = [
            item for item in jobs
            if item.get("id") != job_id
        ]

        temp = RECOVERY_FILE.with_suffix(".tmp")
        temp.write_text(json.dumps(jobs, indent=2))
        temp.replace(RECOVERY_FILE)


def get_recoverable_jobs():
    recoverable = []

    for job in load_jobs():
        if not job.get("url") or not job.get("part_path"):
            continue

        part = Path(job["part_path"])
        parallel_parts = list(
            part.parent.glob(
                f".{part.name}.parallel.*.part"
            )
        )

        if part.exists() or parallel_parts:
            recoverable.append(job)

    return recoverable
