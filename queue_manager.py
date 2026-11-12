import asyncio
import secrets
import time
import json
from pathlib import Path


QUEUE_FILE = Path("queue.json")


class QueueManager:

    def __init__(self):
        self.jobs = {}
        self.lock = asyncio.Lock()
        self.sequence = 0

    def _save_locked(self):
        data = []

        for job in self.jobs.values():
            data.append({
                "id": job["id"],
                "chat_id": job["chat_id"],
                "filename": job["filename"],
                "url": job["url"],
                "priority": job["priority"],
                "priority_value": job["priority_value"],
                "sequence": job["sequence"],
                "status": job["status"],
                "progress": job["progress"],
                "downloaded": job["downloaded"],
                "total": job["total"]
            })

        temp = QUEUE_FILE.with_suffix(".tmp")
        temp.write_text(json.dumps(data, indent=2))
        temp.replace(QUEUE_FILE)

    async def restore(self):
        async with self.lock:
            if not QUEUE_FILE.exists():
                return

            try:
                data = json.loads(QUEUE_FILE.read_text())
            except (ValueError, OSError):
                print("Queue restore failed.")
                return

            if not isinstance(data, list):
                return

            for item in data:
                if not isinstance(item, dict):
                    continue

                status = item.get("status")

                if status not in (
                    "Queued",
                    "Downloading",
                    "Paused"
                ):
                    continue

                job_id = item.get("id")

                if not isinstance(job_id, str):
                    continue

                priority = item.get("priority", "normal")

                if priority not in ("high", "normal", "low"):
                    priority = "normal"

                sequence = max(
                    0, int(item.get("sequence", 0))
                )

                self.sequence = max(
                    self.sequence, sequence
                )

                pause_event = asyncio.Event()

                # Interrupted downloads restart as queued.
                pause_event.set()

                self.jobs[job_id] = {
                    "id": job_id,
                    "chat_id": item.get("chat_id"),
                    "filename": item.get("filename", "download.bin"),
                    "url": item.get("url"),
                    "priority": priority,
                    "priority_value": {
                        "high": 0,
                        "normal": 1,
                        "low": 2
                    }[priority],
                    "sequence": sequence,
                    "created_at": time.monotonic(),
                    "task": None,
                    "pause_event": pause_event,
                    "status": "Queued",
                    "progress": float(item.get("progress", 0)),
                    "downloaded": int(item.get("downloaded", 0)),
                    "total": int(item.get("total", 0)),
                    "speed": 0.0,
                    "eta": 0,
                    "start_time": time.monotonic(),
                    "last_update": time.monotonic()
                }

            self._save_locked()

            print(
                f"Restored {len(self.jobs)} queue job(s)."
            )

    async def create_job(
        self, chat_id, filename, url, priority="normal"
    ):
        priority = str(priority).lower()

        if priority not in ("high", "normal", "low"):
            priority = "normal"

        priority_values = {
            "high": 0,
            "normal": 1,
            "low": 2
        }

        async with self.lock:
            self.sequence += 1

            job_id = secrets.token_hex(4)

            while job_id in self.jobs:
                job_id = secrets.token_hex(4)

            job = {
                "id": job_id,
                "chat_id": chat_id,
                "filename": filename,
                "url": url,
                "priority": priority,
                "priority_value": priority_values[priority],
                "created_at": time.monotonic(),
                "sequence": self.sequence,
                "task": None,
                "pause_event": asyncio.Event(),
                "status": "Queued",
                "progress": 0.0,
                "downloaded": 0,
                "total": 0,
                "speed": 0.0,
                "eta": 0,
                "start_time": time.monotonic(),
                "last_update": time.monotonic()
            }

            job["pause_event"].set()
            self.jobs[job_id] = job

            self._save_locked()

            return job_id

    def get_job(self, job_id):
        return self.jobs.get(job_id)

    def get_active_jobs(self):
        return [
            job for job in self.jobs.values()
            if job["status"] in (
                "Queued",
                "Downloading",
                "Paused",
                "Cancelling"
            )
        ]

    def get_chat_jobs(self, chat_id):
        return [
            job for job in self.jobs.values()
            if job["chat_id"] == chat_id
        ]

    async def set_task(self, job_id, task):
        job = self.get_job(job_id)

        if not job:
            return False

        job["task"] = task
        return True

    async def set_status(self, job_id, status):
        async with self.lock:
            job = self.get_job(job_id)

            if not job:
                return False

            job["status"] = status
            self._save_locked()
            return True

    async def update_progress(self, job_id, current, total):
        job = self.get_job(job_id)

        if not job:
            return False

        now = time.monotonic()
        elapsed = now - job["last_update"]
        previous = job["downloaded"]

        job["downloaded"] = current
        job["total"] = total

        job["progress"] = (
            current * 100 / total if total else 0
        )

        if elapsed > 0:
            instant_speed = max(
                0, current - previous
            ) / elapsed

            old_speed = job["speed"]

            job["speed"] = (
                instant_speed
                if old_speed == 0
                else old_speed * 0.7 + instant_speed * 0.3
            )

        job["last_update"] = now

        remaining = max(0, total - current)

        job["eta"] = (
            remaining / job["speed"]
            if job["speed"] > 0 and total
            else 0
        )

        # Avoid disk writes for every progress callback.
        if not hasattr(self, "_last_save"):
            self._last_save = {}

        if now - self._last_save.get(job_id, 0) >= 10:
            self._last_save[job_id] = now

            async with self.lock:
                self._save_locked()

        return True

    async def pause_job(self, job_id):
        job = self.get_job(job_id)

        if not job or job["status"] != "Downloading":
            return False

        job["pause_event"].clear()
        job["status"] = "Paused"

        async with self.lock:
            self._save_locked()

        return True

    async def resume_job(self, job_id):
        job = self.get_job(job_id)

        if not job or job["status"] != "Paused":
            return False

        job["pause_event"].set()
        job["status"] = "Downloading"

        async with self.lock:
            self._save_locked()

        return True

    async def cancel_job(self, job_id):
        job = self.get_job(job_id)

        if not job:
            return False

        if job["status"] in (
            "Completed",
            "Cancelled",
            "Failed"
        ):
            return False

        task = job.get("task")
        job["pause_event"].set()

        if task and not task.done():
            job["status"] = "Cancelling"
            task.cancel()
        else:
            job["status"] = "Cancelled"

        async with self.lock:
            self._save_locked()

        return True

    async def finish_job(self, job_id, status):
        async with self.lock:
            job = self.get_job(job_id)

            if not job:
                return False

            job["status"] = status
            job["task"] = None

            self._save_locked()
            return True


queue_manager = QueueManager()
