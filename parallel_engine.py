import asyncio
import os
import subprocess
from pathlib import Path

MAX_CONNECTIONS = 4
CHUNK_SIZE = 1024 * 1024
MAX_SIZE = 4 * 1024**3

async def parallel_download(url, destination, pause_event, progress_callback=None):
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)

    def _run_curl():
        # -L = follow redirects, -A = User-Agent, --fail = error on 404
        cmd = ["curl", "-L", "--fail", "-A", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0", "-o", str(destination), url]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
        if result.returncode != 0:
            raise Exception(f"curl failed: {result.stderr[:300]}")
        return destination

    result = await asyncio.to_thread(_run_curl)

    if progress_callback:
        try:
            size = destination.stat().st_size
            r = progress_callback(size, size)
            if asyncio.iscoroutine(r):
                await r
        except Exception:
            pass

    return result
