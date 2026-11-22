import asyncio
import os
import subprocess
import urllib.request
from pathlib import Path
MAX_SIZE = 4 * 1024**3
async def parallel_download(url, destination, pause_event, progress_callback=None):
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    def _run():
        # Try 1: curl
        try:
            r = subprocess.run(["curl", "-L", "--fail", "-A", "Mozilla/5.0", "-o", str(destination), url], capture_output=True, text=True, timeout=3600)
            if r.returncode == 0 and destination.exists() and destination.stat().st_size > 0:
                return "curl"
            print(f"curl failed: {r.stderr[:300]}")
        except Exception as e:
            print(f"curl exception: {e}")
        # Try 2: wget
        try:
            r = subprocess.run(["wget", "-O", str(destination), url], capture_output=True, text=True, timeout=3600)
            if r.returncode == 0 and destination.exists() and destination.stat().st_size > 0:
                return "wget"
            print(f"wget failed: {r.stderr[:300]}")
        except Exception as e:
            print(f"wget exception: {e}")
        # Try 3: python urllib
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=120) as resp, open(destination, "wb") as f:
            while True:
                chunk = resp.read(1024*1024)
                if not chunk:
                    break
                f.write(chunk)
        return "urllib"
    method = await asyncio.to_thread(_run)
    print(f"Downloaded via {method}: {destination}")
    if progress_callback:
        try:
            size = destination.stat().st_size
            r = progress_callback(size, size)
            if asyncio.iscoroutine(r):
                await r
        except Exception:
            pass
    return destination
