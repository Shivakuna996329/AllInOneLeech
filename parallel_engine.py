import asyncio, subprocess, urllib.request, urllib.error
from pathlib import Path
async def parallel_download(url, destination, pause_event, progress_callback=None):
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    def _run():
        errors = []
        # 1. curl
        try:
            r = subprocess.run(["curl", "-L", "--fail", "-A", "Mozilla/5.0", "-o", str(destination), url], capture_output=True, text=True, timeout=120)
            if r.returncode == 0 and destination.exists() and destination.stat().st_size > 0:
                return "curl"
            errors.append(f"curl rc={r.returncode} err={r.stderr[:200]}")
        except Exception as e:
            errors.append(f"curl ex={repr(e)}")
        # 2. wget
        try:
            r = subprocess.run(["wget", "-O", str(destination), url], capture_output=True, text=True, timeout=120)
            if r.returncode == 0 and destination.exists() and destination.stat().st_size > 0:
                return "wget"
            errors.append(f"wget rc={r.returncode} err={r.stderr[:200]}")
        except Exception as e:
            errors.append(f"wget ex={repr(e)}")
        # 3. urllib
        try:
            if destination.exists():
                destination.unlink()
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=60) as resp, open(destination, "wb") as f:
                while True:
                    chunk = resp.read(1024*1024)
                    if not chunk: break
                    f.write(chunk)
            if destination.exists() and destination.stat().st_size > 0:
                return "urllib"
            errors.append("urllib: empty file")
        except Exception as e:
            errors.append(f"urllib ex={repr(e)}")
        raise Exception("ALL FAILED: " + " | ".join(errors))
    method = await asyncio.to_thread(_run)
    print(f"Downloaded via {method}")
    if progress_callback:
        try:
            size = destination.stat().st_size
            r = progress_callback(size, size)
            if asyncio.iscoroutine(r):
                await r
        except Exception:
            pass
    return destination
