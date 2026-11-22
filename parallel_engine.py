import subprocess
from pathlib import Path
async def parallel_download(url, destination, pause_event, progress_callback=None):
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Parallel ledhu, single curl matrame!
    r = subprocess.run(["curl","-L","-o",str(destination),url], capture_output=True, text=True, timeout=60)
    print("SIMPLE CURL RESULT:", r.returncode, r.stderr[:500])
    if r.returncode != 0:
        raise Exception(f"Simple curl kuda fail: {r.stderr[:300]}")
    return destination
