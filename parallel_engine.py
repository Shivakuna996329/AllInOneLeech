import asyncio, subprocess, urllib.request
from pathlib import Path
async def parallel_download(url, destination, pause_event, progress_callback=None):
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    def _run():
        errors = []
        # Curl with DNS-over-HTTPS - system DNS vadadhu!
        for doh in ["https://1.1.1.1/dns-query", "https://8.8.8.8/resolve"]:
            try:
                r = subprocess.run(["curl", "-L", "--fail", "--doh-url", doh, "-A", "Mozilla/5.0", "-o", str(destination), url], capture_output=True, text=True, timeout=120)
                if r.returncode == 0 and destination.exists() and destination.stat().st_size > 0:
                    return f"curl+DoH {doh}"
                errors.append(f"curl DoH {doh} rc={r.returncode} err={r.stderr[:200]}")
            except Exception as e:
                errors.append(f"curl DoH ex={repr(e)}")
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
