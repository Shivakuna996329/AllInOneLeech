import asyncio, subprocess, socket
from pathlib import Path

def resolve_py(hostname):
    # Python system resolver - aiohttp ki pani chese resolver ye!
    try:
        return socket.gethostbyname(hostname)
    except Exception as e:
        print(f"gethostbyname failed: {e}")
    try:
        for fam, _, _, _, sockaddr in socket.getaddrinfo(hostname, 443, socket.AF_INET, socket.SOCK_STREAM):
            return sockaddr[0]
    except Exception as e:
        print(f"getaddrinfo failed: {e}")
    return None

async def parallel_download(url, destination, pause_event, progress_callback=None):
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    def _run():
        from urllib.parse import urlparse
        host = urlparse(url).hostname
        ip = resolve_py(host) if host else None
        print(f"Python Resolved {host} -> {ip}")
        if not ip:
            raise Exception(f"Python DNS failed for {host} - Render DNS down")
        cmd = ["curl", "-L", "--fail", "-4", "--resolve", f"{host}:443:{ip}", "--resolve", f"{host}:80:{ip}", "-A", "Mozilla/5.0", "-o", str(destination), url]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if r.returncode == 0 and destination.exists() and destination.stat().st_size > 0:
            return f"curl+pyresolve {ip}"
        raise Exception(f"curl rc={r.returncode} err={r.stderr[:300]} ip={ip}")
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
