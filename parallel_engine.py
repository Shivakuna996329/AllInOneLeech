import asyncio, subprocess, socket, ssl, json
from pathlib import Path

def resolve_doh_ip(hostname):
    # 1.1.1.1 ki IP tho direct - DNS avasaram ledhu!
    for doh_ip, sni in [("1.1.1.1", "cloudflare-dns.com"), ("8.8.8.8", "dns.google")]:
        try:
            ctx = ssl.create_default_context()
            raw = socket.create_connection((doh_ip, 443), timeout=5)
            ss = ctx.wrap_socket(raw, server_hostname=sni)
            req = f"GET /dns-query?name={hostname}&type=A HTTP/1.1\r\nHost: {sni}\r\nAccept: application/dns-json\r\nConnection: close\r\n\r\n"
            ss.sendall(req.encode())
            data = b""
            while True:
                c = ss.recv(8192)
                if not c: break
                data += c
            ss.close()
            body = data.split(b"\r\n\r\n", 1)[1].decode(errors="ignore")
            # chunked decode simple
            if "\r\n" in body:
                try:
                    parts = body.split("\r\n")
                    body = parts[1] if len(parts) > 1 else body
                except: pass
            j = json.loads(body[body.find("{"):])
            for ans in j.get("Answer", []):
                if ans.get("type") == 1:
                    print(f"DoH {doh_ip} resolved {hostname} -> {ans['data']}")
                    return ans["data"]
        except Exception as e:
            print(f"DoH {doh_ip} failed: {e}")
    return None

async def parallel_download(url, destination, pause_event, progress_callback=None):
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    def _run():
        from urllib.parse import urlparse
        host = urlparse(url).hostname
        ip = resolve_doh_ip(host) if host else None
        if not ip:
            raise Exception(f"DoH-IP kuda fail ayindhi {host} - Render outbound block chesindhi")
        cmd = ["curl", "-L", "--fail", "-4", "--resolve", f"{host}:443:{ip}", "--resolve", f"{host}:80:{ip}", "-A", "Mozilla/5.0", "-o", str(destination), url]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if r.returncode == 0 and destination.exists() and destination.stat().st_size > 0:
            return f"curl+DoH-IP {ip}"
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
