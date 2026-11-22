import asyncio, subprocess, socket, struct, random
from pathlib import Path

def resolve_tcp_dns(hostname):
    # Raw DNS query via TCP to 8.8.8.8:53 and 1.1.1.1:53 - no system DNS!
    for dns_ip in ["8.8.8.8", "1.1.1.1"]:
        try:
            txid = random.randint(0, 65535)
            flags = 0x0100
            header = struct.pack(">HHHHHH", txid, flags, 1, 0, 0, 0)
            qname = b"".join(bytes([len(p)]) + p.encode() for p in hostname.split(".")) + b"\x00"
            question = qname + struct.pack(">HH", 1, 1)
            packet = header + question
            s = socket.create_connection((dns_ip, 53), timeout=5)
            s.sendall(struct.pack(">H", len(packet)) + packet)
            resp_len = struct.unpack(">H", s.recv(2))[0]
            data = b""
            while len(data) < resp_len:
                chunk = s.recv(resp_len - len(data))
                if not chunk: break
                data += chunk
            s.close()
            # Parse answers - find A record
            pos = 12
            while data[pos] != 0:
                pos += 1 + data[pos]
            pos += 5
            ancount = struct.unpack(">H", data[6:8])[0]
            for _ in range(ancount):
                if data[pos] & 0xC0 == 0xC0:
                    pos += 2
                else:
                    while data[pos] != 0:
                        pos += 1 + data[pos]
                    pos += 1
                rtype, rclass, ttl, rdlen = struct.unpack(">HHIH", data[pos:pos+10])
                pos += 10
                if rtype == 1 and rdlen == 4:
                    ip = ".".join(str(b) for b in data[pos:pos+4])
                    return ip
                pos += rdlen
        except Exception as e:
            print(f"TCP DNS {dns_ip} failed: {e}")
            continue
    return None

async def parallel_download(url, destination, pause_event, progress_callback=None):
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    def _run():
        from urllib.parse import urlparse
        host = urlparse(url).hostname
        ip = resolve_tcp_dns(host) if host else None
        print(f"Resolved {host} -> {ip}")
        if not ip:
            raise Exception(f"TCP DNS failed to resolve {host}")
        # --resolve tho DNS bypass - direct IP ki connect!
        cmd = ["curl", "-L", "--fail", "--resolve", f"{host}:443:{ip}", "--resolve", f"{host}:80:{ip}", "-A", "Mozilla/5.0", "-o", str(destination), url]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if r.returncode == 0 and destination.exists() and destination.stat().st_size > 0:
            return f"curl+resolve {ip}"
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
