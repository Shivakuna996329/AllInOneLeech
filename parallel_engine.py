import asyncio
import aiohttp
import os
import math
from pathlib import Path

MAX_CONNECTIONS = 4
CHUNK_SIZE = 1024 * 1024
MAX_SIZE = 4 * 1024**3


async def parallel_download(
    url,
    destination,
    pause_event,
    progress_callback=None
):
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)

    timeout = aiohttp.ClientTimeout(
        total=None,
        sock_connect=30,
        sock_read=120
    )

    connector = aiohttp.TCPConnector(
        limit=MAX_CONNECTIONS,
        limit_per_host=MAX_CONNECTIONS
    )

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector
    ) as session:

        # Check Range support
        async with session.get(
            url,
            headers={"Range": "bytes=0-0"},
            allow_redirects=True
        ) as response:

            content_range = response.headers.get(
                "Content-Range", ""
            )

            range_supported = response.status == 206

            try:
                total = int(content_range.split("/")[-1])
            except (ValueError, IndexError):
                total = 0
                range_supported = False

            if total <= 0 or total > MAX_SIZE:
                range_supported = False

            etag = response.headers.get("ETag")

        # Automatic Single Connection Fallback
        if not range_supported:

            async with session.get(
                url,
                allow_redirects=True
            ) as response:

                response.raise_for_status()

                length = response.headers.get(
                    "Content-Length"
                )

                if length:
                    total = int(length)

                    if total > MAX_SIZE:
                        raise ValueError("File exceeds 4 GB limit")

                downloaded = 0
                temp_path = destination.with_name(
                    destination.name + ".single.part"
                )

                with open(temp_path, "wb") as file:

                    async for data in response.content.iter_chunked(
                        CHUNK_SIZE
                    ):

                        await pause_event.wait()

                        downloaded += len(data)

                        if downloaded > MAX_SIZE:
                            raise ValueError("File exceeds 4 GB limit")

                        file.write(data)

                        if progress_callback:
                            result = progress_callback(
                                downloaded,
                                total or downloaded
                            )

                            if asyncio.iscoroutine(result):
                                await result

                    file.flush()
                    os.fsync(file.fileno())

                if total and downloaded != total:
                    raise ValueError("Incomplete single download")

                os.replace(temp_path, destination)

            return destination

        # Parallel Download
        connections = min(MAX_CONNECTIONS, total)
        chunk_size = math.ceil(total / connections)

        part_paths = [
            destination.parent / (
                f".{destination.name}.parallel.{i}.part"
            )
            for i in range(connections)
        ]

        async def download_part(index):

            start = index * chunk_size
            end = min(
                start + chunk_size - 1,
                total - 1
            )

            part_path = part_paths[index]

            offset = (
                part_path.stat().st_size
                if part_path.exists()
                else 0
            )

            expected_size = end - start + 1

            if offset > expected_size:
                raise ValueError("Invalid partial chunk")

            if offset == expected_size:
                return

            current_start = start + offset

            headers = {
                "Range": f"bytes={current_start}-{end}"
            }

            if etag:
                headers["If-Range"] = etag

            async with session.get(
                url,
                headers=headers,
                allow_redirects=True
            ) as response:

                if response.status != 206:
                    raise ValueError(
                        f"Range failed: HTTP {response.status}"
                    )

                expected_range = (
                    f"bytes {current_start}-{end}/{total}"
                )

                if response.headers.get(
                    "Content-Range"
                ) != expected_range:
                    raise ValueError("Invalid Content-Range")

                if etag and response.headers.get("ETag") != etag:
                    raise ValueError("Server file changed")

                received = offset

                with open(
                    part_path,
                    "ab" if offset else "wb"
                ) as file:

                    async for data in response.content.iter_chunked(
                        CHUNK_SIZE
                    ):

                        await pause_event.wait()

                        received += len(data)

                        if received > expected_size:
                            raise ValueError("Part size exceeded")

                        file.write(data)

                        if progress_callback:
                            current = sum(
                                p.stat().st_size if p.exists() else 0
                                for p in part_paths
                            )

                            result = progress_callback(
                                current,
                                total
                            )

                            if asyncio.iscoroutine(result):
                                await result

                    file.flush()
                    os.fsync(file.fileno())

                if received != expected_size:
                    raise ValueError("Incomplete download part")

        await asyncio.gather(
            *(download_part(i) for i in range(connections))
        )

        # Verify parts
        for i, path in enumerate(part_paths):

            expected = min(
                chunk_size,
                total - i * chunk_size
            )

            if not path.exists():
                raise ValueError("Missing download part")

            if path.stat().st_size != expected:
                raise ValueError("Part verification failed")

        # Merge parts
        temp_path = destination.with_name(
            destination.name + ".assembling"
        )

        try:
            with open(temp_path, "wb") as output:

                for path in part_paths:

                    with open(path, "rb") as source:

                        while True:
                            data = source.read(CHUNK_SIZE)

                            if not data:
                                break

                            output.write(data)

                output.flush()
                os.fsync(output.fileno())

            if temp_path.stat().st_size != total:
                raise ValueError("Merged file size mismatch")

            os.replace(temp_path, destination)

        finally:
            if temp_path.exists():
                temp_path.unlink()

        # Cleanup
        for path in part_paths:
            path.unlink(missing_ok=True)

    return destination
