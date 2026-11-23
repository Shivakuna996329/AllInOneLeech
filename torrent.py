import asyncio, os, pathlib, shutil, tarfile, urllib.request

async def _get_aria2():
    p = shutil.which("aria2c")
    if p:
        return p
    local = pathlib.Path("aria2c")
    if local.exists():
        os.chmod(local, 0o755)
        return str(local.resolve())
    urls = [
        "https://github.com/q3aql/aria2-static-builds/releases/download/v1.37.0/aria2-1.37.0-linux-gnu-amd64-build1.tar.bz2",
        "https://github.com/P3TERX/Aria2-Pro-Core/releases/download/1.37.0_2024-08-10/aria2-1.37.0-linux-gnu-amd64-build1.tar.bz2",
    ]
    for url in urls:
        try:
            print(f"Trying aria2: {url}")
            urllib.request.urlretrieve(url, "aria2.tar.bz2")
            with tarfile.open("aria2.tar.bz2") as tf:
                tf.extractall("aria2_extract")
            for f in pathlib.Path("aria2_extract").rglob("aria2c"):
                shutil.copy(str(f), "aria2c")
                os.chmod("aria2c", 0o755)
                return str(local.resolve())
        except Exception as e:
            print(f"aria2 download failed {url}: {e}")
            continue
    raise RuntimeError("aria2c download failed - all mirrors 404")

async def download_with_aria2(url, final_path, pause_event, progress_cb):
    aria2c = await _get_aria2()
    out_dir = str(pathlib.Path(final_path).parent)
    os.makedirs(out_dir, exist_ok=True)
    # torrent ki separate dir - file peru confuse avvakunda
    tmp_dir = str(pathlib.Path(out_dir) / "aria2_tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    cmd = [aria2c,"--seed-time=0","--max-connection-per-server=16","--split=16","--dir",tmp_dir,"--bt-save-metadata=false",url]
    proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    while True:
        line = await proc.stdout.readline()
        if not line:
            break
    await proc.wait()
    files = [f for f in pathlib.Path(tmp_dir).rglob("*") if f.is_file()]
    if not files:
        raise RuntimeError(f"aria2 exit={proc.returncode} no files downloaded")
    # torrent lo okate file unte dani move cheyyi, ekkuva unte first big file
    newest = max(files, key=lambda f: f.stat().st_size)
    shutil.move(str(newest), str(final_path))
    return proc.returncode == 0
