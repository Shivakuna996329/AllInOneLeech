import asyncio, pathlib, os, shutil, urllib.request, tarfile
async def _get_aria2():
    p = shutil.which("aria2c")
    if p:
        return p
    local = pathlib.Path("aria2c")
    if local.exists():
        return str(local)
    url = "https://github.com/q3aql/aria2-static-builds/releases/download/v1.37.0/aria2-1.37.0-linux-gnu-64bit-build1.tar.bz2"
    urllib.request.urlretrieve(url, "aria2.tar.bz2")
    with tarfile.open("aria2.tar.bz2") as tf:
        tf.extractall("aria2_extract")
    for f in pathlib.Path("aria2_extract").rglob("aria2c"):
        shutil.move(str(f), str(local))
        break
    os.chmod(local, 0o755)
    return str(local)
async def download_with_aria2(url, final_path, pause_event, progress_cb):
    aria2c = await _get_aria2()
    out_dir = str(pathlib.Path(final_path).parent)
    os.makedirs(out_dir, exist_ok=True)
    cmd = [aria2c,"--seed-time=0","--max-connection-per-server=16","--split=16","--dir",out_dir,url]
    proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    while True:
        line = await proc.stdout.readline()
        if not line:
            break
    await proc.wait()
    files = [f for f in pathlib.Path(out_dir).rglob("*") if f.is_file()]
    if files:
        newest = max(files, key=lambda f: f.stat().st_mtime)
        if str(newest) != str(final_path):
            shutil.move(str(newest), str(final_path))
    return proc.returncode == 0
