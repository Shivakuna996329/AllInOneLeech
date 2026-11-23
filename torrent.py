import asyncio, os, pathlib, shutil
async def download_with_aria2(url, final_path, pause_event, progress_cb):
    out_dir = str(pathlib.Path(final_path).parent)
    os.makedirs(out_dir, exist_ok=True)
    cmd = ["aria2c","--seed-time=0","--max-connection-per-server=16","--split=16","--dir",out_dir,url]
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
