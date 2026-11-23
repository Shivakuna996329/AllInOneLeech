import asyncio, pathlib, os
async def download_with_aria2(url, final_path, pause_event, progress_cb):
    import libtorrent as lt
    ses = lt.session()
    ses.listen_on(6881, 6891)
    params = {"save_path": str(pathlib.Path(final_path).parent), "storage_mode": lt.storage_mode_t.storage_mode_sparse}
    if url.startswith("magnet:"):
        h = lt.add_magnet_uri(ses, url, params)
    else:
        h = lt.add_torrent_file(url, params)
    # metadata kosam wait
    for _ in range(100):
        if h.has_metadata():
            break
        await asyncio.sleep(1)
    # download ayye varaku wait
    while not h.is_seed():
        s = h.status()
        if progress_cb:
            try:
                await progress_cb(s.total_downloaded, s.total_wanted)
            except:
                pass
        await asyncio.sleep(2)
        if h.status().state == lt.torrent_status.seeding:
            break
    # files ni final_path ki move
    files = list(pathlib.Path(final_path).parent.rglob("*"))
    files = [f for f in files if f.is_file()]
    if files:
        newest = max(files, key=lambda f: f.stat().st_mtime)
        if str(newest) != str(final_path):
            import shutil
            shutil.move(str(newest), str(final_path))
    return True
