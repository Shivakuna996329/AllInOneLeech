import json

import hashlib
import secrets
import gdown
import asyncio
asyncio.set_event_loop(asyncio.new_event_loop())
import os
import pyrogram
import time
from datetime import datetime
import zipfile
from pathlib import Path
from urllib.parse import urlparse, unquote

import aiohttp
from parallel_engine import parallel_download
from recovery_manager import load_jobs, save_job, remove_job, get_recoverable_jobs
from queue_manager import queue_manager
from pyrogram import Client, filters
from pyrogram.types import BotCommand
from config import API_ID, API_HASH, BOT_TOKEN, OWNER_ID

app = Client(
    "AllInOneLeech",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workdir="."
)
DOWNLOADS = Path("downloads")
DOWNLOADS.mkdir(exist_ok=True)
HISTORY_FILE = Path("download_history.txt")

# =====================================
# V3 DOWNLOAD STATISTICS
# =====================================

STATS_FILE = Path("download_stats.json")

def load_stats():
    default = {
        "total": 0,
        "success": 0,
        "failed": 0,
        "retries": 0
    }

    try:
        if STATS_FILE.exists():
            data = json.loads(STATS_FILE.read_text())

            for key in default:
                default[key] = max(0, int(data.get(key, 0)))

    except (ValueError, TypeError, OSError):
        pass

    return default


download_stats = load_stats()


def update_stats(*keys):
    for key in keys:
        if key in download_stats:
            download_stats[key] += 1

    temp = STATS_FILE.with_suffix(".tmp")

    try:
        temp.write_text(
            json.dumps(download_stats, indent=4)
        )
        os.replace(temp, STATS_FILE)

    except OSError as error:
        print(f"Statistics save error: {error}")

        try:
            temp.unlink(missing_ok=True)
        except OSError:
            pass


MAX_SIZE = 4 * 1024**3
MAX_CONCURRENT = 3
MAX_RETRIES = 3
RETRY_DELAY = 2

active_downloads = {}
pause_events = {}
download_status = {}
semaphore = asyncio.Semaphore(MAX_CONCURRENT)

# =====================================
# CENTRAL PRIORITY SCHEDULER
# =====================================

scheduler_queue = asyncio.PriorityQueue()
scheduler_workers = []


async def scheduler_worker(worker_id):
    while True:
        item = await scheduler_queue.get()

        try:
            priority, sequence, job_id, message, status = item
            job = queue_manager.get_job(job_id)

            if not job or job["status"] != "Queued":
                continue

            task = asyncio.create_task(
                run_queue_job(job_id, message, status)
            )

            await queue_manager.set_task(job_id, task)

            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as error:
                print(f"Worker {worker_id}: {error}")

        finally:
            scheduler_queue.task_done()


def start_priority_scheduler():
    if scheduler_workers:
        return

    for worker_id in range(MAX_CONCURRENT):
        task = asyncio.create_task(
            scheduler_worker(worker_id + 1)
        )
        scheduler_workers.append(task)

    print("Central Priority Scheduler Started!")



def is_owner(message):
    return bool(
        message.from_user
        and message.from_user.id == OWNER_ID
    )







@app.on_message(filters.command("start"))
async def start(client, message):
    print("DEBUG: START command received")
    print("DEBUG: User ID:", message.from_user.id if message.from_user else None)
    print("DEBUG: OWNER CHECK:", is_owner(message))

    if not is_owner(message):
        await message.reply_text("You are not authorized.")
        return

    text = """🔥 AllInOneLeech Personal Bot

📥 /download URL
📤 /upload filename
📁 /files
🔄 /recover
⏸️ /pause
▶️ /resume
❌ /cancel
✏️ /rename old|new
📦 /zip filename
🗜️ /extract filename.zip
📊 /status
📜 /history
⚡ /speedtest
📋 /queue
💾 /storage
📂 /largefiles
🧹 /cleanup
📈 /dashboard
🗑️ /delete filename
❓ /help"""

    await message.reply_text(text)


@app.on_message(filters.command("help"))
async def help_command(client, message):
    if not is_owner(message):
        return

    await start(client, message)

@app.on_message(filters.command("stats"))
async def stats_command(client, message):
    if not is_owner(message):
        return

    await message.reply_text(
        "📊 DOWNLOAD STATISTICS\n\n"
        f"📥 Total: {download_stats['total']}\n"
        f"✅ Successful: {download_stats['success']}\n"
        f"❌ Failed: {download_stats['failed']}\n"
        f"🔄 Retries: {download_stats['retries']}"
    )


@app.on_message(filters.command("status"))
async def status(client, message):
    if not is_owner(message):
        return

    await message.reply_text(
        "✅ AllInOneLeech Online!\n"
        f"📁 Files: {len(list(DOWNLOADS.iterdir()))}\n"
        f"⚡ Active Downloads: {len(active_downloads)}"
    )




# ADVANCED FILE MANAGER V2

file_manager_pending = {}
FM_PAGE_SIZE = 10


def fm_get_items():

    root = DOWNLOADS.resolve()
    items = []

    for path in DOWNLOADS.rglob("*"):

        try:
            resolved = path.resolve()

            if (
                resolved.is_file()
                and root in resolved.parents
            ):
                items.append(resolved)

        except (OSError, RuntimeError):
            continue

    return sorted(
        items,
        key=lambda p: str(p.relative_to(root)).lower()
    )

def fm_valid_path(token):
    path = file_manager_pending.get(token)

    if not path:
        return None

    root = DOWNLOADS.resolve()
    resolved = path.resolve()

    if root not in resolved.parents:
        return None

    if not resolved.is_file():
        return None

    return resolved


@app.on_message(filters.command("files"))
async def files(client, message):

    if not is_owner(message):
        return

    await fm_show_page(message, 0)


async def fm_show_page(message, page):

    items = fm_get_items()

    if not items:
        return await message.reply_text("📂 No files found.")

    total = len(items)
    pages = (total + FM_PAGE_SIZE - 1) // FM_PAGE_SIZE

    page = max(0, min(page, pages - 1))

    selected = items[
        page * FM_PAGE_SIZE:
        (page + 1) * FM_PAGE_SIZE
    ]

    total_size = sum(p.stat().st_size for p in items)

    text = (
        "📁 ADVANCED FILE MANAGER\n\n"
        f"📦 Total Files: {total}\n"
        f"💾 Total Size: {total_size / 1024**3:.2f} GB\n"
        f"📄 Page: {page + 1}/{pages}\n\n"
    )

    buttons = []

    for index, path in enumerate(
        selected,
        start=page * FM_PAGE_SIZE + 1
    ):

        relative = str(path.relative_to(DOWNLOADS.resolve()))

        token = secrets.token_hex(8)

        file_manager_pending[token] = path

        size = path.stat().st_size / 1024**2

        text += (
            f"{index}. 📄 {relative[:65]}\n"
            f"📦 {size:.2f} MB\n\n"
        )

        buttons.append([
            pyrogram.types.InlineKeyboardButton(
                "📤 Upload",
                callback_data=f"fm:upload:{token}"
            ),
            pyrogram.types.InlineKeyboardButton(
                "📥 Download",
                callback_data=f"fm:download:{token}"
            ),
            pyrogram.types.InlineKeyboardButton(
                "🗑️ Delete",
                callback_data=f"fm:delete:{token}"
            )
        ])

    navigation = []

    if page > 0:
        navigation.append(
            pyrogram.types.InlineKeyboardButton(
                "⬅️ Previous",
                callback_data=f"fm:page:{page - 1}"
            )
        )

    navigation.append(
        pyrogram.types.InlineKeyboardButton(
            f"{page + 1}/{pages}",
            callback_data="fm:refresh"
        )
    )

    if page < pages - 1:
        navigation.append(
            pyrogram.types.InlineKeyboardButton(
                "Next ➡️",
                callback_data=f"fm:page:{page + 1}"
            )
        )

    buttons.append(navigation)

    await message.reply_text(
        text,
        reply_markup=pyrogram.types.InlineKeyboardMarkup(buttons)
    )


@app.on_callback_query(filters.regex(r"^fm:"))
async def file_manager_callback(client, query):

    if not is_owner(query):
        return await query.answer(
            "Not authorized!",
            show_alert=True
        )

    parts = query.data.split(":")
    action = parts[1]

    if action in ("page", "refresh"):

        page = int(parts[2]) if action == "page" else 0

        await query.answer()

        await query.message.delete()

        await fm_show_page(
            query.message,
            page
        )

        return

    if len(parts) != 3:
        return await query.answer(
            "Invalid action!",
            show_alert=True
        )

    token = parts[2]
    path = fm_valid_path(token)

    if path is None:
        file_manager_pending.pop(token, None)

        return await query.answer(
            "File expired or not found!",
            show_alert=True
        )

    if action in ("upload", "download"):

        if path.stat().st_size > 2 * 1024**3:
            return await query.answer(
                "File exceeds 2GB upload limit!",
                show_alert=True
            )

        await query.answer("Sending file...")

        try:
            await query.message.reply_document(
                document=str(path),
                caption=f"📁 {path.name}"
            )
        except Exception as error:
            await query.message.reply_text(
                f"❌ Send failed: {str(error)[:150]}"
            )

        return

    if action == "delete":

        await query.answer()

        await query.message.edit_text(
            f"⚠️ DELETE CONFIRMATION\n\n"
            f"📁 {path.name}\n"
            f"📦 {path.stat().st_size / 1024**2:.2f} MB\n\n"
            "Are you sure?",
            reply_markup=pyrogram.types.InlineKeyboardMarkup([
                [
                    pyrogram.types.InlineKeyboardButton(
                        "✅ Confirm",
                        callback_data=f"fm:yes:{token}"
                    ),
                    pyrogram.types.InlineKeyboardButton(
                        "❌ Cancel",
                        callback_data=f"fm:no:{token}"
                    )
                ]
            ])
        )

        return

    if action == "yes":

        try:
            path.unlink()
            file_manager_pending.pop(token, None)

            await query.answer("Deleted!")

            await query.message.edit_text(
                f"✅ Deleted: {path.name}"
            )

        except Exception as error:
            await query.answer(
                str(error)[:150],
                show_alert=True
            )

        return

    if action == "no":

        file_manager_pending.pop(token, None)

        await query.answer("Cancelled!")

        await query.message.edit_text(
            "❌ Deletion cancelled."
        )

        return

    await query.answer("Unknown action!", show_alert=True)


async def progress(current, total, message, start_time):
    elapsed = time.time() - start_time
    speed = current / max(elapsed, 0.001)
    percent = current * 100 / total if total else 0

    filled = int(percent / 10)
    bar = "█" * filled + "░" * (10 - filled)

    remaining = (
        (total - current) / speed
        if total and speed > 0 else 0
    )

    eta = time.strftime("%H:%M:%S", time.gmtime(remaining))

    text = (
        f"📥 Downloading...\n\n"
        f"[{bar}] {percent:.1f}%\n"
        f"📦 Downloaded: {current / 1024**2:.2f} MB\n"
        f"📁 Total: {total / 1024**2:.2f} MB\n"
        f"⚡ Speed: {speed / 1024**2:.2f} MB/s\n"
        f"⏳ Remaining: {eta}"
    )

    try:
        await message.edit_text(text)
    except Exception as error:
        if "MESSAGE_NOT_MODIFIED" not in str(error):
            print(f"Progress Update Error: {error}")


async def upload_progress(current, total, status, start_time):
    now = time.time()

    if not hasattr(upload_progress, "last_update"):
        upload_progress.last_update = 0

    if now - upload_progress.last_update < 2 and current < total:
        return

    upload_progress.last_update = now

    elapsed = max(now - start_time, 0.001)
    speed = current / elapsed
    percent = current * 100 / total if total else 0

    filled = int(percent / 10)
    bar = "█" * filled + "░" * (10 - filled)

    remaining = (total - current) / speed if total and speed else 0
    eta = time.strftime("%H:%M:%S", time.gmtime(remaining))

    text = (
        f"📤 Uploading to Telegram...\n\n"
        f"[{bar}] {percent:.1f}%\n"
        f"📦 Uploaded: {current / 1024**2:.2f} MB\n"
        f"📁 Total: {total / 1024**2:.2f} MB\n"
        f"⚡ Speed: {speed / 1024**2:.2f} MB/s\n"        
        
        f"⏳ Remaining: {eta}"
    )

    try:
        await status.edit_text(text)
    except Exception:
        pass




# =====================================
# INDIVIDUAL DOWNLOAD QUEUE V2
# =====================================

async def run_queue_job(job_id, message, status):

    job = queue_manager.get_job(job_id)
    if not job:
        return

    filename = job["filename"]
    url = job["url"]

    final_path = DOWNLOADS / f"{job_id}_{filename}"
    part_path = DOWNLOADS / f".{job_id}.part"
    recovery_id = f"queue_{job_id}"

    try:
        await save_job({
            "id": recovery_id,
            "chat_id": job["chat_id"],
            "url": url,
            "part_path": str(final_path),
            "final_path": str(final_path),
            "filename": filename,
            "status": "downloading"
        })

        await status.edit_text(
            f"📥 Queued: {filename}\n🆔 {job_id}"
        )

        async with semaphore:

            await queue_manager.set_status(
                job_id, "Downloading"
            )

            start_time = time.time()
            last_update = 0

            async def job_progress(current, total):
                nonlocal last_update

                await queue_manager.update_progress(
                    job_id, current, total
                )

                now = time.time()

                if now - last_update >= 2:
                    await progress(
                        current, total, status, start_time
                    )
                    last_update = now

            # V3 AUTO RETRY
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    await parallel_download(
                        url,
                        final_path,
                        job["pause_event"],
                        job_progress
                    )
                    break

                except asyncio.CancelledError:
                    raise

                except Exception as error:
                    if attempt == MAX_RETRIES:
                        raise

                    try:
                        await status.edit_text(
                            f"⚠️ Download failed!\n\n"
                            f"📁 {filename}\n"
                            f"🔄 Retry: {attempt}/{MAX_RETRIES - 1}\n"
                            f"⏳ Retrying in {RETRY_DELAY * attempt} seconds..."
                        )
                    except Exception:
                        pass

                    update_stats("retries")
                    await asyncio.sleep(RETRY_DELAY * attempt)

        if not final_path.is_file():
            raise ValueError("Downloaded file missing")

        if final_path.stat().st_size <= 0:
            raise ValueError("Downloaded file is empty")
        await remove_job(recovery_id)

        downloaded = final_path.stat().st_size

        with open(HISTORY_FILE, "a") as history:
            history.write(
                f"{datetime.now()} | {final_path.name} | "
                f"{downloaded / 1024**2:.2f} MB\n"
            )

        await queue_manager.finish_job(
            job_id, "Completed"
        )
        update_stats("total", "success")

        await status.edit_text(
            f"✅ DOWNLOAD COMPLETED\n\n"
            f"📁 {final_path.name}\n"
            f"🆔 {job_id}\n"
            f"💾 {downloaded / 1024**2:.2f} MB"
        )

        if downloaded <= 2 * 1024**3:
            try:
                await message.reply_document(
                    document=str(final_path),
                    caption=f"📁 {final_path.name}"
                )
            except Exception as upload_error:
                await message.reply_text(
                    f"⚠️ Download completed, upload failed: "
                    f"{str(upload_error)[:150]}"
                )

    except asyncio.CancelledError:
        await queue_manager.finish_job(
            job_id, "Cancelled"
        )

        try:
            await status.edit_text(
                f"⏹️ Cancelled: {filename}\n"
                f"🆔 {job_id}\n"
                f"Partial data preserved."
            )
        except Exception:
            pass

        raise

    except Exception as error:
        await queue_manager.finish_job(
            job_id, "Failed"
        )
        update_stats("total", "failed")

        try:
            await status.edit_text(
                f"❌ Failed: {filename}\n"
                f"🆔 {job_id}\n"
                f"{str(error)[:180]}"
            )
        except Exception:
            pass


@app.on_message(filters.command("download"))
async def download(client, message):

    if not is_owner(message):
        return

    if len(message.command) < 2:
        return await message.reply_text(
            "Usage: /download URL"
        )

    args = message.text.split(maxsplit=1)[1].split()

    priority = "normal"

    if args[0].lower() in ("high", "normal", "low"):
        priority = args.pop(0).lower()

    urls = args

    if not urls:
        return await message.reply_text("Usage: /download [high|normal|low] URL")

    if len(urls) > 5:
        return await message.reply_text(
            "Maximum 5 URLs allowed."
        )

    created = []
    pending = []

    for url in urls:

        parsed = urlparse(url)

        if (
            parsed.scheme not in ("http", "https")
            or not parsed.hostname
        ):
            continue

        filename = Path(
            unquote(os.path.basename(parsed.path))
        ).name

        filename = filename or "download.bin"

        job_id = await queue_manager.create_job(
            message.chat.id,
            filename,
            url,
            priority=priority
        )

        status = await message.reply_text(
            f"📋 Job Queued\n"
            f"📁 {filename}\n"
            f"🆔 {job_id}"
        )

        pending.append((job_id, message, status))
        created.append(job_id)

    if not created:
        return await message.reply_text(
            "❌ No valid URLs provided."
        )

    for job_id, job_message, job_status in pending:
        job = queue_manager.get_job(job_id)

        if job:
            scheduler_queue.put_nowait((
                job["priority_value"],
                job["sequence"],
                job_id,
                job_message,
                job_status
            ))

    start_priority_scheduler()

    await message.reply_text(
        f"✅ {len(created)} job(s) added to queue."
    )


@app.on_message(filters.command("upload"))
async def upload(client, message):
    if not is_owner(message):
        return

    if len(message.command) < 2:
        return await message.reply_text("Usage: /upload filename")

    filename = Path(message.text.split(maxsplit=1)[1]).name
    path = DOWNLOADS / filename

    if not path.is_file():
        return await message.reply_text("❌ File not found.")

    if path.stat().st_size > 2 * 1024**3:
        return await message.reply_text(
            "❌ File exceeds this bot's 2GB upload limit."
        )

    status_msg = await message.reply_text("📤 Uploading...")

    try:
        await message.reply_document(
            document=str(path),
            caption=f"📁 {filename}"
        )
        await status_msg.delete()

    except Exception as error:
        await status_msg.edit_text(
            f"❌ Upload Error: {str(error)[:250]}"
        )


@app.on_message(filters.command("rename"))
async def rename(client, message):
    if not is_owner(message):
        return

    if len(message.command) < 2 or "|" not in message.text:
        return await message.reply_text(
            "Usage: /rename old|new"
        )

    old, new = message.text.split(maxsplit=1)[1].split("|", 1)

    old_path = DOWNLOADS / Path(old.strip()).name
    new_path = DOWNLOADS / Path(new.strip()).name

    if not old_path.is_file():
        return await message.reply_text("❌ Original file not found.")

    if new_path.exists():
        return await message.reply_text("❌ New filename already exists.")

    old_path.rename(new_path)

    await message.reply_text(
        f"✅ Renamed to {new_path.name}"
    )


@app.on_message(filters.command("zip"))
async def create_zip(client, message):
    if not is_owner(message):
        return

    if len(message.command) < 2:
        return await message.reply_text("Usage: /zip filename")

    filename = Path(message.text.split(maxsplit=1)[1]).name
    source = DOWNLOADS / filename

    if not source.is_file():
        return await message.reply_text("❌ File not found.")

    zip_path = DOWNLOADS / f"{filename}.zip"

    if zip_path.exists():
        return await message.reply_text("❌ ZIP already exists.")

    with zipfile.ZipFile(
        zip_path,
        "w",
        zipfile.ZIP_DEFLATED
    ) as archive:
        archive.write(source, arcname=source.name)

    await message.reply_text(
        f"✅ ZIP Created: {zip_path.name}"
    )


@app.on_message(filters.command("extract"))
async def extract_zip(client, message):
    if not is_owner(message):
        return

    if len(message.command) < 2:
        return await message.reply_text(
            "Usage: /extract filename.zip"
        )

    filename = Path(message.text.split(maxsplit=1)[1]).name
    source = DOWNLOADS / filename

    if not source.is_file():
        return await message.reply_text("❌ ZIP not found.")

    if not zipfile.is_zipfile(source):
        return await message.reply_text("❌ Invalid ZIP file.")

    destination = DOWNLOADS / source.stem

    if destination.exists():
        return await message.reply_text(
            "❌ Extraction folder already exists."
        )

    try:
        with zipfile.ZipFile(source, "r") as archive:
            for info in archive.infolist():
                target = (destination / info.filename).resolve()

                if not target.is_relative_to(destination.resolve()):
                    raise ValueError("Unsafe ZIP path detected.")

            archive.extractall(destination)

        await message.reply_text(
            f"✅ Extracted successfully!\n📁 Folder: {destination.name}"
        )

    except Exception as error:
        await message.reply_text(
            f"❌ Extraction Error: {str(error)[:250]}"
        )


print("AllInOneLeech Full Bot Started!")
@app.on_message(filters.command("history"))
async def history(client, message):
    if not is_owner(message):
        return

    if not HISTORY_FILE.exists():
        return await message.reply_text("📂 No Download History.")

    data = HISTORY_FILE.read_text()

    if not data.strip():
        return await message.reply_text("📂 History is Empty.")

    await message.reply_text(
        "📜 DOWNLOAD HISTORY\n\n" + data[-3500:]
    )


@app.on_message(filters.command("gdrive"))
async def gdrive_download(client, message):
    if not is_owner(message):
        return

    if len(message.command) < 2:
        return await message.reply_text(
            "Usage: /gdrive Google_Drive_Link"
        )

    url = message.text.split(maxsplit=1)[1].strip()

    if "drive.google.com" not in url and "docs.google.com" not in url:
        return await message.reply_text("❌ Invalid Google Drive URL.")

    status = await message.reply_text(
        "⏳ Google Drive Download Starting..."
    )

    path = DOWNLOADS / f"gdrive_{message.id}"

    try:
        result = await asyncio.to_thread(
            gdown.download,
            url=url,
            output=str(path),
            quiet=True,
            retries=2,
            timeout=60
        )

        if not result or not path.is_file():
            return await status.edit_text(
                "❌ Download Failed. Check Drive permissions."
            )

        size = path.stat().st_size

        if size > MAX_SIZE:
            path.unlink()
            return await status.edit_text(
                "❌ File exceeds 4GB limit."
            )

        await status.edit_text(
            "✅ Google Drive Download Completed!\n"
            f"📁 File: {path.name}\n"
            f"📦 Size: {size / 1024**2:.2f} MB"
        )

    except Exception as error:
        if path.exists():
            path.unlink()

        await status.edit_text(
            f"❌ Google Drive Error: {str(error)[:250]}"
        )


# SPEED TEST
@app.on_message(filters.command("speedtest"))
async def speedtest_command(client, message):
    if not is_owner(message):
        return

    status = await message.reply_text("⚡ Running Speed Test...")

    try:
        import speedtest
        import aiohttp
        import statistics

        def run_speed_test():
            test = speedtest.Speedtest()
            server = test.get_best_server()
            download = test.download() / 1_000_000
            upload = test.upload() / 1_000_000
            return (
                download,
                upload,
                server.get("name", "Unknown"),
                server.get("country", "Unknown")
            )

        download, upload, server_name, country = (
            await asyncio.to_thread(run_speed_test)
        )

        times = []
        failed = 0
        url = "https://www.cloudflare.com/cdn-cgi/trace"

        timeout = aiohttp.ClientTimeout(total=8)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            for _ in range(5):
                try:
                    start_time = time.perf_counter()

                    async with session.get(url) as response:
                        await response.read()
                        if response.status >= 500:
                            raise Exception("HTTP server error")

                    elapsed = (time.perf_counter() - start_time) * 1000
                    times.append(elapsed)

                except Exception:
                    failed += 1

                await asyncio.sleep(0.3)

        ping = statistics.mean(times) if times else None

        jitter = (
            statistics.mean(
                abs(times[i] - times[i - 1])
                for i in range(1, len(times))
            )
            if len(times) > 1 else None
        )

        packet_loss = failed / 5 * 100

        ping_text = f"{ping:.2f} ms" if ping is not None else "N/A"
        jitter_text = f"{jitter:.2f} ms" if jitter is not None else "N/A"

        text = "\\n".join([
            "🚀 INTERNET SPEED TEST",
            "",
            f"📥 Download: {download:.2f} Mbps",
            f"📤 Upload: {upload:.2f} Mbps",
            f"📡 HTTP Ping: {ping_text}",
            f"📊 Jitter: {jitter_text}",
            f"📉 Packet Loss: {packet_loss:.1f}%",
            f"🖥️ Server: {server_name}",
            f"🌍 Country: {country}"
        ])

        await status.edit_text(text)

    except Exception as error:
        await status.edit_text(
            f"❌ Speedtest Error: {str(error)[:200]}"
        )


# DELETE FILE
@app.on_message(filters.command("delete"))
async def delete_file(client, message):
    if not is_owner(message):
        return

    if len(message.command) < 2:
        return await message.reply_text("Usage: /delete filename")

    filename = Path(message.text.split(maxsplit=1)[1]).name
    path = DOWNLOADS / filename

    if not path.is_file():
        return await message.reply_text("❌ File not found.")

    path.unlink()
    await message.reply_text(f"✅ Deleted: {filename}")


# CLEAR HISTORY
@app.on_message(filters.command("clear"))
async def clear_history(client, message):
    if not is_owner(message):
        return

    HISTORY_FILE.write_text("")
    await message.reply_text("✅ Download History Cleared!")




# LIVE SYSTEM DASHBOARD
import shutil
import time

BOT_START_TIME = time.time()

@app.on_message(filters.command("dashboard"))
async def dashboard_command(client, message):
    if not is_owner(message):
        return

    try:
        # RAM information from Android /proc
        ram_total = 0
        ram_available = 0

        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    ram_total = int(line.split()[1])
                elif line.startswith("MemAvailable:"):
                    ram_available = int(line.split()[1])

        ram_used = ram_total - ram_available
        ram_percent = (
            ram_used * 100 / ram_total if ram_total else 0
        )

        # Storage information
        total, used, free = shutil.disk_usage(".")

        storage_percent = used * 100 / total if total else 0

        # CPU load average
        try:
            cpu_load = os.getloadavg()[0]
        except (AttributeError, OSError):
            cpu_load = 0

        # Uptime
        uptime = int(time.time() - BOT_START_TIME)
        hours, remainder = divmod(uptime, 3600)
        minutes, seconds = divmod(remainder, 60)

        text = (
            "🚀 AllInOneLeech Dashboard\n\n"
            "🟢 Bot Status: Online\n\n"
            "📊 SYSTEM\n"
            f"🧠 RAM Used: {ram_percent:.1f}%\n"
            f"💾 Storage Used: {storage_percent:.1f}%\n"
            f"⚙️ CPU Load Average: {cpu_load:.2f}\n\n"
            "📥 DOWNLOADS\n"
            f"⚡ Active Downloads: {len(active_downloads)}\n"
            f"🔄 Max Concurrent: {MAX_CONCURRENT}\n\n"
            f"⏱️ Uptime: {hours}h {minutes}m {seconds}s"
        )

        await message.reply_text(text)

    except Exception as error:
        await message.reply_text(
            f"❌ Dashboard Error: {str(error)[:200]}"
        )




# STORAGE MANAGEMENT COMMAND
import shutil

@app.on_message(filters.command("storage"))
async def storage_command(client, message):
    if not is_owner(message):
        return

    try:
        total, used, free = shutil.disk_usage("/storage/emulated/0")

        percent = used * 100 / total if total else 0

        folder_size = sum(
            f.stat().st_size
            for f in DOWNLOADS.rglob("*")
            if f.is_file()
        )

        gb = 1024 ** 3

        warning = (
            "🔴 Storage Almost Full!"
            if percent >= 90
            else "⚠️ Storage Getting Full!"
            if percent >= 80
            else "🟢 Storage Healthy!"
        )

        text = (
            "💾 STORAGE MANAGEMENT\n\n"
            f"📦 Total: {total / gb:.2f} GB\n"
            f"🔴 Used: {used / gb:.2f} GB\n"
            f"🟢 Free: {free / gb:.2f} GB\n"
            f"📊 Usage: {percent:.1f}%\n\n"
            f"📁 Downloads Folder: {folder_size / 1024**2:.2f} MB\n\n"
            f"{warning}"
        )

        await message.reply_text(text)

    except Exception as error:
        await message.reply_text(
            f"❌ Storage Error: {str(error)[:200]}"
        )




# LARGE FILE FINDER
@app.on_message(filters.command("largefiles"))
async def largefiles_command(client, message):
    if not is_owner(message):
        return

    try:
        files = []

        for path in DOWNLOADS.rglob("*"):
            if path.is_file():
                try:
                    size = path.stat().st_size
                    if size >= 10 * 1024 * 1024:
                        files.append((path, size))
                except OSError:
                    continue

        files.sort(key=lambda x: x[1], reverse=True)

        if not files:
            return await message.reply_text(
                "✅ No files larger than 10 MB found."
            )

        lines = ["📁 LARGE FILE REPORT\n"]

        for path, size in files[:30]:
            lines.append(
                f"📄 {path.relative_to(DOWNLOADS)}\n"
                f"   💾 {size / 1024**2:.2f} MB"
            )

        lines.append(f"\n📊 Total Large Files: {len(files)}")

        await message.reply_text("\n".join(lines))

    except Exception as error:
        await message.reply_text(
            f"❌ Error: {str(error)[:200]}"
        )




# SMART CLEANUP SYSTEM
cleanup_pending = {}

@app.on_message(filters.command("cleanup"))
async def cleanup_command(client, message):
    if not is_owner(message):
        return

    try:
        files = []

        for path in DOWNLOADS.rglob("*"):
            if path.is_file():
                try:
                    size = path.stat().st_size
                    if size >= 10 * 1024 * 1024:
                        files.append((path, size))
                except OSError:
                    continue

        files.sort(key=lambda x: x[1], reverse=True)

        if not files:
            return await message.reply_text(
                "✅ No large files found!"
            )

        buttons = []

        for path, size in files[:15]:
            token = __import__("secrets").token_hex(4)
            cleanup_pending[token] = path

            buttons.append([
                pyrogram.types.InlineKeyboardButton(
                    f"🗑️ {path.name[:25]} ({size / 1024**2:.1f} MB)",
                    callback_data=f"cln:ask:{token}"
                )
            ])

        await message.reply_text(
            "🧹 SMART CLEANUP\n\n"
            "Select a file to delete.\n"
            "No file will be deleted without confirmation.",
            reply_markup=pyrogram.types.InlineKeyboardMarkup(buttons)
        )

    except Exception as error:
        await message.reply_text(
            f"❌ Cleanup Error: {str(error)[:150]}"
        )


@app.on_callback_query(filters.regex(r"^cln:"))
async def cleanup_callback(client, query):
    if not is_owner(query):
        return await query.answer("Not authorized!", show_alert=True)

    parts = query.data.split(":")
    action = parts[1]
    token = parts[2]

    path = cleanup_pending.get(token)

    if not path:
        return await query.answer("File expired or not found!", show_alert=True)

    if action == "ask":
        await query.answer()

        await query.message.edit_text(
            f"⚠️ DELETE CONFIRMATION\n\n"
            f"📁 File: {path.name}\n"
            f"📦 Size: {path.stat().st_size / 1024**2:.2f} MB\n\n"
            "Are you sure you want to delete this file?",
            reply_markup=pyrogram.types.InlineKeyboardMarkup([
                [
                    pyrogram.types.InlineKeyboardButton(
                        "✅ Confirm Delete",
                        callback_data=f"cln:yes:{token}"
                    ),
                    pyrogram.types.InlineKeyboardButton(
                        "❌ Cancel",
                        callback_data=f"cln:no:{token}"
                    )
                ]
            ])
        )

    elif action == "yes":
        try:
            resolved = path.resolve()
            root = DOWNLOADS.resolve()

            if root not in resolved.parents or not resolved.is_file():
                raise ValueError("Invalid file or file not found")

            size = resolved.stat().st_size
            resolved.unlink()
            cleanup_pending.pop(token, None)

            await query.answer("File deleted!")
            await query.message.edit_text(
                f"✅ FILE DELETED\n\n"
                f"📁 {path.name}\n"
                f"💾 Freed: {size / 1024**2:.2f} MB"
            )

        except Exception as error:
            await query.answer(str(error)[:150], show_alert=True)

    elif action == "no":
        cleanup_pending.pop(token, None)
        await query.answer("Cancelled!")
        await query.message.edit_text("❌ Cleanup cancelled. File is safe.")





# DOWNLOAD TASK TRACKING
def _clear_download_task(chat_id, task):
    if active_downloads.get(chat_id) is task:
        active_downloads.pop(chat_id, None)



# =====================================
# INDIVIDUAL QUEUE CONTROLS
# =====================================

@app.on_message(filters.command("queue"))
async def queue_command(client, message):

    if not is_owner(message):
        return

    jobs = queue_manager.get_chat_jobs(message.chat.id)

    if not jobs:
        return await message.reply_text(
            "📭 No download jobs found."
        )

    lines = ["📥 INDIVIDUAL DOWNLOAD QUEUE\n"]

    for job in jobs[-20:]:
        lines.append(
            f"📁 {job['filename']}\n"
            f"🆔 {job['id']}\n"
            f"📊 {job['status']} | "
            f"{job['progress']:.1f}%\n"
        )

    lines.append(
        f"\n⚡ Active: "
        f"{len([j for j in jobs if j['status'] in ('Queued', 'Downloading', 'Paused', 'Cancelling')])}"
        f"\n🔄 Max Concurrent: {MAX_CONCURRENT}"
    )

    await message.reply_text("\n".join(lines))


@app.on_message(filters.command("pause"))
async def pause_command(client, message):

    if not is_owner(message):
        return

    if len(message.command) < 2:
        return await message.reply_text(
            "Usage: /pause JOB_ID"
        )

    job_id = message.command[1]
    job = queue_manager.get_job(job_id)

    if not job or job["chat_id"] != message.chat.id:
        return await message.reply_text("❌ Job not found.")

    result = await queue_manager.pause_job(job_id)

    await message.reply_text(
        "⏸️ Job Paused!" if result
        else "❌ Job cannot be paused."
    )


@app.on_message(filters.command("resume"))
async def resume_command(client, message):

    if not is_owner(message):
        return

    if len(message.command) < 2:
        return await message.reply_text(
            "Usage: /resume JOB_ID"
        )

    job_id = message.command[1]
    job = queue_manager.get_job(job_id)

    if not job or job["chat_id"] != message.chat.id:
        return await message.reply_text("❌ Job not found.")

    result = await queue_manager.resume_job(job_id)

    await message.reply_text(
        "▶️ Job Resumed!" if result
        else "❌ Job cannot be resumed."
    )


@app.on_message(filters.command("cancel"))
async def cancel(client, message):

    if not is_owner(message):
        return

    if len(message.command) < 2:
        return await message.reply_text(
            "Usage: /cancel JOB_ID"
        )

    job_id = message.command[1]
    job = queue_manager.get_job(job_id)

    if not job or job["chat_id"] != message.chat.id:
        return await message.reply_text("❌ Job not found.")

    result = await queue_manager.cancel_job(job_id)

    await message.reply_text(
        "⏹️ Job cancellation requested."
        if result else "❌ Job cannot be cancelled."
    )


# RECOVERY COMMAND
@app.on_message(filters.command("recover"))
async def recover_command(client, message):
    if not is_owner(message):
        return

    chat_id = message.chat.id

    jobs = [
        job for job in get_recoverable_jobs()
        if int(job.get("chat_id", 0)) == chat_id
    ]

    if not jobs:
        return await message.reply_text(
            "No recoverable downloads found."
        )

    existing = active_downloads.get(chat_id)
    if existing and not existing.done():
        return await message.reply_text(
            "A download is already running."
        )

    active_downloads[chat_id] = asyncio.current_task()
    pause_events[chat_id] = asyncio.Event()
    pause_events[chat_id].set()

    status = await message.reply_text(
        f"Found {len(jobs)} recovery job(s)."
    )

    results = []

    try:
        for job in jobs:
            part = Path(job["part_path"])
            final = Path(job["final_path"])

            try:
                if final.exists():
                    raise FileExistsError(
                        f"{final.name} already exists"
                    )

                await status.edit_text(
                    f"Recovering {job['filename']}..."
                )

                recovery_destination = (
                    final if part == final else part
                )

                async with semaphore:
                    await parallel_download(
                        job["url"],
                        recovery_destination,
                        pause_events[chat_id]
                    )

                if not recovery_destination.is_file():
                    raise ValueError("Recovered file missing")

                if recovery_destination.stat().st_size <= 0:
                    raise ValueError("Recovered file is empty")

                if recovery_destination != final:
                    os.replace(recovery_destination, final)
                await remove_job(job["id"])

                results.append(f"Recovered: {final.name}")

                if final.stat().st_size <= 2 * 1024**3:
                    await message.reply_document(
                        document=str(final),
                        caption=final.name
                    )

            except Exception as error:
                results.append(
                    f"Failed: {job['filename']} - {str(error)[:120]}"
                )

        await status.edit_text("\n".join(results))

    finally:
        active_downloads.pop(chat_id, None)
        pause_events.pop(chat_id, None)
        download_status.pop(chat_id, None)

async def setup_bot_commands():
    commands = [
        BotCommand("start", "Start the bot"),
        BotCommand("help", "Show all commands"),
        BotCommand("status", "Bot status"),
        BotCommand("files", "File manager"),
        BotCommand("download", "Download files"),
        BotCommand("upload", "Upload files"),
        BotCommand("rename", "Rename files"),
        BotCommand("zip", "Compress files"),
        BotCommand("extract", "Extract archives"),
        BotCommand("gdrive", "Google Drive download"),
        BotCommand("delete", "Delete a file"),
        BotCommand("clear", "Clear downloads"),
        BotCommand("history", "Download history"),
        BotCommand("speedtest", "Internet speed test"),
        BotCommand("dashboard", "Bot dashboard"),
        BotCommand("storage", "Storage information"),
        BotCommand("largefiles", "Find large files"),
        BotCommand("cleanup", "Cleanup downloads"),
        BotCommand("queue", "View download queue"),
        BotCommand("pause", "Pause a download"),
        BotCommand("resume", "Resume a download"),
        BotCommand("cancel", "Cancel a download"),
        BotCommand("recover", "Recover downloads"),
    ]

    await app.set_bot_commands(commands)


async def post_init(client):
    await queue_manager.restore()
    print("Persistent Queue Restored!")

    start_priority_scheduler()
    print("Restoring pending jobs to scheduler...")

    for job in queue_manager.get_active_jobs():
        if job["status"] != "Queued":
            continue

        chat_id = job["chat_id"]

        try:
            message = await client.send_message(
                chat_id,
                f"🔄 Restored Job\n"
                f"📁 {job['filename']}\n"
                f"🆔 {job['id']}"
            )

            scheduler_queue.put_nowait((
                job["priority_value"],
                job["sequence"],
                job["id"],
                message,
                message
            ))

        except Exception as error:
            print(f"Restore scheduling failed: {error}")

    await setup_bot_commands()
    print("Bot Commands Menu Registered!")

async def main():
    await app.start()
    try:
        await post_init(app)
        print("AllInOneLeech Started Successfully!")
        await pyrogram.idle()
    finally:
        await app.stop()

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
