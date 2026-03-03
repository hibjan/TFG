import asyncio
import aiohttp
import gzip
import json
import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from aiolimiter import AsyncLimiter

load_dotenv(".env")

API_TOKEN = os.getenv("TMDB_API_KEY")
if not API_TOKEN:
    raise RuntimeError("TMDB_API_KEY not found in environment. Check your .env file.")

HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "accept": "application/json"
}

RATE_LIMIT = 40
NUM_WORKERS = 50
MAX_RETRIES = 5
WRITE_BUFFER_SIZE = 50

EXPORT_BASE_URL = "https://files.tmdb.org/p/exports"
EXPORT_DIR = "scripts/TMDB/bulk_export_ids"

def build_endpoints(date):
    return [
        {
            "name": "movies",
            "url": "https://api.themoviedb.org/3/movie/{}?append_to_response=credits",
            "export_name": "movie_ids",
            "input_file": os.path.join(EXPORT_DIR, f"movie_ids_{date}.json"),
            "output_file": "scripts/TMDB/raw_data/movies.jsonl",
            "input_key": "id"
        },
        {
            "name": "tv series",
            "url": "https://api.themoviedb.org/3/tv/{}?append_to_response=credits",
            "export_name": "tv_series_ids",
            "input_file": os.path.join(EXPORT_DIR, f"tv_series_ids_{date}.json"),
            "output_file": "scripts/TMDB/raw_data/tv_series.jsonl",
            "input_key": "id"
        },
        {
            "name": "people",
            "url": "https://api.themoviedb.org/3/person/{}",
            "export_name": "person_ids",
            "input_file": os.path.join(EXPORT_DIR, f"person_ids_{date}.json"),
            "output_file": "scripts/TMDB/raw_data/people.jsonl",
            "input_key": "id"
        },
         {
            "name": "companies",
            "url": "https://api.themoviedb.org/3/company/{}",
            "export_name": "production_company_ids",
            "input_file": os.path.join(EXPORT_DIR, f"production_company_ids_{date}.json"),
            "output_file": "scripts/TMDB/raw_data/companies.jsonl",
            "input_key": "id"
        },
        {
            "name": "tv networks",
            "url": "https://api.themoviedb.org/3/network/{}",
            "export_name": "tv_network_ids",
            "input_file": os.path.join(EXPORT_DIR, f"tv_network_ids_{date}.json"),
            "output_file": "scripts/TMDB/raw_data/tv_networks.jsonl",
            "input_key": "id"
        }
    ]


async def download_export(session, export_name, date, output_path):
    """Download and decompress a .json.gz export file from TMDB."""
    url = f"{EXPORT_BASE_URL}/{export_name}_{date}.json.gz"
    print(f"Downloading {url} ...")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    try:
        async with session.get(url) as response:
            if response.status != 200:
                print(f"[ERROR] Failed to download {url} — status {response.status}")
                return False

            compressed = await response.read()

    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"[ERROR] Failed to download {url} — {e}")
        return False

    decompressed = gzip.decompress(compressed)

    with open(output_path, "wb") as f:
        f.write(decompressed)

    size_mb = len(decompressed) / (1024 * 1024)
    print(f"Downloaded {export_name} ({size_mb:.1f} MB)")
    return True


def count_lines(jsonl_path):
    """Count non-empty lines in a JSONL file for progress tracking."""
    count = 0
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                count += 1
    return count


def get_existing_ids(output_file):
    """Read already-fetched IDs from the output file for resumability."""
    existing = set()
    if os.path.exists(output_file):
        with open(output_file, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    try:
                        obj = json.loads(line)
                        existing.add(obj.get("id"))
                    except json.JSONDecodeError:
                        continue
    return existing


def stream_inputs(jsonl_path, key):
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                obj = json.loads(line)
                yield obj[key]


async def fetch(session, limiter, url, retries=MAX_RETRIES):
    async with limiter:
        try:
            async with session.get(url, headers=HEADERS) as response:

                if response.status == 429:
                    if retries <= 0:
                        print(f"[ERROR] {url} — max retries reached on 429")
                        return None
                    retry_after = int(response.headers.get("Retry-After", 1))
                    print(f"[WARN] 429 on {url}, retrying in {retry_after}s ({retries} left)")
                    await asyncio.sleep(retry_after)
                    return await fetch(session, limiter, url, retries=retries - 1)

                if response.status != 200:
                    print(f"[WARN] {url} returned status {response.status}")
                    return None

                return await response.json()

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if retries <= 0:
                print(f"[ERROR] {url} — network error after retries: {e}")
                return None
            await asyncio.sleep(1)
            return await fetch(session, limiter, url, retries=retries - 1)


def flush_buffer(output_file, buffer):
    """Write buffered records to disk in one operation."""
    if not buffer:
        return
    with open(output_file, "a", encoding="utf-8") as f:
        for data in buffer:
            f.write(json.dumps(data, ensure_ascii=False) + "\n")


def format_eta(seconds):
    """Format seconds into a human-readable ETA string.
    """
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}:{secs:02d}m"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) / 60)
        return f"{hours}:{minutes:02d}h"


async def writer_task(write_queue, output_file):
    """Dedicated coroutine that drains the write queue and flushes to disk."""
    buffer = []
    while True:
        item = await write_queue.get()

        if item is None:  # sentinel to stop
            flush_buffer(output_file, buffer)
            break

        buffer.append(item)

        if len(buffer) >= WRITE_BUFFER_SIZE:
            flush_buffer(output_file, buffer)
            buffer.clear()


async def process_endpoint(session, limiter, config):
    name = config["name"]
    url_template = config["url"]
    input_file = config["input_file"]
    output_file = config["output_file"]
    input_key = config["input_key"]

    # Ensure output directory exists once
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Load already-fetched IDs for resumability
    existing_ids = get_existing_ids(output_file)
    if existing_ids:
        print(f"[{name}] Resuming — {len(existing_ids)} already fetched, skipping them")

    # Count total for progress/ETA
    total = count_lines(input_file)

    print(f"\nStarting endpoint: {name} ({total} total IDs)")
    start_time = time.time()

    processed = 0
    failed = 0
    skipped = 0
    last_report = time.time()

    # Dedicated writer running in background
    write_queue = asyncio.Queue()
    writer = asyncio.create_task(writer_task(write_queue, output_file))

    # Bounded input queue — workers pull from this
    input_queue = asyncio.Queue(maxsize=NUM_WORKERS * 2)

    async def worker():
        nonlocal processed, failed, last_report
        while True:
            item_id = await input_queue.get()
            if item_id is None:  # poison pill
                break

            url = url_template.format(item_id)
            data = await fetch(session, limiter, url)

            if data:
                await write_queue.put(data)
                processed += 1
            else:
                failed += 1

            # Print progress every 2 seconds
            now = time.time()
            if now - last_report >= 2:
                last_report = now
                elapsed = now - start_time
                done = processed + failed + skipped
                rate = (processed + failed) / elapsed if elapsed > 0 else 0
                remaining = total - done
                eta = remaining / rate if rate > 0 else 0
                print(
                    f"\r[{name}] {done:,}/{total:,} | "
                    f"OK: {processed:,} | Failed: {failed:,} | Skipped: {skipped:,} | "
                    f"{rate:.1f} req/s | ETA: {format_eta(eta)}          ",
                    end="", flush=True
                )

    # Start fixed worker pool
    workers = [asyncio.create_task(worker()) for _ in range(NUM_WORKERS)]

    # Producer: feed IDs into the queue (backpressure via maxsize)
    for item_id in stream_inputs(input_file, input_key):
        if item_id in existing_ids:
            skipped += 1
            continue
        await input_queue.put(item_id)

    # Send poison pills to shut down all workers
    for _ in range(NUM_WORKERS):
        await input_queue.put(None)

    await asyncio.gather(*workers)

    # Signal writer to flush and stop
    await write_queue.put(None)
    await writer

    elapsed = time.time() - start_time
    rate = (processed + failed) / elapsed if elapsed > 0 else 0

    # Final progress flush to reach 100% before completion
    done = processed + failed + skipped
    print(
        f"\r[{name}] {done:,}/{total:,} | "
        f"OK: {processed:,} | Failed: {failed:,} | Skipped: {skipped:,} | "
        f"{rate:.1f} req/s | ETA: 0s          ",
        end="", flush=True
    )
    
    print(
        f"\nFinished endpoint: {name} in {format_eta(elapsed)} — "
        f"{processed:,} OK, {failed:,} failed, {skipped:,} skipped ({rate:.1f} req/s)"
    )


async def main():
    # Use yesterday's date (TMDB exports are from the previous day)
    yesterday = datetime.now() - timedelta(days=1)
    date = yesterday.strftime("%m_%d_%Y")
    print(f"Using export date: {date}")

    endpoints = build_endpoints(date)

    limiter = AsyncLimiter(RATE_LIMIT, time_period=1)

    # Tuned TCP connector: enough connections to keep the pipeline full
    connector = aiohttp.TCPConnector(limit=NUM_WORKERS, limit_per_host=NUM_WORKERS)

    download_timeout = aiohttp.ClientTimeout(total=300)
    api_timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(timeout=download_timeout) as dl_session:
        async with aiohttp.ClientSession(
            timeout=api_timeout, connector=connector
        ) as api_session:
            for endpoint in endpoints:
                # Download the export file
                success = await download_export(
                    dl_session,
                    endpoint["export_name"],
                    date,
                    endpoint["input_file"]
                )

                if not success:
                    print(f"[SKIP] Skipping {endpoint['name']} — download failed")
                    continue

                # Process the endpoint
                await process_endpoint(api_session, limiter, endpoint)

                # Clean up the downloaded export file
                try:
                    os.remove(endpoint["input_file"])
                    print(f"Removed {endpoint['input_file']}")
                except OSError as e:
                    print(f"[WARN] Could not remove {endpoint['input_file']}: {e}")

    print("\nAll endpoints finished.")


if __name__ == "__main__":
    asyncio.run(main())