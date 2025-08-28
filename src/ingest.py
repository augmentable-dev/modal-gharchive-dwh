import modal
from datetime import date
from .app import (
    app,
    downloaded,
    DOWNLOADED_DATA_PATH,
    parquet,
    PARQUET_DATA_PATH,
    gharchive,
    GHARCHIVE_DATA_PATH,
)


@app.function(
    volumes={GHARCHIVE_DATA_PATH: gharchive},
    cloud="aws",
    region="us-east",
    timeout=36000,
    retries=3,
    # min_containers=500,
    ephemeral_disk=600 * 1024,
)
@modal.concurrent(max_inputs=8)
def download_and_copy_day(year: int, month: int, day: int):
    """Download all JSON.gz files for a specific day into a temporary directory and then copy them into the gharchive volume."""
    import os
    import tempfile
    import time
    import shutil
    import pycurl

    start = time.time()
    tmp_dir = f"/tmp/gharchive/{year}/{month:02d}/{day:02d}"
    os.makedirs(tmp_dir, exist_ok=True)

    for hour in range(24):
        url = f"https://data.gharchive.org/{year}-{month:02d}-{day:02d}-{hour}.json.gz"
        filepath = os.path.join(tmp_dir, f"{year}-{month:02d}-{day:02d}-{hour}.json.gz")
        hour_start = time.time()

        # Create temp file in same dir
        tmp_fd, tmp_path = tempfile.mkstemp(
            dir=os.path.dirname(filepath),
            prefix=f"{year}-{month:02d}-{day:02d}-{hour}.json.gz",
        )
        os.close(tmp_fd)

        with open(tmp_path, "wb") as f:
            c = pycurl.Curl()
            c.setopt(c.FOLLOWLOCATION, 1)
            c.setopt(c.URL, url)
            c.setopt(c.WRITEDATA, f)
            c.perform()
            c.close()

        # fsync the temp file to ensure data durability
        with open(tmp_path, "rb", buffering=0) as f:
            os.fsync(f.fileno())
            file_size = os.fstat(f.fileno()).st_size

        print(
            f"Downloaded {filepath} ({file_size} bytes) in {time.time() - hour_start} seconds ({(file_size / (1024 * 1024)) / (time.time() - hour_start):.2f} MB/s)"
        )

        # Atomically swap into place
        os.replace(tmp_path, filepath)

        # fsync the directory entry to ensure data durability
        dir_fd = os.open(os.path.dirname(filepath), os.O_DIRECTORY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    # Copy the tmp dir to the gharchive volume
    shutil.copytree(
        tmp_dir, os.path.join(GHARCHIVE_DATA_PATH, f"{year}/{month:02d}/{day:02d}")
    )

    # Commit the volumes
    gharchive.commit()

    # Delete the tmp dir
    shutil.rmtree(tmp_dir)

    return time.time() - start, file_size


@app.function(
    volumes={DOWNLOADED_DATA_PATH: downloaded},
    cloud="aws",
    region="us-east-1",
    timeout=36000,
    retries=3,
    # min_containers=500,
    ephemeral_disk=600 * 1024,
)
# @modal.concurrent(max_inputs=1)
def download_file(year: int, month: int, day: int, hour: int) -> tuple[str, int, int]:
    import os
    import time
    import tempfile
    import pycurl

    start = time.time()

    url = f"https://data.gharchive.org/{year}-{month:02d}-{day:02d}-{hour}.json.gz"
    filepath = os.path.join(
        DOWNLOADED_DATA_PATH, f"{year}-{month:02d}-{day:02d}-{hour:02d}.json.gz"
    )

    # Create temp file in same dir (same filesystem = atomic rename possible)
    tmp_fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(filepath) or ".")
    os.close(tmp_fd)

    last_progress_log = time.time()

    def progress(download_t, download_d, upload_t, upload_d):
        nonlocal last_progress_log
        if (time.time() - last_progress_log) > 2:
            last_progress_log = time.time()
            progress = download_d / download_t if download_t > 0 else 0.0
            print(f"{filepath} download progress: {progress:.2%}")

    with open(tmp_path, "wb") as f:
        c = pycurl.Curl()
        c.setopt(c.URL, url)
        c.setopt(c.FOLLOWLOCATION, 1)
        c.setopt(c.WRITEDATA, f)
        c.setopt(c.NOPROGRESS, False)
        c.setopt(c.XFERINFOFUNCTION, progress)
        c.perform()
        c.close()

    # Ensure data durability
    with open(tmp_path, "rb", buffering=0) as f:
        os.fsync(f.fileno())
        file_size = os.fstat(f.fileno()).st_size
    # Atomically swap into place
    os.replace(tmp_path, filepath)

    # Optionally fsync the directory entry
    dir_fd = os.open(os.path.dirname(filepath) or ".", os.O_DIRECTORY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)

    return filepath, time.time() - start, file_size


@app.function(
    timeout=36000,
)
def download_range(start: date, end: date):
    from datetime import timedelta
    import time

    inputs = []
    delta = end - start

    for d in range(delta.days + 1):
        day = start + timedelta(days=d)
        for hour in range(24):
            inputs.append((day.year, day.month, day.day, hour))

    print(
        f"Downloading events from {start} to {end}, {len(inputs)} files for {delta.days} days"
    )

    start = time.time()
    total_size = 0
    for result in download_file.starmap(
        inputs,
        return_exceptions=True,
        wrap_returned_exceptions=False,
        order_outputs=False,
    ):
        if isinstance(result, Exception):
            print(f"Error downloading {result}")
        else:
            filepath, duration, file_size = result
            total_size += file_size

    print(
        f"Downloaded {len(inputs)} files in {time.time() - start} seconds, {total_size / 1024 / 1024 / 1024:.2f} GB"
    )


@app.function(
    volumes={DOWNLOADED_DATA_PATH: downloaded},
    timeout=36000,
)
def download_day(year: int, month: int, day: int) -> list[any]:
    """Download all JSON.gz files for a specific day using pypdl with concurrent downloads and retries. Returns a tuple of the input and a list of failed downloads."""
    import os
    import time
    from pypdl import Pypdl

    # Create year directory if it doesn't exist
    year_dir = os.path.join(DOWNLOADED_DATA_PATH, str(year))
    os.makedirs(year_dir, exist_ok=True)

    # Generate all 24 hourly URLs for the day
    tasks = []
    for hour in range(24):
        url = f"https://data.gharchive.org/{year}-{month:02d}-{day:02d}-{hour}.json.gz"
        filename = f"{year}-{month:02d}-{day:02d}-{hour:02d}.json.gz"
        filepath = os.path.join(year_dir, filename)

        # Skip if file already exists
        if os.path.exists(filepath):
            continue

        tasks.append(
            {
                "url": url,
                "file_path": filepath,
                "retries": 3,
                "overwrite": True,
            }
        )

    if not tasks:
        print(f"All files for {year}-{month:02d}-{day:02d} already exist")
        return (year, month, day), []

    print(f"Downloading {len(tasks)} files for {year}-{month:02d}-{day:02d}...")
    dl = Pypdl(allow_reuse=False, max_concurrent=8)

    start = time.time()
    dl.start(
        tasks=tasks,
        block=True,
        display=False,
    )

    print(
        f"Downloaded {len(dl.success)} files for {year}-{month:02d}-{day:02d} in {time.time() - start} seconds"
    )

    if len(dl.failed) > 0:
        print(
            f"Failed to download {len(dl.failed)} files for {year}-{month:02d}-{day:02d}"
        )

    downloaded.commit()

    return (year, month, day), dl.failed


def download_year(year: int):
    """Download all JSON.gz files for a given year using the download_day function"""
    import os
    import calendar
    import time
    from datetime import datetime, timezone, date

    start = time.time()

    # Create all combinations of month and day for the given year, skipping future dates (UTC)
    day_inputs = []
    today_utc = datetime.now(timezone.utc).date()
    for month in range(1, 13):
        for day in range(1, calendar.monthrange(year, month)[1] + 1):
            d = date(year, month, day)
            if d > today_utc:
                continue
            day_inputs.append((year, month, day))

    print(f"Starting download of {len(day_inputs)} days for {year}...")

    failed_tasks = []
    for result in download_day.starmap(
        day_inputs,
        return_exceptions=True,
        wrap_returned_exceptions=False,
    ):
        input, failed = result
        if failed:
            failed_tasks.append(failed)
        print(
            f"Downloaded {input[0]}-{input[1]}-{input[2]} with {len(failed)} failures"
        )

    print(f"Downloaded {day_inputs} days for {year} in {time.time() - start} seconds")

    for failed in failed_tasks:
        print(failed)


@app.function(
    volumes={DOWNLOADED_DATA_PATH: downloaded, PARQUET_DATA_PATH: parquet},
    timeout=36000,
)
def process_day(year: int, month: int, day: int):
    """Process a single day's JSON.gz files and convert to Parquet using DuckDB"""
    import os
    import duckdb

    year_dir = os.path.join(DOWNLOADED_DATA_PATH, str(year))
    if not os.path.exists(year_dir):
        print(f"Year directory {year_dir} does not exist")
        return False

    # Create parquet output directory
    parquet_dir = os.path.join(PARQUET_DATA_PATH, str(year))
    os.makedirs(parquet_dir, exist_ok=True)

    day_pattern = os.path.join(year_dir, f"{year}-{month:02d}-{day:02d}-*.json.gz")

    try:
        # Create day-specific parquet file
        day_parquet = os.path.join(parquet_dir, f"{year}-{month:02d}-{day:02d}.parquet")

        # Create DuckDB connection
        con = duckdb.connect()

        # Use DuckDB to read all JSON files for the day using glob pattern
        query = f"""
        COPY (
            SELECT * FROM read_json_auto('{day_pattern}', format='newline_delimited', compression='gzip', ignore_errors=true, union_by_name=true, maximum_object_size=250000000)
        ) TO '{day_parquet}' (FORMAT PARQUET, COMPRESSION 'uncompressed');
        """

        con.sql(query)
        con.close()

        parquet.commit()

        # Verify the output file
        if os.path.exists(day_parquet) and os.path.getsize(day_parquet) > 0:
            print(f"Created {day_parquet} ({os.path.getsize(day_parquet)} bytes)")
            return True
        else:
            print(f"Warning: {day_parquet} is empty or missing")
            return False

    except Exception as e:
        print(f"Error processing {year}-{month:02d}-{day:02d}: {e}")
        return False


@app.function(
    volumes={DOWNLOADED_DATA_PATH: downloaded, PARQUET_DATA_PATH: parquet},
    timeout=36000,
)
def process_year(year: int):
    """Process all downloaded JSON.gz files for a year and convert to Parquet using DuckDB"""
    import os
    import calendar

    year_dir = os.path.join(DOWNLOADED_DATA_PATH, str(year))
    if not os.path.exists(year_dir):
        print(f"Year directory {year_dir} does not exist")
        return False

    # Create parquet output directory
    parquet_dir = os.path.join(PARQUET_DATA_PATH, str(year))
    os.makedirs(parquet_dir, exist_ok=True)

    print(f"Starting parallel processing of year {year} by day...")

    # Create inputs for all days in the year
    day_inputs = []
    for month in range(1, 13):
        for day in range(1, calendar.monthrange(year, month)[1] + 1):
            day_inputs.append((year, month, day))

    print(f"Processing {len(day_inputs)} days in parallel...")

    # Process all days in parallel
    successful_days = 0
    failed_days = 0
    for result in process_day.starmap(day_inputs):
        if result:
            successful_days += 1
        else:
            failed_days += 1

    print(
        f"Completed processing year {year}: {successful_days} successful days, {failed_days} failed days"
    )

    parquet.commit()

    return successful_days > 0


@app.local_entrypoint()
def main():
    # download_range.remote(date(2020, 1, 1), date(2020, 1, 10))
    download_and_copy_day.remote(2020, 1, 1)
