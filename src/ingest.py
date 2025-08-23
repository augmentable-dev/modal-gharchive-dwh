import modal
from .app import app, downloaded, DOWNLOADED_DATA_PATH, parquet, PARQUET_DATA_PATH


@app.function(
    volumes={DOWNLOADED_DATA_PATH: downloaded},
    timeout=36000,
)
def download_day(
    year: int, month: int, day: int
) -> tuple[tuple[int, int, int], list[any]]:
    """Download all JSON.gz files for a specific day using pypdl with concurrent downloads and retries. Returns a tuple of the input and a list of failed downloads."""
    import os
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

    dl = Pypdl(allow_reuse=False, max_concurrent=48)

    dl.start(
        tasks=tasks,
        block=True,
        display=False,
        retries=3,
    )

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
        day_inputs, return_exceptions=True, wrap_returned_exceptions=False
    ):
        input, failed = result
        if failed:
            failed_tasks.append(failed)
        print(f"Downloaded {input[0]}-{input[1]}-{input[2]} with {len(failed)} failures")

    print(
        f"Downloaded {day_inputs} days for {year} in {time.time() - start} seconds"
    )
    print(f"Failed tasks: {failed_tasks}")


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
    download_year(2024)

    # Process the downloaded files into Parquet format
    # process_year.remote(2024)

    # Uncomment to download other years
    # download_year.remote(2023)
    # process_year.remote(2023)

    # download_year.remote(2022)
    # process_year.remote(2022)
