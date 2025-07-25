import calendar

from .app import app, downloaded, DOWNLOADED_DATA_PATH, parquet, PARQUET_DATA_PATH

@app.function(
    volumes={DOWNLOADED_DATA_PATH: downloaded},
)
def download_file(year: int, month: int, day: int, hour: int):
    import os
    import requests

    url = f"https://data.gharchive.org/{year}-{month:02d}-{day:02d}-{hour}.json.gz"
    filename_json = f"{year}-{month:02d}-{day:02d}-{hour:02d}.json.gz"

    filepath_json = os.path.join(DOWNLOADED_DATA_PATH, str(year), filename_json)

    # Create year directory if it doesn't exist
    year_dir = os.path.join(DOWNLOADED_DATA_PATH, str(year))
    os.makedirs(year_dir, exist_ok=True)

    # Skip if JSON.gz file already exists
    if os.path.exists(filepath_json):
        return True

    try:
        # Download the JSON.gz file
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(filepath_json, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        # Check if downloaded file has content
        if os.path.getsize(filepath_json) == 0:
            print(f"Warning: Downloaded file {filename_json} is empty, skipping...")
            os.remove(filepath_json)
            return False

        print(f"Downloaded {filename_json} ({os.path.getsize(filepath_json)} bytes)")
        return True

    except Exception as e:
        print(f"Error downloading {year}-{month:02d}-{day:02d}-{hour:02d}: {e}")
        # Clean up the JSON file if it exists and there was an error
        if os.path.exists(filepath_json):
            try:
                os.remove(filepath_json)
                print(f"Cleaned up temporary file {filename_json} after error")
            except:
                pass
        return False


@app.function(
    volumes={DOWNLOADED_DATA_PATH: downloaded},
    timeout=36000,
)
def download_year(year: int):
    """Download all JSON.gz files for a given year"""
    # Create all combinations of month, day, hour for the given year
    inputs = []
    for month in range(1, 13):
        for day in range(1, calendar.monthrange(year, month)[1] + 1):
            for hour in range(24):
                inputs.append((year, month, day, hour))

    # Fan out all the download tasks in parallel using Modal's starmap
    print(f"Starting download of {len(inputs)} files for {year}...")
    successful_downloads = 0
    failed_downloads = 0

    for result in download_file.starmap(inputs):
        if result:
            successful_downloads += 1
        else:
            failed_downloads += 1

    print(
        f"Download complete! {successful_downloads} successful, {failed_downloads} failed"
    )


@app.function(
    volumes={DOWNLOADED_DATA_PATH: downloaded, PARQUET_DATA_PATH: parquet},
    timeout=36000,
)
def process_day(year: int, month: int, day: int):
    """Process a single day's JSON.gz files and convert to Parquet using DuckDB"""
    import os
    import glob
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
        # query = f"""
        # COPY (
        #     SELECT
        #         created_at::timestamp as created_at,
        #         extract(year from created_at) as year,
        #         extract(month from created_at) as month,
        #         extract(day from created_at) as day,
        #         payload, public, type, url, actor, actor_attributes, repository FROM read_json_auto('{day_pattern}', format='newline_delimited', compression='gzip', ignore_errors=true, union_by_name=true)
        # ) TO '{day_parquet}' (FORMAT PARQUET, COMPRESSION 'uncompressed', PARTITION_BY (year, month, day, type), APPEND);
        # """

        query = f"""
        COPY (
            SELECT * FROM read_json_auto('{day_pattern}', format='newline_delimited', compression='gzip', ignore_errors=true, union_by_name=true)
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
    # First download all the JSON.gz files for the year
    # download_year.remote(2018)

    # Then process all the downloaded files into monthly Parquet files
    # process_year.remote(2014)
    # process_year.remote(2015)
    # process_year.remote(2016)
    # process_year.remote(2017)
    # process_year.remote(2018)
    # process_year.remote(2019)
    # process_year.remote(2020)

    # process_year.remote(2018)
    process_day.remote(2020, 11, 28)
