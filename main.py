import modal
import calendar

image = modal.Image.debian_slim().pip_install("duckdb")

app = modal.App("gharchive", image=image)
vol = modal.Volume.from_name("gharchive-files", create_if_missing=True)
duckdbVol = modal.Volume.from_name("gharchive-duckdb", create_if_missing=True)


@app.cls(
    volumes={"/data/downloaded": vol, "/data/duckdb": duckdbVol},
    timeout=36000,
    cpu=(64),
    memory=(344064),
)
@modal.concurrent(max_inputs=100)
class GHArchive:
    @modal.enter()
    def init(self):
        import duckdb

        con = duckdb.connect("/data/duckdb/db.duckdb")

        ct = """
        CREATE TABLE IF NOT EXISTS gharchive (
            id          VARCHAR,
            type        VARCHAR,
            actor       JSON,
            repo        JSON,
            payload     JSON,
            public      BOOLEAN,
            created_at  TIMESTAMPTZ,
            org         JSON
        );
        """
        con.sql(ct)
        self.duckdb = con

    @modal.exit()
    def exit(self):
        self.duckdb.close()

    @modal.method()
    def show(self, sql: str):
        self.duckdb.sql(sql).show()
        return

    @modal.method()
    def sql(self, sql: str):
        self.duckdb.sql(sql)
        return

    @modal.method()
    def download_file(self, year: int, month: int, day: int, hour: int):
        import os

        url = f"https://data.gharchive.org/{year}-{month:02d}-{day:02d}-{hour:02d}.json.gz"
        filename_parquet = f"{year}-{month:02d}-{day:02d}-{hour:02d}.parquet"
        filepath_parquet = os.path.join("/data/downloaded", str(year), filename_parquet)

        # Create year directory if it doesn't exist
        year_dir = os.path.join("/data/downloaded", str(year))
        os.makedirs(year_dir, exist_ok=True)

        # Skip if Parquet file already exists
        if os.path.exists(filepath_parquet):
            return True

        try:
            # Create a new DuckDB instance for this download to enable parallelization
            import duckdb

            con = duckdb.connect()

            # Use DuckDB to directly read compressed JSON from remote URL and write as Parquet
            con.sql(
                f"COPY (SELECT * FROM read_json_auto('{url}', format='newline_delimited', compression='gzip')) TO '{filepath_parquet}' (FORMAT PARQUET);"
            )

            # Close the connection
            con.close()

        except Exception as e:
            print(f"Error processing {year}-{month:02d}-{day:02d}-{hour:02d}: {e}")
            return False

        print(
            f"Downloaded and converted to Parquet: {year}-{month:02d}-{day:02d}-{hour:02d}"
        )
        return True

    @modal.method()
    def download_year(self, year: int):
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

        for result in self.download_file.starmap(inputs):
            if result:
                successful_downloads += 1
            else:
                failed_downloads += 1

        print(
            f"Download complete! {successful_downloads} successful, {failed_downloads} failed"
        )

    @modal.method()
    def import_year(self, year: int):
        import glob
        import calendar

        # Import files day by day
        for month in range(1, 13):
            for day in range(1, calendar.monthrange(year, month)[1] + 1):
                # Get all JSON files for the current day
                day_pattern = (
                    f"/data/downloaded/{year}/{year}-{month:02d}-{day:02d}*.json"
                )
                json_files = glob.glob(day_pattern)

                print(
                    f"Found {len(json_files)} files to import for {year}-{month:02d}-{day:02d}"
                )

                if len(json_files) > 0:
                    # Import all files for this day in one SQL statement
                    # Use array syntax for multiple files
                    files_array = "[" + ", ".join([f"'{f}'" for f in json_files]) + "]"
                    print(f"Importing files for {year}-{month:02d}-{day:02d}...")
                    self.duckdb.sql(
                        f"INSERT INTO gharchive SELECT * FROM read_json_auto({files_array}, format='newline_delimited');"
                    )
                    print(f"Completed import of {year}-{month:02d}-{day:02d}")
                else:
                    print(f"No files found for {year}-{month:02d}-{day:02d}")


@app.local_entrypoint()
def main():
    # GHArchive().download_year.remote(2015)
    # GHArchive().import_year.remote(2016)
    # GHArchive().show.remote("select * from '/data/downloaded/2016/*.json' limit 10")
    # GHArchive().show.remote("select * from gharchive where actor->>'login' = 'patrickdevivo' limit 10")
    # GHArchive().show.remote("insert into gharchive select * from read_json_auto('/data/downloaded/2016/2016-01*.json', format='newline_delimited')")
    GHArchive().show.remote(
        "select * from '/data/downloaded/2015/*.parquet' where actor->>'login' = 'patrickdevivo' limit 10"
    )
