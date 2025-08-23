import time

from .app import app, parquet, PARQUET_DATA_PATH


@app.function(
    volumes={PARQUET_DATA_PATH: parquet},
    timeout=(3600),
)
def query(sql: str):
    import duckdb

    parquet.reload()

    con = duckdb.connect()

    con.sql("""
        CREATE OR REPLACE VIEW events AS
        SELECT
            *,
            filename AS _file,
            CAST(regexp_extract(filename, '(\d{4})', 1) AS INT)             AS year,
            CAST(regexp_extract(filename, '\d{4}-(\d{2})', 1) AS INT)       AS month,
            CAST(regexp_extract(filename, '\d{4}-\d{2}-(\d{2})', 1) AS INT) AS day
        FROM read_parquet(
            '/data/parquet/2014/*.parquet',
            filename = 1,
            union_by_name = 1
        );
    """)
    con.sql(sql).show()
    con.close()


@app.local_entrypoint()
def main():
    start = time.time()
    query.remote("SELECT * FROM events LIMIT 10")
    end = time.time()
    print(f"Time taken: {end - start} seconds")
