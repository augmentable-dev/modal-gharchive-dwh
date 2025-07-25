import modal
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
    con.sql(sql).show()
    con.close()


@app.local_entrypoint()
def main():
    # query.remote(
    #     "EXPLAIN ANALYZE SELECT count(*) as count, type FROM '/data/parquet/2015/*.parquet' where actor->>'login' = 'patrickdevivo' group by type"
    # )

    # query.remote("DESCRIBE FROM '/data/parquet/**/*.parquet'")
    start = time.time()
    query.remote(
        "SELECT count(*) FROM '/data/parquet/2020/*.parquet'"
    )
    end = time.time()
    print(f"Time taken: {end - start} seconds")
