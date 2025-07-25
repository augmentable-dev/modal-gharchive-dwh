import modal

# Modal app and image configuration
image = modal.Image.debian_slim().pip_install("duckdb", "requests")
app = modal.App("gharchive", image=image)

# Volume configurations
downloaded = modal.Volume.from_name("gharchive-downloaded-json", create_if_missing=True)
parquet = modal.Volume.from_name("gharchive-parquet", create_if_missing=True)

# Data paths
DOWNLOADED_DATA_PATH = "/data/downloaded"
PARQUET_DATA_PATH = "/data/parquet"
