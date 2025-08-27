import modal
from datetime import date
from .app import app, gharchive, GHARCHIVE_DATA_PATH


@app.function(
    volumes={GHARCHIVE_DATA_PATH: gharchive},
    timeout=36000,
    retries=modal.Retries(
        max_retries=8,
        backoff_coefficient=2,
        initial_delay=1,
        max_delay=30,
    ),
    ephemeral_disk=800 * 1024,
)
@modal.concurrent(max_inputs=12)
def download_file(year: int, month: int, day: int, hour: int) -> tuple[str, float, int]:
    import os, time, tempfile, shutil, pycurl, random, io

    # Tiny jitter to avoid synchronized bursts (helps with WAF/rate shaping)
    time.sleep(random.uniform(0.01, 0.05))

    # URLs & paths
    url = f"https://data.gharchive.org/{year}-{month:02d}-{day:02d}-{hour}.json.gz"
    vol_path = f"{GHARCHIVE_DATA_PATH}/{year}/{month:02d}/{day:02d}/{hour}.json.gz"

    # Stage to /tmp for speed
    tmp_dir = f"/tmp/{year}/{month:02d}/{day:02d}"
    os.makedirs(tmp_dir, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        dir=tmp_dir, prefix=f"{year}-{month:02d}-{day:02d}-{hour}.json.gz."
    )
    os.close(fd)

    # Configure curl (inline)
    c = pycurl.Curl()
    c.setopt(c.URL, url)
    c.setopt(c.FOLLOWLOCATION, 1)
    c.setopt(c.HTTP_VERSION, pycurl.CURL_HTTP_VERSION_2TLS)  # allow HTTP/2
    c.setopt(
        c.USERAGENT, "gharchive-downloader/1.0 (contact: patrick.devivo@gmail.com)"
    )  # may help with rate limiting
    c.setopt(c.NOSIGNAL, 1)
    c.setopt(c.CONNECTTIMEOUT, 10)  # seconds
    c.setopt(
        c.TIMEOUT, 60 * 5
    )  # total timeout - most downloads should occur within this
    c.setopt(c.LOW_SPEED_LIMIT, 20_000)  # bytes/sec
    c.setopt(c.LOW_SPEED_TIME, 20)  # if below limit for 20s -> timeout

    hdr_buf = io.BytesIO()
    c.setopt(c.HEADERFUNCTION, hdr_buf.write)

    # Download → /tmp, fsync for durability
    with open(tmp_path, "wb", buffering=1024 * 1024) as f:
        c.setopt(c.WRITEDATA, f)
        c.perform()
        f.flush()
        os.fsync(f.fileno())

    # Telemetry from curl
    status = int(c.getinfo(pycurl.RESPONSE_CODE))
    size_b = int(c.getinfo(pycurl.SIZE_DOWNLOAD))
    total_s = float(c.getinfo(pycurl.TOTAL_TIME))
    mbps = float(c.getinfo(pycurl.SPEED_DOWNLOAD)) / (1024 * 1024)
    ttfb_s = float(c.getinfo(pycurl.STARTTRANSFER_TIME))
    headers_s = hdr_buf.getvalue().decode("latin1", errors="replace")
    c.close()

    # Handle non-200s
    if status != 200:
        # Clean partial temp file
        try:
            os.remove(tmp_path)
        except FileNotFoundError:
            pass

        if status == 404:
            time.sleep(random.uniform(0.5, 2.0))
            raise RuntimeError(f"HTTP 404 for {url} (retryable)")

        # Transient / retry-worthy codes → raise so Modal retries
        retry_statuses = {403, 429, 500, 502, 503, 504}
        if status in retry_statuses:
            first_hdr = headers_s.splitlines()[0] if headers_s else f"HTTP {status}"
            raise RuntimeError(f"HTTP {status} for {url} ({first_hdr})")

        # Other client errors: surface as failures (Modal may still retry per policy)
        raise RuntimeError(f"HTTP {status} for {url}")

    # Atomically publish into the volume (inline, no helpers)
    vol_dir = os.path.dirname(vol_path)
    os.makedirs(vol_dir, exist_ok=True)
    tmp_dest = vol_path + ".part"
    with (
        open(tmp_path, "rb", buffering=1024 * 1024) as rf,
        open(tmp_dest, "wb", buffering=1024 * 1024) as wf,
    ):
        shutil.copyfileobj(rf, wf, length=8 * 1024 * 1024)
        wf.flush()
        os.fsync(wf.fileno())
    os.replace(tmp_dest, vol_path)  # atomic within the same FS
    dfd = os.open(vol_dir, os.O_DIRECTORY)
    try:
        os.fsync(dfd)  # fsync directory entry (optional but nice)
    finally:
        os.close(dfd)

    # Clean local temp
    try:
        os.remove(tmp_path)
    except FileNotFoundError:
        pass

    print(
        f"{os.path.basename(vol_path)} — {size_b / 1_048_576:.1f} MB "
        f"in {total_s:.2f}s @ {mbps:.2f} MB/s (TTFB {ttfb_s:.2f}s) → {vol_path}"
    )

    return vol_path, total_s, size_b


@app.function(timeout=36000, volumes={GHARCHIVE_DATA_PATH: gharchive})
def download_range(start: date, end: date = date.today()):
    from datetime import timedelta
    import time

    # Build hour-level inputs
    inputs = []
    delta = end - start
    for d in range(delta.days + 1):
        day = start + timedelta(days=d)
        for hour in range(24):
            inputs.append((day.year, day.month, day.day, hour))

    print(
        f"Downloading events from {start} to {end} — {len(inputs)} files over {delta.days + 1} days"
    )

    t0 = time.time()
    total_size = 0
    ok = 0
    failures = []

    for result in download_file.starmap(
        inputs,
        return_exceptions=True,
        wrap_returned_exceptions=False,
        order_outputs=False,
    ):
        if isinstance(result, Exception):
            failures.append(result)
        else:
            _, _, sz = result
            total_size += sz
            ok += 1

    elapsed = time.time() - t0
    total_gb = total_size / (1024**3)
    agg_gbps = (total_gb / elapsed) if elapsed > 0 else 0.0
    print(
        f"Done: {ok}/{len(inputs)} files in {elapsed:.1f}s — {total_gb:.2f} GB total, avg {agg_gbps:.2f} GB/s"
    )

    if failures:
        print(f"Encountered {len(failures)} failures (Modal handled retries): ")
        for f in failures:
            print(f"[FAIL] {f!s}")

    gharchive.commit()


@app.function(timeout=3600, volumes={GHARCHIVE_DATA_PATH: gharchive})
@modal.concurrent(max_inputs=32)
def count_events_in_file(path: str):
    import gzip
    import io

    # Big buffered reader for speed
    with gzip.open(path, "rb") as gz:
        buf = io.BufferedReader(gz, buffer_size=8 * 1024 * 1024)
        return sum(1 for _ in buf)  # 1 line == 1 event


@app.function(timeout=3600, volumes={GHARCHIVE_DATA_PATH: gharchive})
def count_all_events():
    import os
    from glob import iglob

    files = [f for f in iglob(f"{GHARCHIVE_DATA_PATH}/**/*.json.gz", recursive=True)
             if os.path.isfile(f)]

    total = 0
    for n in count_events_in_file.map(files):
        total += n
        print(f"counted {total:_} events so far")
    print(f"total events: {total:_}")


@app.local_entrypoint()
def main():
    # download_range.remote(date(2020, 1, 1))
    count_all_events.remote()
