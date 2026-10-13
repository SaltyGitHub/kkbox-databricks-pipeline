# Databricks notebook source
# MAGIC %pip install kagglehub

# COMMAND ----------

import os

# Store your Kaggle token in a Databricks secret scope named kkbox.
os.environ["KAGGLE_API_TOKEN"] = dbutils.secrets.get(
    scope="kkbox", key="kaggle-api-token"
)

# COMMAND ----------

import kagglehub
kagglehub.login()

# COMMAND ----------

# Download latest version
path = kagglehub.competition_download('kkbox-churn-prediction-challenge')

print("Path to competition files:", path)

# COMMAND ----------

from pathlib import Path
import shutil

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.kkbox")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.kkbox.raw")

source = Path(path)
destination = Path("/Volumes/workspace/kkbox/raw")

for file in source.rglob("*"):
    if file.is_file():
        target = destination / file.relative_to(source)
        target.parent.mkdir(parents=True, exist_ok=True)

        if not target.exists() or target.stat().st_size != file.stat().st_size:
            print(f"Copying {file.name}...")
            shutil.copyfile(file, target)

display(dbutils.fs.ls(str(destination)))

# COMMAND ----------

from pathlib import Path

source = Path(path)
print("Download folder exists:", source.exists())

if source.exists():
    for file in source.iterdir():
        print(file.name)

display(dbutils.fs.ls("/Volumes/workspace/kkbox/raw"))

# COMMAND ----------

from pathlib import Path
import shutil

source = Path(path)
destination = Path("/Volumes/workspace/kkbox/raw")

files = sorted(
    (f for f in source.iterdir() if f.is_file()),
    key=lambda f: f.stat().st_size
)

for file in files:
    target = destination / file.name
    size = file.stat().st_size

    if target.exists() and target.stat().st_size == size:
        print(f"Already copied: {file.name}", flush=True)
        continue

    print(f"Copying {file.name} ({size / 1e9:.2f} GB)…", flush=True)

    copied = 0
    next_report = 512 * 1024**2

    with file.open("rb") as src, target.open("wb") as dst:
        while chunk := src.read(8 * 1024**2):
            dst.write(chunk)
            copied += len(chunk)

            if copied >= next_report:
                print(f"  {copied / size:.0%}", flush=True)
                next_report += 512 * 1024**2

    print(f"Finished: {file.name}", flush=True)

display(dbutils.fs.ls(str(destination)))

# COMMAND ----------

# MAGIC %pip install py7zr

# COMMAND ----------

from pathlib import Path
import py7zr
import shutil
import uuid

raw = Path("/Volumes/workspace/kkbox/raw")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.kkbox")

archives = sorted(raw.glob("*.csv.7z"), key=lambda f: f.stat().st_size)
if not archives:
    raise RuntimeError("No .csv.7z archives found in the raw volume.")

for archive in archives:
    name = archive.name.removesuffix(".csv.7z")
    table = f"workspace.kkbox_raw.{name}"

    if spark.catalog.tableExists(table):
        print(f"Skipping existing table: {table}", flush=True)
        continue

    # Unique extraction folder; Spark can read it from the volume.
    staging = raw / "_extract" / uuid.uuid4().hex
    staging.mkdir(parents=True, exist_ok=True)

    print(f"Extracting {archive.name}…", flush=True)
    with py7zr.SevenZipFile(archive, mode="r") as bundle:
        print(
            f"Uncompressed size: {bundle.archiveinfo().uncompressed / 1e9:.2f} GB",
            flush=True
        )
        bundle.extractall(path=staging)

    csv_files = list(staging.rglob("*.csv"))
    if len(csv_files) != 1:
        raise RuntimeError(
            f"Expected one CSV in {archive.name}; found {len(csv_files)}. "
            f"Inspect {staging}"
        )

    print(f"Creating {table}…", flush=True)
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "FAILFAST")
        .csv(str(csv_files[0]))
    )

    df.write.format("delta").mode("error").saveAsTable(table)

    # Remove only this temporary extraction after a successful write.
    shutil.rmtree(staging)
    print(f"Created: {table}", flush=True)

display(spark.sql("SHOW TABLES IN workspace.kkbox"))

# COMMAND ----------

from pathlib import Path
import shutil
import uuid
import py7zr

TARGET = "workspace.kkbox_raw.user_logs"
ARCHIVE_NAME = "user_logs.csv.7z"
VOLUME_PATH = Path("/Volumes/workspace/kkbox_raw/ingest_files")

# Capture paths left by the previous notebook cells before changing variables.
previous_csvs = [
    Path(p) for p in globals().get("csv_files", [])
]
download_path = globals().get("path")


def copy_with_progress(source, destination):
    size = source.stat().st_size
    copied = 0
    next_report = 1024**3

    destination.parent.mkdir(parents=True, exist_ok=True)
    print(f"Copying {source.name}: {size / 1e9:.2f} GB", flush=True)

    with source.open("rb") as src, destination.open("wb") as dst:
        while chunk := src.read(8 * 1024**2):
            dst.write(chunk)
            copied += len(chunk)

            if copied >= next_report:
                print(f"  {copied / size:.0%}", flush=True)
                next_report += 1024**3

    if destination.stat().st_size != size:
        raise RuntimeError(f"Copy size mismatch: {destination}")

    print("Copy finished.", flush=True)


def ingest_user_logs():
    if spark.catalog.tableExists(TARGET):
        print(f"{TARGET} already exists; leaving it unchanged.", flush=True)
        return

    spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.kkbox_raw")
    spark.sql("""
        CREATE VOLUME IF NOT EXISTS workspace.kkbox_raw.ingest_files
    """)

    # Verify that Databricks resolves the destination as a real volume.
    dbutils.fs.ls(str(VOLUME_PATH))

    # Look in known locations first.
    candidates = [
        VOLUME_PATH / ARCHIVE_NAME,
        Path("/Volumes/workspace/kkbox/raw") / ARCHIVE_NAME,
    ]

    if isinstance(download_path, (str, Path)):
        candidates.append(Path(download_path) / ARCHIVE_NAME)

    # Also look at the root of other accessible workspace volumes.
    volumes = spark.sql("""
        SELECT volume_catalog, volume_schema, volume_name
        FROM workspace.information_schema.volumes
    """).collect()

    for volume in volumes:
        candidates.append(
            Path("/Volumes")
            / volume.volume_catalog
            / volume.volume_schema
            / volume.volume_name
            / ARCHIVE_NAME
        )

    archive_source = None
    for candidate in dict.fromkeys(candidates):
        try:
            if candidate.is_file():
                archive_source = candidate
                break
        except OSError:
            continue

    if archive_source is None:
        raise FileNotFoundError(
            f"Could not find {ARCHIVE_NAME}. Restore the downloaded archive "
            f"to {VOLUME_PATH}, then rerun this cell."
        )

    # Read archive metadata to identify the expected CSV and its full size.
    with py7zr.SevenZipFile(archive_source, mode="r") as bundle:
        entries = [
            entry for entry in bundle.list()
            if not entry.is_directory
            and entry.filename.lower().endswith(".csv")
        ]

    if len(entries) != 1:
        raise RuntimeError(
            f"Expected one CSV in {archive_source}; found {len(entries)}."
        )

    expected_size = entries[0].uncompressed
    print(f"Expected CSV size: {expected_size / 1e9:.2f} GB", flush=True)

    # Keep extraction in a registered volume so Spark can read it.
    staging = VOLUME_PATH / "_extract" / uuid.uuid4().hex
    staging.mkdir(parents=True, exist_ok=True)

    recovered_csv = None
    for candidate in previous_csvs:
        try:
            if (
                candidate.name == Path(entries[0].filename).name
                and candidate.is_file()
                and candidate.stat().st_size == expected_size
            ):
                recovered_csv = candidate
                break
        except OSError:
            continue

    if recovered_csv is not None:
        print("Reusing the CSV from the previous extraction.", flush=True)
        csv_path = staging / recovered_csv.name
        copy_with_progress(recovered_csv, csv_path)
    else:
        # Preserve the archive in the new registered volume.
        archive_path = VOLUME_PATH / ARCHIVE_NAME
        if archive_source != archive_path:
            copy_with_progress(archive_source, archive_path)

        print("Extracting user_logs; this step may take a while…", flush=True)
        with py7zr.SevenZipFile(archive_path, mode="r") as bundle:
            bundle.extractall(path=staging)

        extracted = list(staging.rglob("*.csv"))
        if len(extracted) != 1:
            raise RuntimeError(f"Inspect unexpected extraction at {staging}")

        csv_path = extracted[0]

    if csv_path.stat().st_size != expected_size:
        raise RuntimeError(f"Extracted CSV has the wrong size: {csv_path}")

    # Verify that Spark's filesystem can see the CSV before loading it.
    display(dbutils.fs.ls(str(csv_path.parent)))

    print(f"Creating {TARGET}…", flush=True)

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "FAILFAST")
        .csv(str(csv_path))
    )

    expected_columns = [
        "msno", "date", "num_25", "num_50", "num_75",
        "num_985", "num_100", "num_unq", "total_secs"
    ]

    if df.columns != expected_columns:
        raise RuntimeError(f"Unexpected CSV columns: {df.columns}")

    # Atomic Delta write; existing tables are not overwritten.
    df.write.format("delta").mode("error").saveAsTable(TARGET)

    # Only remove this run's temporary CSV after the write succeeds.
    shutil.rmtree(staging)
    print(f"Created {TARGET}", flush=True)


ingest_user_logs()

display(spark.sql("SHOW TABLES IN workspace.kkbox_raw"))
