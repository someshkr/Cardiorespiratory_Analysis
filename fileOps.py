from glob import glob
from concurrent.futures import ProcessPoolExecutor
from scipy.io import loadmat
from tqdm import tqdm

import multiprocessing as mp
import polars as pl
import numpy as np
import os


def read_header(hea_file):
    """
    Reads both standard and non-standard WFDB headers.

    Supports

    Format 1
    --------
    JS00002 12 500 5000

    Format 2
    --------
    JS01052 12 500 500000/mV 16 0 15 31255 0 I
    """

    with open(hea_file, "r") as f:
        lines = [line.strip() for line in f if line.strip()]

    first = lines[0].split()

    record_id = first[0]
    num_leads = int(first[1])
    fs = int(first[2])

    lead_names = []
    metadata = {}

    # -------------------------------------------------------
    # FORMAT 1
    # JS00002 12 500 5000
    # -------------------------------------------------------
    if "/" not in first[3]:

        num_samples = int(first[3])

        signal_lines = lines[1:1 + num_leads]

        for line in signal_lines:
            lead_names.append(line.split()[-1])

        meta_start = 1 + num_leads

    # -------------------------------------------------------
    # FORMAT 2
    # JS01052 12 500 500000/mV 16 ... I
    # -------------------------------------------------------
    else:

        # First lead already exists in first line
        lead_names.append(first[-1])

        signal_lines = lines[1:1 + (num_leads - 1)]

        for line in signal_lines:
            lead_names.append(line.split()[-1])

        meta_start = 1 + (num_leads - 1)

        # Determine samples from MAT file later
        num_samples = None

    # Read metadata
    for line in lines[meta_start:]:

        if line.startswith("#") and ":" in line:

            key, value = line[1:].split(":", 1)

            metadata[key.strip()] = value.strip()

    return {
        "record_id": record_id,
        "fs": fs,
        "num_leads": num_leads,
        "num_samples": num_samples,
        "lead_names": lead_names,
        "metadata": metadata,
    }

def process_record(hea_file):
    """
    Process a single ECG record.

    Supports both header formats:
    1. Standard WFDB
    2. Non-standard merged-header format
    """

    record_path = os.path.splitext(hea_file)[0]

    # Read header
    header = read_header(hea_file)

    # Read MAT file
    mat = loadmat(record_path + ".mat")

    # ECG signal
    # Shape: (num_samples, 12)
    signal = mat["val"].T.astype(np.float32)

    # If header doesn't contain sample count (Format 2),
    # infer it from the signal.
    if header["num_samples"] is None:
        num_samples = signal.shape[0]
    else:
        num_samples = header["num_samples"]

    row = {
        "record_id": header["record_id"],
        "record_path": record_path,
        "fs": header["fs"],
        "num_samples": num_samples,
        "num_leads": header["num_leads"],
        "duration_sec": round(num_samples / header["fs"], 2),

        # Metadata
        "Age": header["metadata"].get("Age"),
        "Sex": header["metadata"].get("Sex"),
        "Dx": header["metadata"].get("Dx"),
        "Rx": header["metadata"].get("Rx"),
        "Hx": header["metadata"].get("Hx"),
        "Sx": header["metadata"].get("Sx"),
    }

    # Store all ECG leads
    for i, lead in enumerate(header["lead_names"]):

        # Safety check
        if i < signal.shape[1]:
            row[lead] = signal[:, i].tolist()

    return row

def read_all_records(root_dir, output_dir, workers=None):

    os.makedirs(output_dir, exist_ok=True)

    hea_files = sorted(
        glob(
            os.path.join(root_dir, "**", "*.hea"),
            recursive=True,
        )
    )

    print(f"\nFound {len(hea_files):,} ECG records")

    if workers is None:
        # M4 recommendation
        workers = min(8, mp.cpu_count())

    print(f"Using {workers} workers\n")

    csv_file = os.path.join(output_dir, "ecg_records.csv")
    parquet_file = os.path.join(output_dir, "ecg_records.parquet")

    # Process in batches to avoid memory overflow
    batch_size = 1000

    with ProcessPoolExecutor(max_workers=workers) as executor:

        with tqdm(
            total=len(hea_files),
            desc="Processing ECG records",
            unit="patient",
            dynamic_ncols=True,
            bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
        ) as pbar:

            for i in range(0, len(hea_files), batch_size):

                batch = hea_files[i:i + batch_size]
                rows = []

                results = executor.map(
                    process_record,
                    batch,
                    chunksize=200,
                )

                for row in results:
                    rows.append(row)
                    pbar.update(1)

                # Write batch to disk
                print(f"\nWriting batch {i // batch_size + 1} to disk...")
                df_batch = pl.DataFrame(rows)

                if i == 0:
                    # First batch - create files
                    df_batch.write_parquet(parquet_file, compression="zstd")
                else:
                    # Append subsequent batches
                    existing = pl.read_parquet(parquet_file)
                    combined = pl.concat([existing, df_batch])
                    combined.write_parquet(parquet_file, compression="zstd")

                del rows, df_batch  # Free memory

    print("\nWriting CSV from Parquet...")
    print("\nDone!")
    print(f"Parquet  : {parquet_file}")

    return df


if __name__ == "__main__":

    ROOT_DIR = "/Users/tushitaa/Documents/DNN/data/WFDBRecords"

    OUTPUT_DIR = "/Users/tushitaa/Documents/DNN/code/output_dir"

    df = read_all_records(
        ROOT_DIR,
        OUTPUT_DIR,
    )

    print(df.head())