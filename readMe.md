# ECG Record Processing Pipeline

## Overview

`fileOps.py` is a high-performance Python script for batch processing ECG records stored in WFDB format. It reads header files (`.hea`) and signal data (`.mat`), extracts metadata and 12-lead ECG signals, and outputs a structured Parquet file for efficient downstream analysis.

## Features

- **Parallel Processing**: Uses multiprocessing with configurable worker count
- **Memory-Efficient**: Processes records in batches (1000 per batch) to prevent memory overflow
- **Dual Header Format Support**: Handles both standard WFDB and non-standard merged-header formats
- **Progress Tracking**: Real-time progress bar with ETA
- **Compressed Output**: Generates ZSTD-compressed Parquet files for optimal storage

## Requirements

```bash
pip install polars numpy scipy tqdm
```

## Usage

```
>>> import polars as pl
>>> df = pl.read_parquet('./DNN/code/output_dir/ecg_records.parquet')
>>> df.columns
['record_id', 'record_path', 'fs', 'num_samples', 'num_leads', 'duration_sec', 'Age', 'Sex', 'Dx', 'Rx', 'Hx', 'Sx', 'I', 'II', 'III', 'aVR', 'aVL', 'aVF', 'V1', 'V2', 'V3', 'V4', 'V5', 'V6']
>>> df.head(1)
shape: (1, 24)
┌───────────┬─────────────────────────────────┬─────┬─────────────┬───┬─────────────────────────┬──────────────────────────┬──────────────────────────┬─────────────────────────┐
│ record_id ┆ record_path                     ┆ fs  ┆ num_samples ┆ … ┆ V3                      ┆ V4                       ┆ V5                       ┆ V6                      │
│ ---       ┆ ---                             ┆ --- ┆ ---         ┆   ┆ ---                     ┆ ---                      ┆ ---                      ┆ ---                     │
│ str       ┆ str                             ┆ i64 ┆ i64         ┆   ┆ list[f64]               ┆ list[f64]                ┆ list[f64]                ┆ list[f64]               │
╞═══════════╪═════════════════════════════════╪═════╪═════════════╪═══╪═════════════════════════╪══════════════════════════╪══════════════════════════╪═════════════════════════╡
│ JS00001   ┆ /Users/tushitaa/Documents/DNN/… ┆ 500 ┆ 5000        ┆ … ┆ [-98.0, -98.0, … 142.0] ┆ [810.0, 810.0, … -171.0] ┆ [810.0, 810.0, … -166.0] ┆ [527.0, 527.0, … 112.0] │
└───────────┴─────────────────────────────────┴─────┴─────────────┴───┴─────────────────────────┴──────────────────────────┴──────────────────────────┴─────────────────────────┘
>>> df[ 'I']
shape: (45_152,)
Series: 'I' [list[f64]]
[
	[-254.0, -254.0, … 5.0]
	[-10.0, -24.0, … 10.0]
	[195.0, 195.0, … 102.0]
	[5.0, 5.0, … 0.0]
	[-29.0, -29.0, … -10.0]
	…
	[-337.0, -498.0, … -122.0]
	[88.0, 93.0, … -93.0]
	[59.0, 54.0, … 185.0]
	[-54.0, -59.0, … -151.0]
	[0.0, 0.0, … 59.0]
]
>>> df[ 'I'].shape
(45152,)
>>> df.shape
(45152, 24)
>>> df[ 'I'].to_numpy()
array([array([-254., -254., -254., ...,  -34.,   24.,    5.], shape=(5000,)),
       array([-10., -24., -20., ...,  15.,  10.,  10.], shape=(5000,)),
       array([195., 195., 195., ...,  98.,  88., 102.], shape=(5000,)),
       ...,
       array([ 59.,  54.,  24., ..., 190., 190., 185.], shape=(5000,)),
       array([ -54.,  -59.,  -49., ..., -142., -137., -151.], shape=(5000,)),
       array([ 0.,  0., 34., ..., 44., 49., 59.], shape=(5000,))],
      shape=(45152,), dtype=object)
>>> df.head(2)
shape: (2, 24)
┌───────────┬─────────────────────────────────┬─────┬─────────────┬───┬─────────────────────────┬──────────────────────────┬──────────────────────────┬─────────────────────────┐
│ record_id ┆ record_path                     ┆ fs  ┆ num_samples ┆ … ┆ V3                      ┆ V4                       ┆ V5                       ┆ V6                      │
│ ---       ┆ ---                             ┆ --- ┆ ---         ┆   ┆ ---                     ┆ ---                      ┆ ---                      ┆ ---                     │
│ str       ┆ str                             ┆ i64 ┆ i64         ┆   ┆ list[f64]               ┆ list[f64]                ┆ list[f64]                ┆ list[f64]               │
╞═══════════╪═════════════════════════════════╪═════╪═════════════╪═══╪═════════════════════════╪══════════════════════════╪══════════════════════════╪═════════════════════════╡
│ JS00001   ┆ /Users/tushitaa/Documents/DNN/… ┆ 500 ┆ 5000        ┆ … ┆ [-98.0, -98.0, … 142.0] ┆ [810.0, 810.0, … -171.0] ┆ [810.0, 810.0, … -166.0] ┆ [527.0, 527.0, … 112.0] │
│ JS00002   ┆ /Users/tushitaa/Documents/DNN/… ┆ 500 ┆ 5000        ┆ … ┆ [63.0, 59.0, … 15.0]    ┆ [54.0, 34.0, … -24.0]    ┆ [49.0, 34.0, … -29.0]    ┆ [0.0, -15.0, … -5.0]    │
└───────────┴─────────────────────────────────┴─────┴─────────────┴───┴─────────────────────────┴──────────────────────────┴──────────────────────────┴─────────────────────────┘
>>> df.shape
(45152, 24)


```

1. Here V5 stores arrays. It means No of time samples for V5 lead.

2. Entire dataframe shape is (45152,24). It means 45152 number of records and 24 columns (['record_id', 'record_path', 'fs', 'num_samples', 'num_leads', 'duration_sec', 'Age', 'Sex', 'Dx', 'Rx', 'Hx', 'Sx', 'I', 'II', 'III', 'aVR', 'aVL', 'aVF', 'V1', 'V2', 'V3', 'V4', 'V5', 'V6'])


### Basic Usage

```python
python3 fileOps.py
```

### Configure Input/Output Paths

Edit the script's main block:

```python
if __name__ == "__main__":
    ROOT_DIR = "/path/to/your/WFDBRecords"
    OUTPUT_DIR = "/path/to/output_dir"
    
    df = read_all_records(ROOT_DIR, OUTPUT_DIR)
```

### Custom Worker Count

```python
df = read_all_records(
    ROOT_DIR,
    OUTPUT_DIR,
    workers=4  # Use 4 workers instead of default (8 or CPU count)
)
```

## Input Format

### Expected Directory Structure

```
WFDBRecords/
├── patient1.hea
├── patient1.mat
├── patient2.hea
├── patient2.mat
└── ...
```

### Supported Header Formats

**Format 1 (Standard WFDB)**
```
JS00002 12 500 5000
```

**Format 2 (Non-Standard)**
```
JS01052 12 500 500000/mV 16 0 15 31255 0 I
```

## Output

### Parquet File Schema

| Column | Type | Description |
|--------|------|-------------|
| `record_id` | string | Patient/record identifier |
| `record_path` | string | Full path to record files |
| `fs` | int | Sampling frequency (Hz) |
| `num_samples` | int | Number of samples per lead |
| `num_leads` | int | Number of ECG leads (typically 12) |
| `duration_sec` | float | Recording duration in seconds |
| `Age` | string | Patient age |
| `Sex` | string | Patient sex |
| `Dx` | string | Diagnosis codes |
| `Rx` | string | Treatment information |
| `Hx` | string | Patient history |
| `Sx` | string | Symptoms |
| `I, II, III, aVR, aVL, aVF, V1-V6` | list[float] | 12-lead ECG signals |

### File Locations

- **Parquet**: `output_dir/ecg_records.parquet` (3.3GB for 45K records)
- **CSV**: Not generated (nested signal data incompatible with CSV format)

## Performance

- **Processing Speed**: ~156 patients/second (8 workers on M4)
- **Memory Usage**: ~1-2GB (batch processing prevents overflow)
- **Total Time**: ~12 minutes for 45,152 records

## Memory Optimization

The script processes records in batches of 1000 to prevent memory exhaustion:

```python
batch_size = 1000  # Adjust if needed
```

For systems with limited RAM, reduce batch size:
```python
batch_size = 500  # More frequent disk writes, lower memory usage
```

## Reading Output Data

### Load Full Dataset

```python
import polars as pl

df = pl.read_parquet("output_dir/ecg_records.parquet")
print(f"Total records: {len(df):,}")
```

### Load Specific Columns

```python
# Load only metadata (no signals)
df = pl.read_parquet(
    "output_dir/ecg_records.parquet",
    columns=["record_id", "Age", "Sex", "Dx", "fs", "duration_sec"]
)
```

### Access ECG Signals

```python
# Get signal for first patient
first_record = df[0]
lead_I = first_record["I"].to_list()  # Lead I signal as Python list
lead_II = first_record["II"].to_list()  # Lead II signal

print(f"Lead I samples: {len(lead_I)}")
```

## Troubleshooting

### "zsh: killed" Error

**Cause**: Out of memory (OOM)

**Solutions**:
1. Reduce worker count: `workers=2`
2. Reduce batch size: `batch_size=500`
3. Close other applications

### Missing Dependencies

```bash
pip install --upgrade polars numpy scipy tqdm
```

### Permission Errors

Ensure write permissions for output directory:
```bash
chmod 755 /path/to/output_dir
```

## Technical Details

### Batch Processing Flow

1. Discover all `.hea` files recursively
2. Split into batches of 1000 records
3. Process each batch in parallel using `ProcessPoolExecutor`
4. Write batch to Parquet (append mode)
5. Clear batch from memory
6. Repeat until all records processed

### Signal Data Handling

- Signals read from `.mat` files as numpy arrays
- Converted to Python lists for Parquet storage
- Shape: `(num_samples, 12)` → transposed to 12 separate lists

## License

MIT License

Copyright (c) 2026 Somesh Kumar Routray

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

## Contact

someshkumarroutray1@gmail.com
