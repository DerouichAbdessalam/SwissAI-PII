# PII Context Extraction Pipeline

This project implements a pipeline to detect Personally Identifiable Information (PII) in web crawls, extract surrounding context, and validate the detection using LLMs.

## Features
- **PII Detection**: Detects PII based on EU-specific regex patterns.
- **Context Extraction**: Extracts context around PII candidates.
- **LLM Validation**: Uses an LLM model to validate if a PII candidate is accurate.
- **Pipeline Execution**: Built using `datatrove`'s `LocalPipelineExecutor`.

## Setup

1. **Install Dependencies**: Ensure you have the necessary dependencies installed, including the `datatrove` library and the required packages.

2. **Ensure Input Data**:
    - Parquet file: The input file in Parquet format that the pipeline will process.
    - EU Regexes: The regex file (`resources/eu_regexes.xlsx`) that contains the regex patterns to detect PII.

## Running the Pipeline

### Command Line Usage

To run the pipeline, use the `main.py` script with the following options:

```bash
python main.py --workers <num_workers> --start_method <start_method> --logging_dir <logging_dir> --tasks <num_tasks> --parquet_file <path_to_parquet_file> --output_path <output_directory>
```


### Example
```bash
python main.py --workers 2 --start_method spawn --logging_dir logs/pii_context --tasks 3 --parquet_file hf://datasets/HuggingFaceFW/fineweb/sample/100BT --output_path output_contextExtractor/
```
### Arguments:
- `workers`: Number of workers for parallel processing.
- `start_method`: Multiprocessing start method, default is spawn.
- `logging_dir`: Directory for pipeline logs.
- `tasks`: Number of tasks to process.
- `parquet_file`: Input Parquet file to process.
- `output_path`: Directory to write the output JSONL files.

---

## Cluster Execution

To execute the pipeline on a cluster, use the provided bash script below.

### Bash Script
A bash script is provided to easily run the pipeline on a cluster.

#### Steps to Run:
1. Edit the bash script `run_pipeline_cluster.sh` and adjust the arguments for your cluster environment.
2. Submit the script to your cluster's job scheduler.

```bash
bash run_pipeline_cluster.sh
```

