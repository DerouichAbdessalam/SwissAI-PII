#!/bin/bash

# Specify the number of workers, logging directory, and tasks as arguments
NUM_WORKERS=4
START_METHOD="spawn"
LOGGING_DIR="logs/pii_context"
NUM_TASKS=3
PARQUET_FILE="hf://datasets/HuggingFaceFW/fineweb/sample/100BT"
OUTPUT_PATH="output_contextExtractor/"
EU_REGEX_FILE="resources/eu_regexes.xlsx"

# Run the pipeline
python main.py --workers $NUM_WORKERS --start_method $START_METHOD --logging_dir $LOGGING_DIR --tasks $NUM_TASKS --parquet_file $PARQUET_FILE --output_path $OUTPUT_PATH --eu_file_path $EU_REGEX_FILE
