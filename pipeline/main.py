import argparse
from datatrove.executor import LocalPipelineExecutor
from datatrove.pipeline.readers import ParquetReader
from datatrove.pipeline.writers import JsonlWriter
from PIIContextExtractor import PIIContextExtractor

def run_pipeline(workers, start_method, logging_dir, tasks, parquet_file, output_path):
    # Enforce that we use the predefined EU regex file for detection
    eu_file_path = 'resources/eu_regexes.xlsx'
    
    pipeline_exec = LocalPipelineExecutor(
        pipeline=[
            ParquetReader(parquet_file, limit=2000),
            PIIContextExtractor(eu_file_path=eu_file_path, context_window=60),
            JsonlWriter(f"{output_path}")
        ],
        start_method=start_method,
        workers=workers,
        logging_dir=logging_dir,
        tasks=tasks
    )

    pipeline_exec.run()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run PII context extraction pipeline with custom options.")
    
    # Define the arguments
    parser.add_argument('--workers', type=int, default=1, help='Number of workers to use in the pipeline execution.')
    parser.add_argument('--start_method', type=str, default='spawn', help='Start method for multiprocessing.')
    parser.add_argument('--logging_dir', type=str, default='logs/pii_contextExtractor', help='Directory for logging.')
    parser.add_argument('--tasks', type=int, default=1, help='Number of tasks to run in the pipeline.')
    parser.add_argument('--parquet_file', type=str, required=True, help='Path to the Parquet file to read from.')
    parser.add_argument('--output_path', type=str, required=True, help='Output directory for JSONL writer.')
    
    # Parse the arguments
    args = parser.parse_args()

    # Run the pipeline with parsed arguments
    run_pipeline(
        workers=args.workers,
        start_method=args.start_method,
        logging_dir=args.logging_dir,
        tasks=args.tasks,
        parquet_file=args.parquet_file,
        output_path=args.output_path
    )
