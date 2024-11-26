import argparse
from datatrove.executor import SlurmPipelineExecutor
from datatrove.pipeline.filters import LambdaFilter
from datatrove.pipeline.readers import ParquetReader
from datatrove.pipeline.writers import JsonlWriter
from PIIContextExtractor import PIIContextExtractor  # Same folder
from pii_formatter import PIIFormatter  # Assuming you added this file to your project

def run_pipeline(start_method, logging_dir, tasks, parquet_file, output_path, eu_file_path, use_context, limit):
    
    # Select either the PIIContextExtractor or PIIFormatter based on the --use_context argument
    if use_context:
        pii_processor = PIIContextExtractor(eu_file_path=eu_file_path, context_window=60)
    else:
        pii_processor = PIIFormatter(eu_file_path=eu_file_path)  # Assuming you need EU file path for PIIFormatter
    
    pipeline_exec = SlurmPipelineExecutor(
        pipeline=[
            ParquetReader(parquet_file, limit=limit),
            pii_processor, 
            JsonlWriter(f"{output_path}")
        ],
        logging_dir=logging_dir,
        tasks=tasks
    )

    pipeline_exec.run()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run PII processing pipeline with custom options.")
    
    # Define the arguments
    # parser.add_argument('--workers', type=int, default=1, help='Number of workers to use in the pipeline execution.')
    parser.add_argument('--start_method', type=str, default='spawn', help='Start method for multiprocessing.')
    parser.add_argument('--logging_dir', type=str, default='logs/pii_pipeline', help='Directory for logging.')
    parser.add_argument('--tasks', type=int, default=1000, help='Number of tasks to run in the pipeline.')
    parser.add_argument('--parquet_file', type=str, required=True, help='Path to the Parquet file to read from.')
    parser.add_argument('--output_path', type=str, required=True, help='Output directory for JSONL writer.')
    parser.add_argument('--eu_file_path', type=str, default='./resources/eu_regex.xlsx', help='Path to the EU regex file.')
    parser.add_argument('--use_context', action='store_true', help='Whether to use PIIContextExtractor for context extraction.')
    parser.add_argument('--limit', type=int, default=-1, help='Maximum number of documents to read from the Parquet file.') 

    # Parse the arguments
    args = parser.parse_args()

    # Run the pipeline with parsed arguments
    run_pipeline(
        start_method=args.start_method,
        logging_dir=args.logging_dir,
        tasks=args.tasks,
        parquet_file=args.parquet_file,
        output_path=args.output_path,
        eu_file_path=args.eu_file_path,
        use_context=args.use_context, 
        limit=args.limit)
    

