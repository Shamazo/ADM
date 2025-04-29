import argparse
from pathlib import Path
from data_conversion import ParquetRelationConverter


def main():
    parser = argparse.ArgumentParser(description="Process input and output directories.")
    parser.add_argument(
        "--input-dir",
        type=Path,
        required=True,
        help="Path to the input directory containing parquet files for the relation."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Path to the output directory."
    )
    args = parser.parse_args()

    if not args.input_dir.exists() or not args.input_dir.is_dir():
        raise FileNotFoundError(f"Input directory does not exist: {args.input_dir}")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    # sort for reproducibility
    parquet_files = sorted(list(args.input_dir.glob("*.parquet")))

    converter = ParquetRelationConverter(parquet_files, "taxi", "yellow_tripdata", args.output_dir)
    """    
    For now skipping this column because it is not used in queries and to add it we need to extend the convertor to 
    handle string types. String types are slightly non-trivial, because we are using DuckDB which treats all strings as 
    varchars, even if they are known fixed length like "store_and_fwd_flag"
    """
    converter.convert_relation(["store_and_fwd_flag", "vendor_id"])


if __name__ == "__main__":
    main()
