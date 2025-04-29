Scripts to download the taxi yellow trip dataset [(NYC TLC Trip Record Data)](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and convert it to the Proteus format for in-memory processing. 
This downloads and converts yellow trip data for 2011-2024 (inclusive)

Usage:
1. ./download_taxi.sh parquet_data
2. python3 prepare_yellow_tripdata.py --input-dir parquet_data --output-dir prepared_data
3. ln -s prepared_data/taxi proteus_source_root/tests/inputs/taxi

The script depends on duckdb and numpy. If you are using conda, you can create an environment with the required packages using the `environment.yml` file: `conda env create -f environment.yml`