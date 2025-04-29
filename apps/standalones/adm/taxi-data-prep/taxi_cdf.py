import matplotlib.pyplot as plt
import argparse
import duckdb
from pathlib import Path
import numpy as np

def main():
    parser = argparse.ArgumentParser(description="Quick and dirty script to get empirical quantiles/CDFs for columns in the taxi data")
    parser.add_argument(
        "--input-dir",
        type=Path,
        required=True,
        help="Path to the input directory containing parquet files for the relation."
    )
    args = parser.parse_args()

    if not args.input_dir.exists() or not args.input_dir.is_dir():
        raise FileNotFoundError(f"Input directory does not exist: {args.input_dir}")


    parquet_files = sorted(list(args.input_dir.glob("*.parquet")))
    con = duckdb.connect(':memory:')
    parquet_file_paths_string = ",".join([f'\'{x}\'' for x in parquet_files])

    num_quantiles = 1000
    quantiles = np.linspace(1.0 / num_quantiles, 1.0, num_quantiles)
    quantiles_sql_array = '[' + ', '.join(map(str, quantiles)) + ']'

    # Fetch sorted values and their percentile ranks
    sql_query = f"""
    WITH QuantileData AS (
        SELECT
            approx_quantile(trip_distance, {quantiles_sql_array}) AS trip_distance_quantiles
        FROM read_parquet([{parquet_file_paths_string}])
        WHERE trip_distance > 0
    )
    -- Unnest the calculated trip distance quantiles and the original quantile values
    SELECT
        UNNEST(q.trip_distance_quantiles) AS trip_distance,
        UNNEST({quantiles_sql_array}) AS cdf
    FROM QuantileData q
    ORDER BY cdf; -- Order the final result for plotting
    """

    # Execute query and get results as numpy arrays
    result = con.execute(sql_query).fetchnumpy()
    values = result['trip_distance']
    cdf = result['cdf']

    # Create the CDF plot
    plt.figure(figsize=(10, 6))
    plt.plot(values, cdf, '-')
    plt.grid(True)
    plt.xlabel('trip_distance')
    plt.ylabel('Cumulative Probability')
    plt.xscale('log')
    plt.xlim(left=0)
    plt.title('Empirical CDF')
    plt.savefig('trip_distance_empirical_cdf.png', dpi=300, bbox_inches='tight')

    sql_query = f"""
    WITH QuantileData AS (
        SELECT
            approx_quantile(fare_amount, {quantiles_sql_array}) AS fare_amount_quantiles
        FROM read_parquet([{parquet_file_paths_string}])
        WHERE fare_amount > 0
    )
    -- Unnest the calculated trip distance quantiles and the original quantile values
    SELECT
        UNNEST(q.fare_amount_quantiles) AS fare_amount,
        UNNEST({quantiles_sql_array}) AS cdf
    FROM QuantileData q
    ORDER BY cdf; -- Order the final result for plotting
    """

    # Execute query and get results as numpy arrays
    result = con.execute(sql_query).fetchnumpy()
    values = result['fare_amount']
    cdf = result['cdf']

    # Create the CDF plot
    plt.figure(figsize=(10, 6))
    plt.plot(values, cdf, '-')
    plt.grid(True)
    plt.xlabel('fare_amount')
    plt.ylabel('Cumulative Probability')
    plt.xscale('log')
    plt.xlim(left=0)
    plt.title('Empirical CDF')
    plt.savefig('fare_amount_empirical_cdf.png', dpi=300, bbox_inches='tight')

    sql_query = f"""
            SELECT
                quantile_cont(fare_amount, 0.001) AS p0_1_quantile,
                quantile_cont(fare_amount, 0.01) AS p1_quantile,
                quantile_cont(fare_amount, 0.1) AS p10_quantile,
                quantile_cont(fare_amount, 0.5) AS p50_quantile
            FROM read_parquet([{parquet_file_paths_string}])
            WHERE fare_amount > 0
        """
    result = con.execute(sql_query).fetchnumpy()
    print(f"fare amount: P0.1 {result['p0_1_quantile']} , P1 {result['p1_quantile']} , P10 {result['p10_quantile']} , P50 {result['p50_quantile']} ")

    sql_query = f"""
                SELECT
                    quantile_cont(trip_distance, 0.001) AS p0_1_quantile,
                    quantile_cont(trip_distance, 0.01) AS p1_quantile,
                    quantile_cont(trip_distance, 0.1) AS p10_quantile,
                    quantile_cont(trip_distance, 0.5) AS p50_quantile
                FROM read_parquet([{parquet_file_paths_string}]) 
                WHERE trip_distance > 0
            """
    result = con.execute(sql_query).fetchnumpy()
    print(f"trip distance: P0.1 {result['p0_1_quantile']} , P1 {result['p1_quantile']} , P10 {result['p10_quantile']} ,P50 {result['p50_quantile']} ")

if __name__ == "__main__":
    main()