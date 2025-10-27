#!/usr/bin/env python3
"""
Script to add synthetic columns to the taxi dataset.

This script generates two synthetic columns:
1. DOLocationID_synthetic: Based on taxi zone LocationIDs with controlled airport distribution
2. payment_type_synthetic: Payment type codes with controlled distribution

The data is generated in two phases with different distributions:
- Phase 1 (first 50%): 80% payment_type=1, 1% airport locations (JFK, LaGuardia, Newark)
- Phase 2 (second 50%): 15% payment_type=1, 70% airport locations (JFK, LaGuardia, Newark)

This script does not partition the columns and writes them as single binary files.
It reads the existing catalog.json, updates it to include the new columns,
and writes the updated catalog back to disk, creating a backup of the original.
"""

import argparse
import csv
import json
import shutil
from pathlib import Path

import numpy as np


def parse_zone_lookup(zone_lookup_path):
    """
    Parse taxi_zone_lookup.csv and separate LocationIDs by zone.

    Returns:
        tuple: (airport_location_ids, non_airport_location_ids)
    """
    airport_zones = {'JFK Airport', 'LaGuardia Airport', 'Newark Airport'}
    airport_ids = []
    non_airport_ids = []

    with open(zone_lookup_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            location_id = int(row['LocationID'])
            zone = row['Zone']

            if zone in airport_zones:
                airport_ids.append(location_id)
            else:
                non_airport_ids.append(location_id)

    return np.array(airport_ids, dtype=np.int64), np.array(non_airport_ids, dtype=np.int64)


def generate_payment_type_column(num_rows, phase1_rows, seed=42):
    """
    Generate payment_type_synthetic column.

    Phase 1: 80% value=1, remaining uniformly distributed among {0,2,3,4,5,6}
    Phase 2: 15% value=1, remaining uniformly distributed among {0,2,3,4,5,6}

    Args:
        num_rows: Total number of rows
        phase1_rows: Number of rows in phase 1
        seed: Random seed

    Returns:
        numpy array of int64
    """
    rng = np.random.default_rng(seed)
    payment_data = np.zeros(num_rows, dtype=np.int64)

    # Phase 1: First 50% of rows
    phase1_mask = rng.random(phase1_rows) < 0.80  # 60% get value 1
    payment_data[:phase1_rows][phase1_mask] = 1

    # For remaining rows in phase 1, choose uniformly from {0,2,3,4,5,6}
    non_one_indices_p1 = np.where(~phase1_mask)[0]
    payment_data[non_one_indices_p1] = rng.choice([0, 2, 3, 4, 5, 6], size=len(non_one_indices_p1))

    # Phase 2: Second 50% of rows
    phase2_rows = num_rows - phase1_rows
    phase2_mask = rng.random(phase2_rows) < 0.15  # 35% get value 1
    payment_data[phase1_rows:][phase2_mask] = 1

    # For remaining rows in phase 2, choose uniformly from {0,2,3,4,5,6}
    non_one_indices_p2 = np.where(~phase2_mask)[0]
    payment_data[phase1_rows + non_one_indices_p2] = rng.choice([0, 2, 3, 4, 5, 6], size=len(non_one_indices_p2))

    return payment_data


def generate_location_column(num_rows, phase1_rows, airport_ids, non_airport_ids, seed=42):
    """
    Generate DOLocationID_synthetic column.

    Phase 1: 1% Airport LocationIDs, 99% non-Airport LocationIDs
    Phase 2: 70% Airport LocationIDs, 30% non-Airport LocationIDs

    Args:
        num_rows: Total number of rows
        phase1_rows: Number of rows in phase 1
        airport_ids: Array of Airport LocationIDs (JFK, LaGuardia, Newark)
        non_airport_ids: Array of non-Airport LocationIDs
        seed: Random seed

    Returns:
        numpy array of int64
    """
    rng = np.random.default_rng(seed + 1)  # Different seed for location column
    location_data = np.zeros(num_rows, dtype=np.int64)

    # Phase 1: 2% Airport
    phase1_mask = rng.random(phase1_rows) < 0.01  # 1% get Airport
    airport_count_p1 = np.sum(phase1_mask)
    non_airport_count_p1 = phase1_rows - airport_count_p1

    location_data[:phase1_rows][phase1_mask] = rng.choice(airport_ids, size=airport_count_p1)
    location_data[:phase1_rows][~phase1_mask] = rng.choice(non_airport_ids, size=non_airport_count_p1)

    # Phase 2: 8% Airport
    phase2_rows = num_rows - phase1_rows
    phase2_mask = rng.random(phase2_rows) < 0.50  # 10% get Airport
    airport_count_p2 = np.sum(phase2_mask)
    non_airport_count_p2 = phase2_rows - airport_count_p2

    location_data[phase1_rows:][phase2_mask] = rng.choice(airport_ids, size=airport_count_p2)
    location_data[phase1_rows:][~phase2_mask] = rng.choice(non_airport_ids, size=non_airport_count_p2)

    return location_data


def write_binary_file(data, output_path):
    """
    Write numpy array to binary file in native byte order.

    Args:
        data: numpy array
        output_path: Path to output file
    """
    with open(output_path, 'wb') as f:
        f.write(data.tobytes())
    print(f"Written {len(data)} rows ({data.nbytes} bytes) to {output_path}")


def backup_catalog(catalog_path):
    """
    Create a backup of catalog.json.

    Args:
        catalog_path: Path to catalog.json

    Returns:
        Path to backup file
    """
    backup_path = catalog_path.parent / f"{catalog_path.stem}{catalog_path.suffix}_backup"
    shutil.copy2(catalog_path, backup_path)
    print(f"Backed up catalog to {backup_path}")
    return backup_path


def update_catalog(catalog_path):
    """
    Update catalog.json to add the two new synthetic columns.

    Args:
        catalog_path: Path to catalog.json
    """
    with open(catalog_path, 'r') as f:
        catalog = json.load(f)

    # Get the attributes list for inputs_taxi_yellow_tripdata
    attributes = catalog['inputs_taxi_yellow_tripdata']['type']['inner']['attributes']

    # Add DOLocationID_synthetic at attrNo 20
    attributes.append({
        "type": {
            "type": "int64"
        },
        "relName": "inputs/taxi/yellow_tripdata.csv",
        "attrName": "DOLocationID_synthetic",
        "attrNo": 20
    })

    # Add payment_type_synthetic at attrNo 21
    attributes.append({
        "type": {
            "type": "int64"
        },
        "relName": "inputs/taxi/yellow_tripdata.csv",
        "attrName": "payment_type_synthetic",
        "attrNo": 21
    })

    # Write updated catalog
    with open(catalog_path, 'w') as f:
        json.dump(catalog, f, indent=4)

    print(f"Updated catalog with 2 new synthetic columns")


def main():
    parser = argparse.ArgumentParser(
        description='Generate synthetic columns for taxi dataset'
    )
    parser.add_argument(
        '--catalog-path',
        type=Path,
        default=Path('/scratch/nicholso/deploy/tmp/tmp.YD2SgUVlV5/cmake-build-release-dias49/opt/pelago/inputs/taxi/catalog.json'),
        help='Path to catalog.json'
    )
    parser.add_argument(
        '--zone-lookup-path',
        type=Path,
        default=Path('/nvme31/nicholso/data/taxi/taxi_zone_lookup.csv'),
        help='Path to taxi_zone_lookup.csv'
    )
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=Path('/scratch/nicholso/deploy/tmp/tmp.YD2SgUVlV5/cmake-build-release-dias49/opt/pelago/inputs/taxi'),
        help='Directory for output binary files'
    )

    args = parser.parse_args()

    print("=" * 60)
    print("Taxi Synthetic Column Generator")
    print("=" * 60)
    print(f"Catalog path: {args.catalog_path}")
    print(f"Zone lookup path: {args.zone_lookup_path}")
    print(f"Output directory: {args.output_dir}")
    print()

    # Read catalog to get linehint (total number of rows)
    with open(args.catalog_path, 'r') as f:
        catalog = json.load(f)

    num_rows = catalog['inputs_taxi_yellow_tripdata']['plugin']['linehint']
    phase1_rows = num_rows // 2  # First 50% of rows

    print(f"Total rows: {num_rows:,}")
    print(f"Phase 1 rows: {phase1_rows:,}")
    print(f"Phase 2 rows: {num_rows - phase1_rows:,}")
    print()

    # Parse zone lookup to get Airport and non-Airport LocationIDs
    print("Parsing zone lookup data...")
    airport_ids, non_airport_ids = parse_zone_lookup(args.zone_lookup_path)
    print(f"Airport LocationIDs (JFK, LaGuardia, Newark): {len(airport_ids)}")
    print(f"Non-Airport LocationIDs: {len(non_airport_ids)}")
    print()

    # Generate payment_type_synthetic column
    print("Generating payment_type_synthetic column...")
    payment_data = generate_payment_type_column(num_rows, phase1_rows, seed=42)

    # Generate DOLocationID_synthetic column
    print("Generating DOLocationID_synthetic column...")
    location_data = generate_location_column(num_rows, phase1_rows, airport_ids, non_airport_ids, seed=42)
    print()

    # Write binary files
    print("Writing binary files...")
    payment_output_path = args.output_dir / 'yellow_tripdata.csv.payment_type_synthetic'
    location_output_path = args.output_dir / 'yellow_tripdata.csv.DOLocationID_synthetic'

    write_binary_file(payment_data, payment_output_path)
    write_binary_file(location_data, location_output_path)
    print()

    # Backup and update catalog
    print("Updating catalog...")
    backup_catalog(args.catalog_path)
    update_catalog(args.catalog_path)
    print()

    print("=" * 60)
    print("Synthetic column generation complete!")
    print("=" * 60)
    print()
    print("Summary:")
    print(f"  - payment_type_synthetic: {payment_output_path}")
    print(f"  - DOLocationID_synthetic: {location_output_path}")
    print(f"  - Updated catalog: {args.catalog_path}")
    print(f"  - Backup catalog: {args.catalog_path.parent / f'{args.catalog_path.stem}_backup{args.catalog_path.suffix}'}")


if __name__ == '__main__':
    main()
