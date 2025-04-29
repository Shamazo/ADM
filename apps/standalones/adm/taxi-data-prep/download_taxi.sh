#!/bin/bash

# Set default download directory to current directory
DOWNLOAD_DIR="${1:-.}"

# Create the download directory if it doesn't exist
mkdir -p "$DOWNLOAD_DIR"

YEAR=2011
MONTH=1
END_YEAR=2024
END_MONTH=12
BASE_URL='https://d37ci6vzurychx.cloudfront.net'

echo "Files will be downloaded to: $DOWNLOAD_DIR"

while [ $YEAR -le $END_YEAR ]; do
    while [ $MONTH -le 12 ]; do
        if [ $YEAR -eq $END_YEAR ] && [ $MONTH -gt $END_MONTH ]; then
            break
        fi

        # Convert single-digit month to two-digit format
        if [ $MONTH -lt 10 ]; then
            MONTH_STR="0$MONTH"
        else
            MONTH_STR="$MONTH"
        fi

        file_name="yellow_tripdata_${YEAR}-${MONTH_STR}.parquet"
        file_url="${BASE_URL}/trip-data/${file_name}"
        file_path="${DOWNLOAD_DIR}/${file_name}"

        echo "Downloading $file_name..."

        # Download the file with progress bar using wget
        wget -c "$file_url" --show-progress -O "$file_path"

        # Increment month
        MONTH=$((MONTH + 1))
    done

    # Increment year and reset month
    YEAR=$((YEAR + 1))
    MONTH=1
done

echo "All downloads completed in $DOWNLOAD_DIR"