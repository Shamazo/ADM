#!/bin/bash

# Needs to be manually set depending where proteus is built
cd /tmp/tmp.5P5YmHb9o4/cmake-build-debug-dias44/opt/pelago
set -e

# 1 drive compressed
./proteusadm-partition-ssb --input_directory=/scratch/data/ssbm1000 --output_directories=/scratch/nicholso/data/compressed_ssbm1000 --compress_data

# 2 drive compressed
./proteusadm-partition-ssb --input_directory=/scratch/data/ssbm1000 --output_directories=/scratch/nicholso/data/compressed_ssbm1000_0_2,/scratch2/nicholso/data/compressed_ssbm1000_1_2 --compress_data


# 1 drive uncompressed
./proteusadm-partition-ssb --input_directory=/scratch/data/ssbm1000 --output_directories=/scratch/nicholso/data/ssbm1000
# 2 drives uncompressed
./proteusadm-partition-ssb --input_directory=/scratch/data/ssbm1000 --output_directories=/scratch/nicholso/data/ssbm1000_0_2,/scratch2/nicholso/data/ssbm1000_1_2
