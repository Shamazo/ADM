#!/bin/bash

# Needs to be manually set depending where proteus is built
cd /tmp/tmp/tmp.J1VERIwUSP/cmake-build-release-dias46/opt/pelago
set -e

#/scratch numa 0
#/scratch2 numa 0
#/scratch3 numa 1
#/scratch4 numa 1

## SSBM 100
#### compressed
## 1 drive compressed
./proteusadm-partition-ssb --input_directory=/scratch2/data/ssbm100 --output_directories=/scratch/nicholso/data/compressed_ssbm100 --compress_data
# 2 drive compressed
./proteusadm-partition-ssb --input_directory=/scratch2/data/ssbm100 --output_directories=/scratch/nicholso/data/compressed_ssbm100_0_2,/scratch3/nicholso/data/compressed_ssbm100_1_2 --compress_data
# 4 drives compressed
./proteusadm-partition-ssb --input_directory=/scratch2/data/ssbm100 --output_directories=/scratch/nicholso/data/compressed_ssbm100_0_4,/scratch2/nicholso/data/compressed_ssbm100_1_4,/scratch3/nicholso/data/compressed_ssbm100_2_4,/scratch4/nicholso/data/compressed_ssbm100_3_4 --compress_data

#### uncompressed
./proteusadm-partition-ssb --input_directory=/scratch2/data/ssbm100 --output_directories=/scratch/nicholso/data/ssbm100
# 2 drive compressed
./proteusadm-partition-ssb --input_directory=/scratch2/data/ssbm100 --output_directories=/scratch/nicholso/data/ssbm100_0_2,/scratch3/nicholso/data/ssbm100_1_2
# 4 drives compressed
./proteusadm-partition-ssb --input_directory=/scratch2/data/ssbm100 --output_directories=/scratch/nicholso/data/ssbm100_0_4,/scratch2/nicholso/data/ssbm100_1_4,/scratch3/nicholso/data/ssbm100_2_4,/scratch4/nicholso/data/ssbm100_3_4

# SSBM 1000
### compressed
# 1 drive compressed
./proteusadm-partition-ssb --input_directory=/scratch2/data/ssbm1000 --output_directories=/scratch4/nicholso/data/compressed_ssbm1000 --compress_data
# 2 drive compressed
./proteusadm-partition-ssb --input_directory=/scratch2/data/ssbm1000 --output_directories=/scratch/nicholso/data/compressed_ssbm1000_0_2,/scratch3/nicholso/data/compressed_ssbm1000_1_2 --compress_data
# 4 drives compressed
./proteusadm-partition-ssb --input_directory=/scratch2/data/ssbm1000 --output_directories=/scratch/nicholso/data/compressed_ssbm1000_0_4,/scratch2/nicholso/data/compressed_ssbm1000_1_4,/scratch3/nicholso/data/compressed_ssbm1000_2_4,/scratch4/nicholso/data/compressed_ssbm1000_3_4 --compress_data

#### uncompressed
./proteusadm-partition-ssb --input_directory=/scratch2/data/ssbm1000 --output_directories=/scratch4/nicholso/data/ssbm1000
# 2 drive compressed
./proteusadm-partition-ssb --input_directory=/scratch2/data/ssbm1000 --output_directories=/scratch/nicholso/data/ssbm1000_0_2,/scratch3/nicholso/data/ssbm1000_1_2 
# 4 drives compressed
./proteusadm-partition-ssb --input_directory=/scratch2/data/ssbm1000 --output_directories=/scratch/nicholso/data/ssbm1000_0_4,/scratch2/nicholso/data/ssbm1000_1_4,/scratch3/nicholso/data/ssbm1000_2_4,/scratch4/nicholso/data/ssbm1000_3_4




