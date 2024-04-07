#!/bin/bash

# Needs to be manually set depending where proteus is built
cd /tmp/tmp.qoUKJ3y3fN/cmake-build-release-dias49/opt/pelago
set -e

## SSBM 1000
#### compressed
# 1 drive compressed
./proteusadm-partition-ssb --input_directory=/scratch/data/ssbm1000 --output_directories=/nvme14/nicholso/data/compressed_ssbm1000 --compress_data
# 2 drive compressed
./proteusadm-partition-ssb --input_directory=/scratch/data/ssbm1000 --output_directories=/nvme12/nicholso/data/compressed_ssbm1000_0_2,/nvme22/nicholso/data/compressed_ssbm1000_1_2 --compress_data
# 4 drives compressed
./proteusadm-partition-ssb --input_directory=/scratch/data/ssbm1000 --output_directories=/nvme0/nicholso/data/compressed_ssbm1000_0_4,/nvme13/nicholso/data/compressed_ssbm1000_1_4,/nvme28/nicholso/data/compressed_ssbm1000_2_4,/nvme22/nicholso/data/compressed_ssbm1000_3_4 --compress_data
# 6 drives compressed
./proteusadm-partition-ssb --input_directory=/scratch/data/ssbm1000 --output_directories=/nvme0/nicholso/data/compressed_ssbm1000_0_6,/nvme13/nicholso/data/compressed_ssbm1000_1_6,/nvme6/nicholso/data/compressed_ssbm1000_2_6,/nvme28/nicholso/data/compressed_ssbm1000_3_6,/nvme20/nicholso/data/compressed_ssbm1000_4_6,/nvme24/nicholso/data/compressed_ssbm1000_5_6 --compress_data
# 8 drives compressed
./proteusadm-partition-ssb --input_directory=/scratch/data/ssbm1000 --output_directories=/nvme0/nicholso/data/compressed_ssbm1000_0_8,/nvme13/nicholso/data/compressed_ssbm1000_1_8,/nvme14/nicholso/data/compressed_ssbm1000_2_8,/nvme6/nicholso/data/compressed_ssbm1000_3_8,/nvme28/nicholso/data/compressed_ssbm1000_4_8,/nvme20/nicholso/data/compressed_ssbm1000_5_8,/nvme21/nicholso/data/compressed_ssbm1000_6_8,/nvme24/nicholso/data/compressed_ssbm1000_7_8 --compress_data
# 10 drives compressed
./proteusadm-partition-ssb --input_directory=/scratch/data/ssbm1000 --output_directories=/nvme0/nicholso/data/compressed_ssbm1000_0_10,/nvme7/nicholso/data/compressed_ssbm1000_1_10,/nvme13/nicholso/data/compressed_ssbm1000_2_10,/nvme14/nicholso/data/compressed_ssbm1000_3_10,/nvme6/nicholso/data/compressed_ssbm1000_4_10,/nvme28/nicholso/data/compressed_ssbm1000_5_10,/nvme29/nicholso/data/compressed_ssbm1000_6_10,/nvme20/nicholso/data/compressed_ssbm1000_7_10,/nvme21/nicholso/data/compressed_ssbm1000_8_10,/nvme24/nicholso/data/compressed_ssbm1000_9_10 --compress_data
# 12 drives compressed
./proteusadm-partition-ssb --input_directory=/scratch/data/ssbm1000 --output_directories=/nvme0/nicholso/data/compressed_ssbm1000_0_12,/nvme7/nicholso/data/compressed_ssbm1000_1_12,/nvme13/nicholso/data/compressed_ssbm1000_2_12,/nvme14/nicholso/data/compressed_ssbm1000_3_12,/nvme6/nicholso/data/compressed_ssbm1000_4_12,/nvme9/nicholso/data/compressed_ssbm1000_5_12,/nvme28/nicholso/data/compressed_ssbm1000_6_12,/nvme29/nicholso/data/compressed_ssbm1000_7_12,/nvme20/nicholso/data/compressed_ssbm1000_8_12,/nvme21/nicholso/data/compressed_ssbm1000_9_12,/nvme24/nicholso/data/compressed_ssbm1000_10_12,/nvme25/nicholso/data/compressed_ssbm1000_11_12 --compress_data
# 18 drives compressed
./proteusadm-partition-ssb --input_directory=/scratch/data/ssbm1000 --output_directories=/nvme0/nicholso/data/compressed_ssbm1000_0_18,/nvme7/nicholso/data/compressed_ssbm1000_1_18,/nvme2/nicholso/data/compressed_ssbm1000_2_18,/nvme13/nicholso/data/compressed_ssbm1000_3_18,/nvme14/nicholso/data/compressed_ssbm1000_4_18,/nvme15/nicholso/data/compressed_ssbm1000_5_18,/nvme6/nicholso/data/compressed_ssbm1000_6_18,/nvme9/nicholso/data/compressed_ssbm1000_7_18,/nvme10/nicholso/data/compressed_ssbm1000_8_18,/nvme28/nicholso/data/compressed_ssbm1000_9_18,/nvme29/nicholso/data/compressed_ssbm1000_10_18,/nvme30/nicholso/data/compressed_ssbm1000_11_18,/nvme20/nicholso/data/compressed_ssbm1000_12_18,/nvme21/nicholso/data/compressed_ssbm1000_13_18,/nvme22/nicholso/data/compressed_ssbm1000_14_18,/nvme24/nicholso/data/compressed_ssbm1000_15_18,/nvme25/nicholso/data/compressed_ssbm1000_16_18,/nvme26/nicholso/data/compressed_ssbm1000_17_18 --compress_data
# 24 drives compressed
./proteusadm-partition-ssb --input_directory=/scratch/data/ssbm1000 --output_directories=/nvme0/nicholso/data/compressed_ssbm1000_0_24,/nvme7/nicholso/data/compressed_ssbm1000_1_24,/nvme2/nicholso/data/compressed_ssbm1000_2_24,/nvme3/nicholso/data/compressed_ssbm1000_3_24,/nvme12/nicholso/data/compressed_ssbm1000_4_24,/nvme13/nicholso/data/compressed_ssbm1000_5_24,/nvme14/nicholso/data/compressed_ssbm1000_6_24,/nvme15/nicholso/data/compressed_ssbm1000_7_24,/nvme6/nicholso/data/compressed_ssbm1000_8_24,/nvme9/nicholso/data/compressed_ssbm1000_9_24,/nvme10/nicholso/data/compressed_ssbm1000_10_24,/nvme11/nicholso/data/compressed_ssbm1000_11_24,/nvme28/nicholso/data/compressed_ssbm1000_12_24,/nvme29/nicholso/data/compressed_ssbm1000_13_24,/nvme30/nicholso/data/compressed_ssbm1000_14_24,/nvme31/nicholso/data/compressed_ssbm1000_15_24,/nvme20/nicholso/data/compressed_ssbm1000_16_24,/nvme21/nicholso/data/compressed_ssbm1000_17_24,/nvme22/nicholso/data/compressed_ssbm1000_18_24,/nvme23/nicholso/data/compressed_ssbm1000_19_24,/nvme24/nicholso/data/compressed_ssbm1000_20_24,/nvme25/nicholso/data/compressed_ssbm1000_21_24,/nvme26/nicholso/data/compressed_ssbm1000_22_24,/nvme27/nicholso/data/compressed_ssbm1000_23_24 --compress_data

#### uncompressed
## 1 drive
./proteusadm-partition-ssb --input_directory=/scratch/data/ssbm1000 --output_directories=/nvme13/nicholso/data/sbm1000
# 2 drive
./proteusadm-partition-ssb --input_directory=/scratch/data/ssbm1000 --output_directories=/nvme12/nicholso/data/sbm1000_0_2,/nvme23/nicholso/data/sbm1000_1_2
# 4 drives
./proteusadm-partition-ssb --input_directory=/scratch/data/ssbm1000 --output_directories=/nvme0/nicholso/data/sbm1000_0_4,/nvme12/nicholso/data/sbm1000_1_4,/nvme28/nicholso/data/sbm1000_2_4,/nvme21/nicholso/data/sbm1000_3_4
# 6 drives
./proteusadm-partition-ssb --input_directory=/scratch/data/ssbm1000 --output_directories=/nvme0/nicholso/data/sbm1000_0_6,/nvme13/nicholso/data/sbm1000_1_6,/nvme6/nicholso/data/sbm1000_2_6,/nvme28/nicholso/data/sbm1000_3_6,/nvme20/nicholso/data/sbm1000_4_6,/nvme24/nicholso/data/sbm1000_5_6
# 8 drives
./proteusadm-partition-ssb --input_directory=/scratch/data/ssbm1000 --output_directories=/nvme0/nicholso/data/sbm1000_0_8,/nvme13/nicholso/data/sbm1000_1_8,/nvme14/nicholso/data/sbm1000_2_8,/nvme6/nicholso/data/sbm1000_3_8,/nvme28/nicholso/data/sbm1000_4_8,/nvme20/nicholso/data/sbm1000_5_8,/nvme21/nicholso/data/sbm1000_6_8,/nvme24/nicholso/data/sbm1000_7_8
# 10 drives
./proteusadm-partition-ssb --input_directory=/scratch/data/ssbm1000 --output_directories=/nvme0/nicholso/data/sbm1000_0_10,/nvme7/nicholso/data/sbm1000_1_10,/nvme13/nicholso/data/sbm1000_2_10,/nvme14/nicholso/data/sbm1000_3_10,/nvme6/nicholso/data/sbm1000_4_10,/nvme28/nicholso/data/sbm1000_5_10,/nvme29/nicholso/data/sbm1000_6_10,/nvme20/nicholso/data/sbm1000_7_10,/nvme21/nicholso/data/sbm1000_8_10,/nvme24/nicholso/data/sbm1000_9_10
# 12 drives
./proteusadm-partition-ssb --input_directory=/scratch/data/ssbm1000 --output_directories=/nvme0/nicholso/data/sbm1000_0_12,/nvme7/nicholso/data/sbm1000_1_12,/nvme13/nicholso/data/sbm1000_2_12,/nvme14/nicholso/data/sbm1000_3_12,/nvme6/nicholso/data/sbm1000_4_12,/nvme9/nicholso/data/sbm1000_5_12,/nvme28/nicholso/data/sbm1000_6_12,/nvme29/nicholso/data/sbm1000_7_12,/nvme20/nicholso/data/sbm1000_8_12,/nvme21/nicholso/data/sbm1000_9_12,/nvme24/nicholso/data/sbm1000_10_12,/nvme25/nicholso/data/sbm1000_11_12
# 18 drives
./proteusadm-partition-ssb --input_directory=/scratch/data/ssbm1000 --output_directories=/nvme0/nicholso/data/sbm1000_0_18,/nvme7/nicholso/data/sbm1000_1_18,/nvme2/nicholso/data/sbm1000_2_18,/nvme13/nicholso/data/sbm1000_3_18,/nvme14/nicholso/data/sbm1000_4_18,/nvme15/nicholso/data/sbm1000_5_18,/nvme6/nicholso/data/sbm1000_6_18,/nvme9/nicholso/data/sbm1000_7_18,/nvme10/nicholso/data/sbm1000_8_18,/nvme28/nicholso/data/sbm1000_9_18,/nvme29/nicholso/data/sbm1000_10_18,/nvme30/nicholso/data/sbm1000_11_18,/nvme20/nicholso/data/sbm1000_12_18,/nvme21/nicholso/data/sbm1000_13_18,/nvme22/nicholso/data/sbm1000_14_18,/nvme24/nicholso/data/sbm1000_15_18,/nvme25/nicholso/data/sbm1000_16_18,/nvme26/nicholso/data/sbm1000_17_18
# 24 drives
./proteusadm-partition-ssb --input_directory=/scratch/data/ssbm1000 --output_directories=/nvme0/nicholso/data/sbm1000_0_24,/nvme7/nicholso/data/sbm1000_1_24,/nvme2/nicholso/data/sbm1000_2_24,/nvme3/nicholso/data/sbm1000_3_24,/nvme12/nicholso/data/sbm1000_4_24,/nvme13/nicholso/data/sbm1000_5_24,/nvme14/nicholso/data/sbm1000_6_24,/nvme15/nicholso/data/sbm1000_7_24,/nvme6/nicholso/data/sbm1000_8_24,/nvme9/nicholso/data/sbm1000_9_24,/nvme10/nicholso/data/sbm1000_10_24,/nvme11/nicholso/data/sbm1000_11_24,/nvme28/nicholso/data/sbm1000_12_24,/nvme29/nicholso/data/sbm1000_13_24,/nvme30/nicholso/data/sbm1000_14_24,/nvme31/nicholso/data/sbm1000_15_24,/nvme20/nicholso/data/sbm1000_16_24,/nvme21/nicholso/data/sbm1000_17_24,/nvme22/nicholso/data/sbm1000_18_24,/nvme23/nicholso/data/sbm1000_19_24,/nvme24/nicholso/data/sbm1000_20_24,/nvme25/nicholso/data/sbm1000_21_24,/nvme26/nicholso/data/sbm1000_22_24,/nvme27/nicholso/data/sbm1000_23_24
