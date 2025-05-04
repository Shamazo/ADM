#!/bin/bash

# Needs to be manually set depending where proteus is built
cd /tmp/tmp.YD2SgUVlV5/cmake-build-debug/opt/pelago
set -e

## 12 drives
./proteusadm-partition-ssb --input_directory=/nvme31/nicholso/data/taxi --output_directories=/nvme16/nicholso/data/taxi_0_12,/nvme22/nicholso/data/taxi_1_12,/nvme29/nicholso/data/taxi_2_12,/nvme24/nicholso/data/taxi_3_12,/nvme18/nicholso/data/taxi_4_12,/nvme23/nicholso/data/taxi_5_12,/nvme30/nicholso/data/taxi_6_12,/nvme25/nicholso/data/taxi_7_12,/nvme19/nicholso/data/taxi_8_12,/nvme28/nicholso/data/taxi_9_12,/nvme26/nicholso/data/taxi_10_12,/nvme27/nicholso/data/taxi_11_12

#          "/nvme16/nicholso/data/sbm1000_0_12",   // node 4
#          "/nvme22/nicholso/data/sbm1000_1_12",   // node 5
#          "/nvme29/nicholso/data/sbm1000_2_12",   // node 6
#          "/nvme24/nicholso/data/sbm1000_3_12",   // node 7
#          "/nvme18/nicholso/data/sbm1000_4_12",   // node 4
#          "/nvme23/nicholso/data/sbm1000_5_12",   // node 5
#          "/nvme30/nicholso/data/sbm1000_6_12",   // node 6
#          "/nvme25/nicholso/data/sbm1000_7_12",   // node 7 
#          "/nvme19/nicholso/data/sbm1000_8_12",   // node 4
#          "/nvme28/nicholso/data/sbm1000_9_12",   // node 6
#          "/nvme26/nicholso/data/sbm1000_10_12",  // node 7
#          "/nvme27/nicholso/data/sbm1000_11_12"   // node 7