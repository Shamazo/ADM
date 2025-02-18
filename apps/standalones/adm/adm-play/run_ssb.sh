#!/bin/bash

cd /tmp/tmp/tmp.fcZC0ZT5Sl/cmake-build-release-dias46/opt/pelago

#echo "running ssb uncompressed"
#./proteusadm-play --bench_varybw_cpu_ssb --scale_factor=1000 --server_number=46 --result_file=ssb_bw.csv

echo "running ssb compressed"
./proteusadm-play --bench_varybw_cpu_ssb_compressed --scale_factor=1000 --server_number=46 --result_file=ssb_compressed_bw.csv
