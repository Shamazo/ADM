#!/bin/bash

cd /tmp/tmp/tmp.fcZC0ZT5Sl/cmake-build-release-dias46/opt/pelago

echo "running scan uncompressed"
./proteusadm-play --bench_varybw_cpu --scale_factor=1000 --server_number=46 --result_file=scan_bw.csv

echo "running scan compressed"
./proteusadm-play --bench_varybw_cpu_compressed --scale_factor=1000 --server_number=46 --result_file=scan_compressed_bw.csv
