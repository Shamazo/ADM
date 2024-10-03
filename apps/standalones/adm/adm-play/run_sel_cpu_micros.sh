#!/bin/bash

# Initialize our own variables
run_with_move=false
profile_amd=false
profile_perf=false
perf_events="cache-misses"
profile_perf_mem=false

# Parse command-line arguments
while getopts ":mapq" opt; do
  case ${opt} in
    m)
      run_with_move=true
      ;;
    a)
      profile_amd=true
      ;;
    p)
      profile_perf=true
      ;;
    q)
      profile_perf_mem=true
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      ;;
  esac
done

cd /tmp/tmp.YD2SgUVlV5/cmake-build-release/opt/pelago
bin_dir="/tmp/tmp.YD2SgUVlV5/cmake-build-release/opt/pelago/"
perf_archive_path="/tmp/tmp.YD2SgUVlV5/tools/profiling/perf_archive.sh"
setup_perf_ctrl_path="/tmp/tmp.YD2SgUVlV5/tools/profiling/setup_perf_ctrl.sh"

adm_output_dir="/scratch/nicholso/adm_output"
common_args="--scale_factor=1000 --server_number=49 --num_iterations=3"
# set a subdir for all results. e.g. for specific hardware settings
result_subdir="gmi_x8"

#selectivities=("0.0001" "0.0005" "0.001" "0.005" "0.01" "0.05" "0.1" "0.2" "0.3" "0.4"
#                "0.5" "0.6" "0.7" "0.8" "0.9" "1.0")

selectivities=("0.0001" "0.0005" "0.001" "0.005" "0.01" "0.05" "0.1")

#selectivities=("1.0")

base_output="${adm_output_dir}/${result_subdir}/"
if $profile_amd; then
  base_output="${adm_output_dir}/${result_subdir}_amd_pcm/"
fi

declare -a bench_output_dirs=(
  "${base_output}sel_cpu_pd_dop_4/"
  "${base_output}sel_cpu_pd_dop_max/"
  "${base_output}sel_cpu_pd_dop_24/"
  "${base_output}sel_cpu_baseline/"
  "${base_output}sel_cpu_stage_one/"
  "${base_output}sel_cpu_stage_both/"
  )

if $run_with_move; then
  # adding explicit memory move benchmarks (not always run to save time)
  bench_output_dirs+=("${base_output}sel_cpu_pd_memmove_dop_4/")
  bench_output_dirs+=("${base_output}sel_cpu_pd_memmove_dop_max/")
fi

# Use eval to interpret variables inside strings
for i in "${!bench_output_dirs[@]}"; do
  bench_output_dirs[$i]=$(eval echo "${bench_output_dirs[$i]}")
done



# specific arguments for each benchmark
declare -a bench_args=(
  "${common_args} --bench_micro_cpu_socket_pushdown_filter --pushdown_dop=4 --result_file={output_dir}sel_cpu_pd_dop_4.csv --timestamp_file={output_dir}sel_cpu_pd_dop_4_timestamps.csv"
  "${common_args} --bench_micro_cpu_socket_pushdown_filter --result_file={output_dir}sel_cpu_pd_dop_max.csv --timestamp_file={output_dir}sel_cpu_pd_dop_max_timestamps.csv" #with no --pushdown_dop, it defaults to all cores on storage node
  "${common_args} --bench_micro_cpu_socket_pushdown_filter --pushdown_dop=24 --result_file={output_dir}sel_cpu_pd_dop_24.csv --timestamp_file={output_dir}sel_cpu_pd_dop_24_timestamps.csv"
  "${common_args} --bench_micro_cpu_socket_pushdown_baseline --result_file={output_dir}sel_cpu_baseline.csv --timestamp_file={output_dir}sel_cpu_baseline_timestamps.csv"
  "${common_args} --bench_micro_cpu_socket_stage_one --result_file={output_dir}sel_cpu_stage_one.csv --timestamp_file={output_dir}sel_cpu_stage_one_timestamps.csv"
  "${common_args} --bench_micro_cpu_socket_stage_both --result_file={output_dir}sel_cpu_stage_both.csv --timestamp_file={output_dir}sel_cpu_stage_both_timestamps.csv"
  )

if $run_with_move; then
  # adding explicit memory move benchmarks (not always run to save time)
  bench_args+=("${common_args} --bench_micro_cpu_socket_pushdown_filter_memmove --pushdown_dop=4 --result_file={output_dir}sel_cpu_pd_memmove_dop_4.csv --timestamp_file={output_dir}sel_cpu_pd_memmove_dop_4_timestamps.csv")
  bench_args+=("${common_args} --bench_micro_cpu_socket_pushdown_filter_memmove --result_file={output_dir}sel_cpu_pd_memmove_dop_max.csv --timestamp_file={output_dir}sel_cpu_pd_memmove_dop_max_timestamps.csv")
fi


# Use eval to interpret variables inside strings

for i in "${!bench_args[@]}"; do
  bench_output_dir="${bench_output_dirs[$i]}"
  bench_args[$i]=$(eval echo "${bench_args[$i]}")
done

# Note, profiling parts most likely need updating to use --selectivity=${sel}
# see the default case
if $profile_amd; then
    for i in "${!bench_args[@]}"; do
      bench_output_dir="${bench_output_dirs[$i]}$(date +%Y-%m-%d-%H%M%S)"
      # Set the output directory for each benchmark with a string replacement
      bench_args="${bench_args[$i]//\{output_dir\}/${bench_output_dir}}"
      mkdir -p $bench_output_dir
      log_file="${bench_output_dir}log.txt"
      echo "sudo -E /opt/AMDuProf_4.1-424/bin/AMDuProfPcm -m xgmi,memory -a -A package 20   -s -o ${bench_output_dir}amd_pcm.csv  -k  -- bench_command.sh > ${log_file} 2>&1"
      echo "LD_LIBRARY_PATH=../lib:/scratch/pelago/llvm-14/opt/lib ${bin_dir}proteusadm-play ${bench_args} 2>&1" > bench_command.sh
      chmod +x bench_command.sh
# AMDuProfPcm needs sudo (unless using with perf backend which is broken for me)
      sudo -E /opt/AMDuProf_4.1-424/bin/AMDuProfPcm -r -m xgmi,memory,dc -a -A package -t 20  -s -o ${bench_output_dir}amd_pcm.csv -q -k  -- $PWD/bench_command.sh > ${log_file} 2>&1
      rm bench_command.sh
      sudo chmod -R 755 ${bench_output_dir}
# hardcoded to hamish
      sudo chown -R nicholso:DIAS-unit ${bench_output_dir}
    done
elif $profile_perf; then
  source ${setup_perf_ctrl_path}
  for i in "${!bench_args[@]}"; do
    rm -f ${bin_dir}/generated_code/*
    bench_output_dir="${bench_output_dirs[$i]}$(date +%Y-%m-%d-%H%M%S)/"
    # Set the output directory for each benchmark with a string replacement
    bench_args="${bench_args[$i]//\{output_dir\}/${bench_output_dir}}"
    mkdir -p $bench_output_dir
    log_file="${bench_output_dir}log.txt"
    for sel in "${selectivities[@]}"; do
      echo "perf record -k 1 --delay=-1 --control fd:${PERF_CTL_FD},${PERF_CTL_ACK_FD} -e ${perf_events} --sample-cpu -o /tmp/perf.data.${sel} -- ./proteusadm-play ${bench_args} --selectivity=${sel} > ${log_file} 2>&1"
      perf record -k 1 --delay=-1 --control fd:${PERF_CTL_FD},${PERF_CTL_ACK_FD} -e ${perf_events} --sample-cpu -o /tmp/perf.data.${sel} -- ./proteusadm-play ${bench_args} --selectivity=${sel} > ${log_file} 2>&1
      perf inject -j -i /tmp/perf.data.${sel} -o perf.data.jit.${sel}
      ${perf_archive_path} --all perf.data.jit.${sel}
      mv *.tar.gz ${bench_output_dir}
      cp -r ./generated_code ${bench_output_dir}
    done
  done
elif $profile_perf_mem; then
      bench_output_dir="${bench_output_dirs[$i]}$(date +%Y-%m-%d-%H%M%S)/"
      # Set the output directory for each benchmark with a string replacement
      bench_args="${bench_args[$i]//\{output_dir\}/${bench_output_dir}}"
      source ${setup_perf_ctrl_path}
      rm -f ${bin_dir}/generated_code/*
      mkdir -p $bench_output_dir
      log_file="${bench_output_dir}log.txt"
      echo "perf mem record -k 1 --delay=-1 --control fd:${PERF_CTL_FD},${PERF_CTL_ACK_FD} -o perf.data -- ./proteusadm-play ${bench_args} > ${log_file} 2>&1"
      perf mem record  --delay=-1 --control fd:${PERF_CTL_FD},${PERF_CTL_ACK_FD} -o perf.data -- ./proteusadm-play ${bench_args} > ${log_file} 2>&1
#      perf inject -j -i perf.data -o perf.data.jit && rm perf.data
#      ${perf_archive_path} --all perf.data.jit
#      mv *.tar.gz ${bench_output_dir}
      cp -r ./generated_code ${bench_output_dir}
else
  for i in "${!bench_args[@]}"; do
    rm -f ${bin_dir}/generated_code/*
    bench_output_dir="${bench_output_dirs[$i]}$(date +%Y-%m-%d-%H%M%S)/"
    # Set the output directory for each benchmark with a string replacement
    bench_args="${bench_args[$i]//\{output_dir\}/${bench_output_dir}}"
    mkdir -p $bench_output_dir
    log_file="${bench_output_dir}log.txt"
    for sel in "${selectivities[@]}"; do
      echo "Running ./proteusadm-play ${bench_args} --selectivity=${sel} >> ${log_file} 2>&1" | tee -a ${log_file}
      ./proteusadm-play ${bench_args} --selectivity=${sel} >> ${log_file} 2>&1
      cp -r ./generated_code ${bench_output_dir}
      sleep 10s
    done
    sleep 30s
  done
fi
