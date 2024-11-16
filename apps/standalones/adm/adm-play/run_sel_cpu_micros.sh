#!/bin/bash

# Initialize our own variables
run_with_move=false
profile_amd=false
profile_perf=false
perf_events="cpu-cycles"
profile_perf_mem=false
flame_graph=false
off_cpu_flame_graph=false

create_trace() {
#  assumes conda env is setup
  local outdir=$1
  conda run -n profiling --live-stream python3 /tmp/tmp.YD2SgUVlV5/tools/tracing/cli.py . out.trace
  if [ $? -eq 0 ]; then
    pigz out.trace
    mv out.trace.gz $outdir
    rm timeline*
  else
    echo "Failed to create trace"
  fi
}

# Parse command-line arguments
while getopts ":mapfo" opt; do
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
    o)
      off_cpu_flame_graph=true
      ;;
    f)
      flame_graph=true
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
flame_graph_path="/home/nicholso/FlameGraph"

adm_output_dir="/scratch/nicholso/adm_output"
common_args="--server_number=49 --num_iterations=3 --timestamp_file=adm-timestamps.csv"
# set a subdir for all results. e.g. for specific hardware settings
result_subdir="gmi_x8_noavx"

base_output="${adm_output_dir}/${result_subdir}/$(date +%Y-%m-%d-%H%M)"
if $profile_amd; then
  base_output="${adm_output_dir}/${result_subdir}_amd_pcm/"
fi

declare -a bench_output_dirs=(
  "${base_output}sel_cpu_pd/"
  "${base_output}sel_cpu_pd/"
#  "${base_output}sel_cpu_pd/"
#  "${base_output}sel_cpu_pd/"
#  "${base_output}sel_cpu_pd/"
#  "${base_output}sel_cpu_pd/"
#  "${base_output}sel_cpu_pd/"
#  "${base_output}sel_cpu_pd_dop_max/"
  "${base_output}sel_cpu_baseline/"
#  "${base_output}sel_cpu_stage_one/"
  "${base_output}sel_cpu_stage_both/"
  "${base_output}sel_cpu_adaptive/"
  "${base_output}sel_cpu_adaptive/"
#  "${base_output}sel_cpu_adaptive/"
#  "${base_output}sel_cpu_adaptive/"
#  "${base_output}sel_cpu_adaptive/"
#  "${base_output}sel_cpu_adaptive/"
#"${base_output}sel_cpu_grouter_staging/"
"${base_output}sel_cpu_grouter_pd/"
"${base_output}sel_cpu_grouter_pd/"
#"${base_output}sel_cpu_pd/"
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
#  "${common_args} --bench_micro_cpu_socket_pushdown_filter --pushdown_dop=1 --result_file={output_dir}sel_cpu_pd_dop_1.csv "
#  "${common_args} --bench_micro_cpu_socket_pushdown_filter --pushdown_dop=2 --result_file={output_dir}sel_cpu_pd_dop_2.csv "
  "${common_args} --bench_micro_cpu_socket_pushdown_filter --pushdown_dop=4 --result_file={output_dir}sel_cpu_pd_dop_4.csv "
#  "${common_args} --bench_micro_cpu_socket_pushdown_filter --pushdown_dop=8 --result_file={output_dir}sel_cpu_pd_dop_8.csv "
#  "${common_args} --bench_micro_cpu_socket_pushdown_filter --pushdown_dop=12 --result_file={output_dir}sel_cpu_pd_dop_12.csv "
  "${common_args} --bench_micro_cpu_socket_pushdown_filter --pushdown_dop=16 --result_file={output_dir}sel_cpu_pd_dop_16.csv "
#  "${common_args} --bench_micro_cpu_socket_pushdown_filter --pushdown_dop=24 --result_file={output_dir}sel_cpu_pd_dop_24.csv "
  #  "${common_args} --bench_micro_cpu_socket_pushdown_filter --result_file={output_dir}sel_cpu_pd_dop_max.csv " #with no --pushdown_dop, it defaults to all cores on storage node
  "${common_args} --bench_micro_cpu_socket_pushdown_baseline --result_file={output_dir}sel_cpu_baseline.csv "
#  "${common_args} --bench_micro_cpu_socket_stage_one --result_file={output_dir}sel_cpu_stage_one.csv "
  "${common_args} --bench_micro_cpu_socket_stage_both --result_file={output_dir}sel_cpu_stage_both.csv "
#  "${common_args} --bench_micro_cpu_socket_adaptive --pushdown_dop=1 --result_file={output_dir}sel_cpu_adaptive.csv "
#  "${common_args} --bench_micro_cpu_socket_adaptive --pushdown_dop=2 --result_file={output_dir}sel_cpu_adaptive.csv "
  "${common_args} --bench_micro_cpu_socket_adaptive --pushdown_dop=4 --result_file={output_dir}sel_cpu_adaptive.csv "
#  "${common_args} --bench_micro_cpu_socket_adaptive --pushdown_dop=8 --result_file={output_dir}sel_cpu_adaptive.csv "
#  "${common_args} --bench_micro_cpu_socket_adaptive --pushdown_dop=12 --result_file={output_dir}sel_cpu_adaptive.csv "
  "${common_args} --bench_micro_cpu_socket_adaptive --pushdown_dop=16 --result_file={output_dir}sel_cpu_adaptive.csv "
#  "${common_args} --bench_micro_cpu_socket_adaptive --pushdown_dop=24 --result_file={output_dir}sel_cpu_adaptive.csv "
#"${common_args} --bench_micro_cpu_socket_grouter_stage_both --result_file={output_dir}sel_cpu_grouter_pd.csv "
"${common_args} --bench_micro_cpu_socket_grouter_pd --pushdown_dop=4 --result_file={output_dir}sel_cpu_grouter_pd.csv "
"${common_args} --bench_micro_cpu_socket_grouter_pd --pushdown_dop=16 --result_file={output_dir}sel_cpu_grouter_pd.csv "
#  "${common_args} --bench_micro_cpu_socket_pushdown_filter --pushdown_dop=4 --result_file={output_dir}sel_cpu_pd_dop_4.csv "
  )

if $run_with_move; then
  # adding explicit memory move benchmarks (not always run to save time)
  bench_args+=("${common_args} --bench_micro_cpu_socket_pushdown_filter_memmove --pushdown_dop=4 --result_file={output_dir}sel_cpu_pd_memmove_dop_4.csv ")
  bench_args+=("${common_args} --bench_micro_cpu_socket_pushdown_filter_memmove --result_file={output_dir}sel_cpu_pd_memmove_dop_max.csv ")
fi


# Use eval to interpret variables inside strings

for i in "${!bench_args[@]}"; do
  bench_output_dir="${bench_output_dirs[$i]}"
  bench_args[$i]=$(eval echo "${bench_args[$i]}")
done

if $profile_amd; then
    for i in "${!bench_args[@]}"; do
      bench_output_dir="${bench_output_dirs[$i]}"
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
    bench_output_dir="${bench_output_dirs[$i]}/"
    # Set the output directory for each benchmark with a string replacement
    bench_args="${bench_args[$i]//\{output_dir\}/${bench_output_dir}}"
    mkdir -p $bench_output_dir
    log_file="${bench_output_dir}log.txt"
      echo "perf record -k 1 --delay=-1 --control fd:${PERF_CTL_FD},${PERF_CTL_ACK_FD} -e ${perf_events} --sample-cpu -o /tmp/perf.data -- ./selectivity-micros ${bench_args} > ${log_file} 2>&1"
      perf record -k 1 --delay=-1 --control fd:${PERF_CTL_FD},${PERF_CTL_ACK_FD} -e ${perf_events} --sample-cpu -o /tmp/perf.data -- ./selectivity-micros ${bench_args} > ${log_file} 2>&1
      perf inject -j -i /tmp/perf.data -o perf.data.jit
      ${perf_archive_path} --all perf.data.jit
      mv *.tar.gz ${bench_output_dir}
      cp -r ./generated_code ${bench_output_dir}
  done
elif $flame_graph; then
  source ${setup_perf_ctrl_path}
  for i in "${!bench_args[@]}"; do
    rm -f ${bin_dir}/generated_code/*
    bench_output_dir="${bench_output_dirs[$i]}/"
    # Set the output directory for each benchmark with a string replacement
    bench_args="${bench_args[$i]//\{output_dir\}/${bench_output_dir}}"
    mkdir -p $bench_output_dir
    log_file="${bench_output_dir}log.txt"
      echo "perf record -k 1 --delay=-1 --control fd:${PERF_CTL_FD},${PERF_CTL_ACK_FD} -F 199 -e cpu-clock --call-graph dwarf -o /tmp/perf.data -- ./selectivity-micros ${bench_args} > ${log_file} 2>&1"
      perf record -k 1 --delay=-1 --control fd:${PERF_CTL_FD},${PERF_CTL_ACK_FD} -F 199 -e cpu-clock --call-graph dwarf -o /tmp/perf.data -- ./selectivity-micros ${bench_args} > ${log_file} 2>&1
      perf inject -j -i /tmp/perf.data -o perf.data.jit
      perf script -i perf.data.jit | ${flame_graph_path}/stackcollapse-perf.pl | ${flame_graph_path}/flamegraph.pl --width 2400 --subtitle "./selectivity-micros ${bench_args}" > flame_graph.svg
      sleep 1s
      cp *.svg ${bench_output_dir}/
      create_trace ${bench_output_dir}
      cp *.csv ${bench_output_dir}/
      rm /tmp/perf.data
      rm perf.data.jit
      cp -r ./generated_code ${bench_output_dir}
done
elif $off_cpu_flame_graph; then
  for i in "${!bench_args[@]}"; do
    rm -f ${bin_dir}/generated_code/*
    bench_output_dir="${bench_output_dirs[$i]}/"
    # Set the output directory for each benchmark with a string replacement
    bench_args="${bench_args[$i]//\{output_dir\}/${bench_output_dir}}"
    mkdir -p $bench_output_dir
    log_file="${bench_output_dir}log.txt"
      echo "./selectivity-micros ${bench_args} --exit_loop > ${log_file} 2>&1"
      ./selectivity-micros ${bench_args} --exit-exit_loop > ${log_file} 2>&1 & pid=$!
      echo "Profiling off-CPU time for PID $pid"
      sudo offcputime-bpfcc -df -p $pid > off_cpu.stacks 2>/dev/null &
      # Wait for benchmark to complete
      sleep 100 # NOTE this is hard coded to how long we expect the particular benchmark to run
      profiler_pid=$(pgrep -f "/usr/sbin/offcputime-bpfcc")
      echo "Profiler PID: $profiler_pid"
      # Send SIGINT to offcputime-bpfcc to stop it before proteus so that it can resolve symbols
      echo "Stopping offcputime-bpfcc $profiler_pid"
      sudo kill -SIGINT "$profiler_pid"
      while [ -e /proc/$profiler_pid ]; do sleep 1; done
      # then kill proteus, which is expected to wait in a do-while loop until it is done
      echo "stopping proteus $pid"
      kill -SIGKILL $pid
      wait "$pid"
      ${flame_graph_path}/flamegraph.pl --color=io --title="Off-CPU Time Flame Graph" --countname=us --width 2400 --subtitle "off-cpu ./selectivity-micros ${bench_args}" < off_cpu.stacks > flame_graph_off_cpu.svg
      cp *.svg ${bench_output_dir}/
#      rm off_cpu.stacks
      cp -r ./generated_code ${bench_output_dir}
done
else
  for i in "${!bench_args[@]}"; do
    rm -f ${bin_dir}/generated_code/*
    bench_output_dir="${bench_output_dirs[$i]}/"
    # Set the output directory for each benchmark with a string replacement
    bench_args="${bench_args[$i]//\{output_dir\}/${bench_output_dir}}"
    mkdir -p $bench_output_dir
    log_file="${bench_output_dir}log.txt"
      echo "Running ./selectivity-micros ${bench_args} >> ${log_file} 2>&1" | tee -a ${log_file}
      ./selectivity-micros ${bench_args} >> ${log_file} 2>&1
      sleep 1s
      cp -r ./generated_code ${bench_output_dir}
      create_trace ${bench_output_dir}
      cp *.csv ${bench_output_dir}
      sleep 30s
  done
fi
