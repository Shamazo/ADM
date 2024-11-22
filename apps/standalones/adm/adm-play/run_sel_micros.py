#!/usr/bin/env python3

import argparse
import logging
import os
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Optional
import json
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import traceback


@dataclass
class BenchmarkConfig:
    """Configuration for a single benchmark run."""
    shortname: str
    args: str
    shortname_with_args: Optional[str] = None


class SlackThread:
    def __init__(self, initial_message: str):
        self.slack_client = None
        self.slack_thread_ts = None
        self.channel_id = None
        if os.environ.get('SLACK_TOKEN'):
            self.slack_client = WebClient(token=os.environ['SLACK_TOKEN'])
            self.channel_id = self.get_channel_id("adm-reports")
            thread_parent = self.slack_client.chat_postMessage(
                channel=self.channel_id,
                text=f"{initial_message}"
            )
            self.slack_thread_ts = thread_parent['ts']

    def get_channel_id(self, channel_name: str):
        """
        Get channel ID from channel name

        Args:
            channel_name (str): Channel name without # (e.g. 'general')
        Returns:
            str: Channel ID
        """
        # List all channels
        result = self.slack_client.conversations_list(types='private_channel')
        for channel in result["channels"]:
            if channel["name"] == channel_name:
                return channel["id"]

        raise ValueError(f"Channel {channel_name} not found")

    def send_slack_message(self, message: str, to_channel: bool = False):
        """Send a message to the existing slack thread."""
        if self.slack_client is not None:
            try:
                self.slack_client.chat_postMessage(
                    channel=self.channel_id,
                    text=message,
                    thread_ts=self.slack_thread_ts,
                    reply_broadcast=to_channel
                )
            except SlackApiError as e:
                print(f"Error sending message to slack: {e}")

    def send_file(self, message: str, file_path: str):
        """Send a file to the existing slack thread."""
        if self.slack_client is not None:
            try:
                response = self.slack_client.files_upload_v2(
                    channel=self.channel_id,
                    file=file_path,
                    title=Path(file_path).name,
                    thread_ts=self.slack_thread_ts,
                    initial_comment=f"f{file_path}"
                )
                permalink = response['files'][0]['permalink']
                self.send_slack_message(f"{message} \n {permalink}")
            except SlackApiError as e:
                print(f"Error uploading file to slack: {e}")


class BenchmarkRunner:
    def __init__(self, args, benchmarks: List[BenchmarkConfig]):
        self.args = args
        self.benchmarks = benchmarks
        self.common_args = f"--server_number=49 --num_iterations={args.num_iterations} --timestamp_file=adm-timestamps.csv {'--selectivities=' + args.selectivities if args.selectivities is not None else ''}"
        self.result_subdir = "gmi_x8_noavx"
        self.perf_events = "cpu-cycles"

        # Set base output directory
        timestamp = datetime.now().strftime("%Y-%m-%d-%H%M")
        self.base_output = self.args.output_dir / self.result_subdir / timestamp
        os.makedirs(self.base_output, exist_ok=True)
        self.slack_thread = SlackThread(
            f":weight_lifter: running selectivity micros :weight_lifter: \n Result directory will be {self.base_output}")

    def get_benchmark_output_dir(self, benchmark: BenchmarkConfig) -> Path:
        """Construct full output path for a benchmark."""
        return self.base_output / benchmark.shortname

    def get_benchmark_result_filepath(self, benchmark: BenchmarkConfig, output_dir: Path) -> str:
        file_name = (benchmark.shortname_with_args if benchmark.shortname_with_args else benchmark.shortname) + ".csv"
        return f"{output_dir}/{file_name}"

    def get_benchmark_args(self, benchmark: BenchmarkConfig, output_dir: Path) -> str:
        """Construct full argument string for a benchmark."""
        return f"{self.common_args} {benchmark.args} --result_file={self.get_benchmark_result_filepath(benchmark, output_dir)}"

    def start_iostat_collection(self) -> subprocess.Popen:
        """Start iostat collection process."""
        # run iostat once to clear any previous data
        os.environ["S_TIME_FORMAT"] = "ISO"
        subprocess.run(['iostat'], check=True)
        iostat_output = "iostat_output.json"
        return subprocess.Popen(
            ['iostat', '-xm', '1', '-o', 'JSON', '-t'],
            stdout=open(iostat_output, 'w'),
            stderr=subprocess.PIPE,
            env=os.environ
        )

    def parse_iostat_and_check_health(self, iostat_output: Path = "iostat_output.json") -> None:
        with open(iostat_output, 'r') as f:
            try:
                iostat_data = json.load(f)
            except Exception as E:
                logging.warning(f"Failed to parse io stat json from {iostat_output} with {E}")
                return

        have_logged_already = set()
        for entry in iostat_data['sysstat']['hosts'][0]['statistics']:
            ts = datetime.fromisoformat(entry['timestamp'])
            for disk in entry['disk']:
                if float(disk['r_await']) > 60.0 and disk['disk_device'] not in have_logged_already:
                    have_logged_already.add(disk['disk_device'])
                    logging.error("=" * 80)
                    logging.error(f"Disk {disk['disk_device']} read await time is high at {disk['r_await']} ms at {ts}")
                    logging.error("=" * 80)
                    self.slack_thread.send_slack_message(
                        f" {':warning:' * 10} \n Disk {disk['disk_device']} read await time is high at {disk['r_await']} ms at {ts} \n {':warning:' * 10}",
                        to_channel=True)

    def create_trace(self, outdir: Path, trace_name: str = "out.trace") -> None:
        """Create a trace file using conda."""
        try:
            logging.info(f"Creating trace: {trace_name}")
            subprocess.run(
                ["python3", "/tmp/tmp.YD2SgUVlV5/tools/tracing/cli.py", ".", f"{trace_name}"],
                check=True,
                capture_output=True,
                text=True
            )
            subprocess.run(["pigz", f"{trace_name}"], check=True, capture_output=True, text=True)
            subprocess.run(["mv", f"{trace_name}.gz", str(outdir)], check=True, capture_output=True, text=True)
            subprocess.run(f"rm -f timeline*", shell=True)
        except subprocess.CalledProcessError as e:
            logging.error(f"Failed to create trace: {str(e)}")
            logging.error(f"Command output: {e.output}")
            logging.error(f"Command stderr: {e.stderr}")
            if e.returncode:
                logging.error(f"Return code: {e.returncode}")

    def setup_perf_env(self) -> Tuple[int, int]:
        """Setup perf control FIFOs and file descriptors."""
        logging.info("Setting up perf environment")

        # Define FIFO paths
        ctl_fifo = Path("/tmp/perf_ctl_fd.fifo")
        ack_fifo = Path("/tmp/perf_ctl_fd_ack.fifo")

        # Remove existing FIFOs if they exist
        for fifo in [ctl_fifo, ack_fifo]:
            if fifo.exists():
                fifo.unlink()

        # Create new FIFOs
        for fifo in [ctl_fifo, ack_fifo]:
            os.mkfifo(str(fifo))

        # Open FIFOs and get file descriptors
        ctl_fd = os.open(str(ctl_fifo), os.O_RDWR)
        ack_fd = os.open(str(ack_fifo), os.O_RDWR)

        # Set environment variables
        os.environ["PERF_CTL_FD"] = str(ctl_fd)
        os.environ["PERF_CTL_ACK_FD"] = str(ack_fd)

        logging.info(f"Set: PERF_CTL_FD={ctl_fd} and PERF_CTL_ACK_FD={ack_fd}")

        return ctl_fd, ack_fd

    def run_amd_profile(self):
        """Run benchmarks with AMD profiling."""
        for bench in self.benchmarks:
            output_dir = self.get_benchmark_output_dir(bench)
            os.makedirs(output_dir, exist_ok=True)
            log_file = output_dir / f"{bench.shortname_with_args if bench.shortname_with_args else bench.shortname}_log.txt"

            # Create benchmark command script
            with open("bench_command.sh", "w") as f:
                cmd = f"LD_LIBRARY_PATH=../lib:/scratch/pelago/llvm-14/opt/lib {self.args.bin_dir}/proteusadm-play {self.get_benchmark_args(bench, output_dir)}"
                f.write(cmd)
            os.chmod("bench_command.sh", 0o755)

            logging.info(f"Running AMD profile for benchmark: {bench.shortname}")
            # Run AMD profiler
            try:
                subprocess.run([
                    "sudo", "-E", "/opt/AMDuProf_4.1-424/bin/AMDuProfPcm",
                    "-r", "-m", "xgmi,memory,dc", "-a", "-A", "package", "-t", "20",
                    "-s", "-o", str(output_dir / "amd_pcm.csv"),
                    "-q", "-k", "--", f"{os.getcwd()}/bench_command.sh"
                ], stdout=open(log_file, "w"), stderr=subprocess.STDOUT, check=True)
            except subprocess.CalledProcessError as e:
                logging.error(f"AMD profiling failed: {e}")
                continue

            os.remove("bench_command.sh")
            subprocess.run(["sudo", "chmod", "-R", "755", str(output_dir)])
            subprocess.run(["sudo", "chown", "-R", "nicholso:DIAS-unit", str(output_dir)])

    def run_perf_profile(self):
        """Run benchmarks with perf profiling."""
        ctl_fd, ack_fd = self.setup_perf_env()

        try:
            for bench in self.benchmarks:
                output_dir = self.get_benchmark_output_dir(bench)
                os.makedirs(output_dir, exist_ok=True)
                log_file = output_dir / f"{bench.shortname_with_args if bench.shortname_with_args else bench.shortname}_log.txt"

                # Clear generated code
                subprocess.run(f"rm -f {self.args.bin_dir}/generated_code/*", shell=True)

                benchmark_args = self.get_benchmark_args(bench, output_dir)
                logging.info(f"Running perf profile for benchmark: {bench.shortname}")

                try:
                    # Record perf data
                    subprocess.run([
                                       "perf", "record", "-k", "1", "--delay=-1",
                                       "--control", f"fd:{ctl_fd},{ack_fd}",
                                       "-e", self.perf_events, "--sample-cpu",
                                       "-o", "/tmp/perf.data", "--",
                                       "./selectivity-micros"
                                   ] + benchmark_args.split(),
                                   stdout=open(log_file, "w"),
                                   stderr=subprocess.STDOUT,
                                   check=True,
                                   pass_fds=(ctl_fd, ack_fd),
                                   env=os.environ)  # Pass the environment with our FD variables

                    # Process perf data
                    subprocess.run(["perf", "inject", "-j", "-i", "/tmp/perf.data", "-o", "perf.data.jit"], check=True)
                    subprocess.run([str(self.args.perf_archive_path), "--all", "perf.data.jit"], check=True)

                    # Move results
                    self.create_trace(output_dir,
                                      f"{bench.shortname_with_args if bench.shortname_with_args else bench.shortname}.trace")
                    subprocess.run(f"cp *.csv {output_dir}", shell=True, check=True)
                    subprocess.run(["cp", "-r", "./generated_code", str(output_dir)], check=True)

                except subprocess.CalledProcessError as e:
                    logging.error(f"Perf profiling failed: {e}")
                    continue
        finally:
            # Cleanup
            for fd in [ctl_fd, ack_fd]:
                try:
                    os.close(fd)
                except OSError as e:
                    logging.error(f"Error closing file descriptor {fd}: {e}")

            for fifo in ["/tmp/perf_ctl_fd.fifo", "/tmp/perf_ctl_fd_ack.fifo"]:
                try:
                    Path(fifo).unlink()
                except OSError as e:
                    logging.error(f"Error removing FIFO {fifo}: {e}")

    def run_flame_graph(self):
        """Run benchmarks and generate flame graphs."""
        ctl_fd, ack_fd = self.setup_perf_env()

        for bench in self.benchmarks:
            output_dir = self.get_benchmark_output_dir(bench)
            os.makedirs(output_dir, exist_ok=True)
            log_file = output_dir / f"{bench.shortname_with_args if bench.shortname_with_args else bench.shortname}_log.txt"

            # Clear generated code
            subprocess.run(f"rm -f {self.args.bin_dir}/generated_code/*", shell=True)

            benchmark_args = self.get_benchmark_args(bench, output_dir)
            logging.info(f"Generating flame graph for benchmark: {bench.shortname}")

            try:
                # Record perf data
                cmd = [
                          "perf", "record", "-k", "1", "--delay=-1",
                          "--control", f"fd:{ctl_fd},{ack_fd}",
                          "-F", "199", "-e", "cpu-clock", "--call-graph", "dwarf",
                          "-o", "/tmp/perf.data", "--",
                          "./selectivity-micros"
                      ] + benchmark_args.split()

                iostat_process = self.start_iostat_collection()
                logging.debug(f"Running command: {' '.join(cmd)}")
                subprocess.run(
                    cmd,
                    stdout=open(log_file, "w"),
                    pass_fds=(ctl_fd, ack_fd),
                    stderr=subprocess.STDOUT,
                    check=True,
                    env=os.environ)
                iostat_process.send_signal(subprocess.signal.SIGINT)
                time.sleep(1)
                iostat_process.terminate()
                self.parse_iostat_and_check_health()

                # Process perf data and generate flame graph
                subprocess.run(["perf", "inject", "-j", "-i", "/tmp/perf.data", "-o", "perf.data.jit"], check=True)

                with open(
                        f"flame_graph_{bench.shortname_with_args if bench.shortname_with_args else bench.shortname}.svg",
                        "w") as f:
                    perf_script = subprocess.Popen(["perf", "script", "-i", "perf.data.jit"], stdout=subprocess.PIPE)
                    collapse = subprocess.Popen([f"{self.args.flame_graph_path}/stackcollapse-perf.pl"],
                                                stdin=perf_script.stdout, stdout=subprocess.PIPE)
                    flamegraph = subprocess.Popen([f"{self.args.flame_graph_path}/flamegraph.pl",
                                                   "--width", "2400",
                                                   "--subtitle", f"./selectivity-micros {benchmark_args}"],
                                                  stdin=collapse.stdout, stdout=f)
                    flamegraph.wait()

                time.sleep(1)
                self.slack_thread.send_file(
                    f"{bench.shortname_with_args if bench.shortname_with_args else bench.shortname} (flame graph) results",
                    self.get_benchmark_result_filepath(bench, output_dir))

                for file in Path().glob("*.svg"):
                    self.slack_thread.send_file(f"Flame graph for {bench.shortname} ({benchmark_args}) ", str(file))
                    subprocess.run(["mv", str(file), str(output_dir)], check=True)

                self.create_trace(output_dir,
                                  f"{bench.shortname_with_args if bench.shortname_with_args else bench.shortname}.trace")
                subprocess.run(f"cp *.csv {output_dir}", shell=True, check=True)
                subprocess.run(["cp", "-r", "./generated_code", str(output_dir)], check=True)

                # Cleanup
                os.remove("/tmp/perf.data")
                os.remove("perf.data.jit")

            except subprocess.CalledProcessError as e:
                logging.error(f"Flame graph generation failed: {e}")
                continue

        # Cleanup
        for fd in [ctl_fd, ack_fd]:
            try:
                os.close(fd)
            except OSError as e:
                logging.error(f"Error closing file descriptor {fd}: {e}")

        for fifo in ["/tmp/perf_ctl_fd.fifo", "/tmp/perf_ctl_fd_ack.fifo"]:
            try:
                Path(fifo).unlink()
            except OSError as e:
                logging.error(f"Error removing FIFO {fifo}: {e}")


    def run_off_cpu_flame_graph(self):
        """Run benchmarks and generate off-CPU flame graphs."""
        for bench in self.benchmarks:
            output_dir = self.get_benchmark_output_dir(bench)
            os.makedirs(output_dir, exist_ok=True)
            log_file = output_dir / f"{bench.shortname_with_args if bench.shortname_with_args else bench.shortname}_log.txt"

            # Clear generated code
            subprocess.run(f"rm -f {self.args.bin_dir}/generated_code/*", shell=True)

            benchmark_args = self.get_benchmark_args(bench, output_dir)
            logging.info(f"Generating off-CPU flame graph for benchmark: {bench.shortname}")

            try:
                # Start the benchmark process
                benchmark_proc = subprocess.Popen(
                    ["./selectivity-micros"] + f"{benchmark_args} --exit-exit_loop".split(),
                    stdout=open(log_file, "a"),
                    stderr=subprocess.STDOUT
                )

                pid = benchmark_proc.pid
                logging.info(f"Profiling off-CPU time for PID {pid}")

                # Start the profiler
                profiler = subprocess.Popen(["sudo", "offcputime-bpfcc", "-df", "-p", str(pid)],
                                            stdout=open("off_cpu.stacks", "w"),
                                            stderr=subprocess.DEVNULL)

                # Wait for benchmark
                time.sleep(100)  # Hard-coded wait time as per original script

                # Stop profiler
                profiler.terminate()
                profiler.wait()

                # Stop benchmark
                benchmark_proc.kill()
                benchmark_proc.wait()

                # Generate flame graph
                with open("flame_graph_off_cpu.svg", "w") as f:
                    subprocess.run([
                        f"{self.args.flame_graph_path}/flamegraph.pl",
                        "--color=io",
                        "--title=Off-CPU Time Flame Graph",
                        "--countname=us",
                        "--width", "2400",
                        "--subtitle", f"off-cpu ./selectivity-micros {benchmark_args}"
                    ], stdin=open("off_cpu.stacks"), stdout=f, check=True)

                # Copy results
                for file in Path().glob("*.svg"):
                    subprocess.run(["cp", str(file), str(output_dir)], check=True)
                subprocess.run(["cp", "-r", "./generated_code", str(output_dir)], check=True)

            except subprocess.CalledProcessError as e:
                logging.error(f"Off-CPU flame graph generation failed: {e}")
                continue
            except Exception as e:
                logging.error(f"Unexpected error during off-CPU profiling: {e}")
                continue


    def run_standard_benchmark(self):
        """Run benchmarks without profiling."""
        for bench in self.benchmarks:
            output_dir = self.get_benchmark_output_dir(bench)
            os.makedirs(output_dir, exist_ok=True)
            log_file = output_dir / f"{bench.shortname_with_args if bench.shortname_with_args else bench.shortname}_log.txt"

            # Clear generated code
            subprocess.run(f"rm -f {self.args.bin_dir}/generated_code/*", shell=True)

            benchmark_args = self.get_benchmark_args(bench, output_dir)
            logging.info(f"Running benchmark: {bench.shortname}")
            logging.debug(f"Command: ./selectivity-micros {benchmark_args}")

            try:
                iostat_process = self.start_iostat_collection()
                subprocess.run(
                    ["./selectivity-micros"] + benchmark_args.split(),
                    stdout=open(log_file, "a"),
                    stderr=subprocess.STDOUT,
                    check=True
                )
                iostat_process.send_signal(subprocess.signal.SIGINT)
                time.sleep(1)
                iostat_process.terminate()
                self.parse_iostat_and_check_health()
                self.slack_thread.send_file(
                    f"{bench.shortname_with_args if bench.shortname_with_args else bench.shortname} results",
                    self.get_benchmark_result_filepath(bench, output_dir))
                subprocess.run(f"cp -r ./generated_code {output_dir}", shell=True, check=True)
                self.create_trace(output_dir,
                                  f"{bench.shortname_with_args if bench.shortname_with_args else bench.shortname}.trace")
                subprocess.run(f"cp *.csv {output_dir}", shell=True, check=True)
                time.sleep(30)

            except subprocess.CalledProcessError as e:
                logging.error(f"Benchmark failed: {e}")
                continue


def setup_benchmarks() -> List[BenchmarkConfig]:
    """Create list of benchmark configurations."""
    return [
        # BenchmarkConfig(
        #     shortname="sel_cpu_pd",
        #     args="--bench_micro_cpu_socket_pushdown_filter --pushdown_dop=4 --result_file={output_dir}/sel_cpu_pd_dop_1.csv",
        #     shortname_with_args="sel_cpu_pd_dop_4"
        # ),
        # BenchmarkConfig(
        #     shortname="sel_cpu_pd",
        #     args="--bench_micro_cpu_socket_pushdown_filter --pushdown_dop=16 --result_file={output_dir}/sel_cpu_pd_dop_4.csv",
        #     shortname_with_args="sel_cpu_pd_dop_16"
        # ),
        # BenchmarkConfig(
        #     shortname="sel_cpu_baseline",
        #     args="--bench_micro_cpu_socket_pushdown_baseline --result_file={output_dir}/sel_cpu_baseline.csv"
        # ),
        # BenchmarkConfig(
        #     shortname="sel_cpu_stage_one",
        #     args="--bench_micro_cpu_socket_stage_one --result_file={output_dir}/sel_cpu_stage_one.csv"
        # ),

        BenchmarkConfig(
            shortname="sel_cpu_stage_both",
            args="--bench_micro_cpu_socket_stage_both"
        ),
        BenchmarkConfig(
            shortname="sel_cpu_grouter_staging",
            args="--bench_micro_cpu_socket_grouter_stage_both --grouter_policy=DISTINCT_RANDOM_SPLIT_PREFER_DATA_LOCAL"
        ),
        BenchmarkConfig(
            shortname="sel_cpu_grouter_staging_partial_sum",
            args="--bench_micro_cpu_socket_grouter_stage_both_partial_sum --grouter_policy=DISTINCT_RANDOM_SPLIT_PREFER_DATA_LOCAL"
        ),
        BenchmarkConfig(
            # currently this relies on hard coding the routing policy
            shortname="sel_cpu_grouter_adaptive_force_staging",
            args="--bench_micro_cpu_socket_adaptive --pushdown_dop=4 --grouter_policy=DISTINCT_THROUGHPUT_SPLIT_PREFER_DATA_LOCAL"
        ),
        # BenchmarkConfig(
        #     shortname="sel_cpu_grouter_adaptive_force_staging_partial",
        #     args="--bench_micro_cpu_socket_adaptive --pushdown_dop=4 --grouter_policy=DISTINCT_THROUGHPUT_SPLIT_PREFER_DATA_LOCAL --result_file={output_dir}/sel_cpu_grouter_force_staging_partial.csv"
        # ),
        # BenchmarkConfig(
        #     shortname="sel_cpu_grouter_adaptive_throughput",
        #     args="--bench_micro_cpu_socket_adaptive --pushdown_dop=4 --grouter_policy=DISTINCT_THROUGHPUT_SPLIT_PREFER_DATA_LOCAL --result_file={output_dir}/sel_cpu_grouter_adaptive_force_staging.csv",
        #     shortname_with_args="sel_cpu_adaptive_tp_pd_dop_4"
        # ),
        # BenchmarkConfig(
        #     shortname="sel_cpu_grouter_adaptive_backpressure",
        #     args="--bench_micro_cpu_socket_adaptive --pushdown_dop=4 --grouter_policy=DISTINCT_RANDOM_SPLIT_PREFER_DATA_LOCAL --result_file={output_dir}/sel_cpu_grouter_adaptive.csv",
        #     shortname_with_args="sel_cpu_adaptive_bp_pd_dop_4"
        # ),
        # BenchmarkConfig(
        #     shortname="sel_cpu_grouter_adaptive_throughput",
        #     args="--bench_micro_cpu_socket_adaptive --pushdown_dop=16 --grouter_policy=DISTINCT_THROUGHPUT_SPLIT_PREFER_DATA_LOCAL --result_file={output_dir}/sel_cpu_grouter_adaptive_force_staging.csv",
        #     shortname_with_args="sel_cpu_adaptive_tp_pd_dop_16"
        # ),
        # BenchmarkConfig(
        #     shortname="sel_cpu_grouter_adaptive_backpressure",
        #     args="--bench_micro_cpu_socket_adaptive --pushdown_dop=16 --grouter_policy=DISTINCT_RANDOM_SPLIT_PREFER_DATA_LOCAL --result_file={output_dir}/sel_cpu_grouter_adaptive.csv",
        #     shortname_with_args="sel_cpu_adaptive_bp_pd_dop_16"
        # ),

        # BenchmarkConfig(
        #     shortname="sel_cpu_grouter_pd",
        #     args="--bench_micro_cpu_socket_grouter_pd --pushdown_dop=1 --result_file={output_dir}/sel_cpu_grouter_pd_dop1.csv",
        #     shortname_with_args="sel_cpu_grouter_pd_dop_1"
        # ),
        # BenchmarkConfig(
        #     shortname="sel_cpu_grouter_pd",
        #     args="--bench_micro_cpu_socket_grouter_pd --pushdown_dop=4 --result_file={output_dir}/sel_cpu_grouter_pd.csv"
        # )
    ]


def main():
    parser = argparse.ArgumentParser(description="Benchmark runner script")
    parser.add_argument("-a", "--profile-amd", action="store_true", help="Enable AMD profiling")
    parser.add_argument("-p", "--profile-perf", action="store_true", help="Enable perf profiling")
    parser.add_argument("-f", "--flame-graph", action="store_true", help="Generate flame graph")
    parser.add_argument("-o", "--off-cpu-flame-graph", action="store_true", help="Generate off-CPU flame graph")

    # Add path arguments with defaults
    parser.add_argument("--bin-dir", type=Path,
                        default="/tmp/tmp.YD2SgUVlV5/cmake-build-release/opt/pelago/",
                        help="Binary directory path")
    parser.add_argument("--perf-archive-path", type=Path,
                        default="/tmp/tmp.YD2SgUVlV5/tools/profiling/perf_archive.sh",
                        help="Perf archive script path")
    parser.add_argument("--flame-graph-path", type=Path,
                        default="/home/nicholso/FlameGraph",
                        help="FlameGraph directory path")
    parser.add_argument("--output-dir", type=Path,
                        default="/scratch/nicholso/adm_output",
                        help="Output directory path")
    parser.add_argument("--selectivities", type=str, default=None,
                        help="Selectivities to run. Comma seperated list of doubles. ")
    parser.add_argument("--num-iterations", type=int, default=5, help="Number of iterations to run each benchmark")

    args = parser.parse_args()
    benchmarks = setup_benchmarks()
    runner = BenchmarkRunner(args, benchmarks)
    os.chdir(args.bin_dir)

    # Setup logging
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Create formatters
    formatter = logging.Formatter('%(levelname)-8s | %(asctime)s | %(filename)s.py:%(lineno)d | %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    # Create file handler
    file_handler = logging.FileHandler(runner.base_output / 'run_sel_micros.log')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    try:
        if args.profile_amd:
            runner.run_amd_profile()
        elif args.profile_perf:
            runner.run_perf_profile()
        elif args.flame_graph:
            runner.run_flame_graph()
        elif args.off_cpu_flame_graph:
            runner.run_off_cpu_flame_graph()
        else:
            runner.run_standard_benchmark()
    except Exception as e:
        logging.error(f"Error running benchmarks: {e}")
        runner.slack_thread.send_slack_message(
            f"Error running benchmarks: \n ```{"".join(traceback.format_exception(e))}```")
    runner.slack_thread.send_slack_message("Benchmark run complete <@U02D23JN031>")


if __name__ == "__main__":
    main()
