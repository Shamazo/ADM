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


@dataclass
class BenchmarkConfig:
    """Configuration for a single benchmark run."""
    shortname: str
    args: str
    shortname_with_args: Optional[str] = None


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

    def get_benchmark_output_dir(self, benchmark: BenchmarkConfig) -> Path:
        """Construct full output path for a benchmark."""
        return self.base_output / benchmark.shortname

    def get_benchmark_args(self, benchmark: BenchmarkConfig, output_dir: Path) -> str:
        """Construct full argument string for a benchmark."""
        return f"{self.common_args} {benchmark.args}".replace("{output_dir}", str(output_dir))

    def create_trace(self, outdir: Path, trace_name: str = "out.trace") -> None:
        """Create a trace file using conda."""
        try:
            logging.info(f"Creating trace: {trace_name}")
            subprocess.run(
                ["conda", "run", "-n", "profiling", "--live-stream",
                 "python3", "/tmp/tmp.YD2SgUVlV5/tools/tracing/cli.py", ".", f"{trace_name}"],
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

                logging.debug(f"Running command: {' '.join(cmd)}")
                subprocess.run(
                    cmd,
                    stdout=open(log_file, "w"),
                    pass_fds=(ctl_fd, ack_fd),
                    stderr=subprocess.STDOUT,
                    check=True,
                    env=os.environ)

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
                for file in Path().glob("*.svg"):
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
                subprocess.run(
                    ["./selectivity-micros"] + benchmark_args.split(),
                    stdout=open(log_file, "a"),
                    stderr=subprocess.STDOUT,
                    check=True
                )

                time.sleep(1)
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
        # BenchmarkConfig(
        #     shortname="sel_cpu_stage_both",
        #     args="--bench_micro_cpu_socket_stage_both --result_file={output_dir}/sel_cpu_stage_both.csv"
        # ),
        # BenchmarkConfig(
        #     shortname="sel_cpu_adaptive",
        #     args="--bench_micro_cpu_socket_adaptive --pushdown_dop=4 --result_file={output_dir}/sel_cpu_adaptive_pd_dop_4.csv",
        #     shortname_with_args="sel_cpu_adaptive_pd_dop_4"
        # ),
        BenchmarkConfig(
            shortname="sel_cpu_adaptive",
            args="--bench_micro_cpu_socket_adaptive --pushdown_dop=16 --result_file={output_dir}/sel_cpu_adaptive_pd_dop_16.csv",
            shortname_with_args="sel_cpu_adaptive_pd_dop_16"
        ),
        # BenchmarkConfig(
        #     shortname="sel_cpu_grouter_staging",
        #     args="--bench_micro_cpu_socket_grouter_stage_both --result_file={output_dir}/sel_cpu_grouter_pd.csv"
        # ),
        # BenchmarkConfig(
        #     shortname="sel_cpu_grouter_pd",
        #     args="--bench_micro_cpu_socket_grouter_pd --pushdown_dop=1 --result_file={output_dir}/sel_cpu_grouter_pd.csv"
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
    parser.add_argument("--num-iterations", type=int, default=3, help="Number of iterations to run each benchmark")

    # Setup logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(levelname)-8s | %(asctime)s | %(filename)s.py:%(lineno)d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    args = parser.parse_args()
    os.chdir(args.bin_dir)
    benchmarks = setup_benchmarks()

    runner = BenchmarkRunner(args, benchmarks)

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


if __name__ == "__main__":
    main()
