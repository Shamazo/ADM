import subprocess
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import os
import json
import time
from pathlib import Path
import numpy as np
from typing import List, Dict
import argparse
from collections import namedtuple
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError




class NVMeHealthMonitor:
    def __init__(self, fio_bin: str, paths: List[str] = None, output_dir: str = "nvme_health_reports"):
        """
        Initialize the NVMe health monitor.

        Args:
            paths: Optional list of mount points to test. If None, all NVMe drives are tested.
            output_dir: Directory to store test results.
        """
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.output_dir = Path(output_dir) / self.timestamp
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.num_threads = 6  # Number of threads per drive
        self.nvme_info = self._get_nvme_info(paths)
        # Get number of NUMA nodes in system
        self.numa_nodes = self._get_numa_node_count()
        self.fio_bin = fio_bin

        self.slack_client = None
        self.slack_thread_ts = None
        self.channel_id = None
        if os.environ.get('SLACK_TOKEN'):
            self.slack_client = WebClient(token=os.environ['SLACK_TOKEN'])
            self.channel_id = self.get_channel_id("adm-reports")
            thread_parent = self.slack_client.chat_postMessage(
                channel=self.channel_id,
                text=f"Starting NVMe health check - {self.timestamp} - {"alll drives " if paths is None else paths}"
            )
            self.slack_thread_ts = thread_parent['ts']

    def get_channel_id(self, channel_name):
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

    def send_slack_message(self, message):
        if self.slack_client is not None:
            try:
                self.slack_client.chat_postMessage(
                    channel=self.channel_id,
                    text=message,
                    thread_ts=self.slack_thread_ts
                )
            except SlackApiError as e:
                print(f"Error sending message to slack: {e}")

    def _get_numa_node_count(self) -> int:
        """Get the number of NUMA nodes in the system."""
        numa_nodes = [d for d in os.listdir('/sys/devices/system/node') if d.startswith('node')]
        return len(numa_nodes)

        def _get_nvme_numa_node(self, device: str) -> int:
            """Get the NUMA node for a specific NVMe device."""

        try:
            numa_path = Path(f"/sys/block/{device}/device/numa_node")
            with open(numa_path, 'r') as f:
                numa_node = int(f.read().strip())
                # Some systems might return -1 if NUMA info is not available
                return max(0, numa_node)
        except (FileNotFoundError, ValueError):
            print(f"Warning: Could not determine NUMA node for {device}, defaulting to 0")
            return 0

    def _get_nvme_numa_node(self, device: str) -> int:
        """Get the NUMA node for a specific NVMe device."""
        try:
            numa_path = Path(f"/sys/block/{device}/device/numa_node")
            with open(numa_path, 'r') as f:
                numa_node = int(f.read().strip())
                # Some systems might return -1 if NUMA info is not available
                return max(0, numa_node)
        except (FileNotFoundError, ValueError):
            print(f"Warning: Could not determine NUMA node for {device}, defaulting to 0")
            return 0

    def _get_nvme_info(self, selected_paths: List[str] = None) -> Dict[str, Dict[str, str]]:
        """
        Get NVMe mount points and NUMA nodes from df and sysfs.

        Args:
            selected_paths: Optional list of mount points to filter for.
        """
        df_output = subprocess.check_output(['df'], text=True)
        nvme_info = {}

        # Create a set of normalized paths for comparison
        selected_paths_set = None
        if selected_paths:
            selected_paths_set = {Path(p).resolve() for p in selected_paths}

        for line in df_output.split('\n')[1:]:
            if not line:
                continue
            parts = line.split()
            if 'nvme' in parts[0]:
                device = parts[0].split('/')[-1].split('p')[0]  # Extract nvmeXnY
                mount_point = parts[-1]

                # Skip if we have selected paths and this mount point isn't in them
                if selected_paths_set is not None:
                    mount_path = Path(mount_point).resolve()
                    if mount_path not in selected_paths_set:
                        continue

                numa_node = self._get_nvme_numa_node(device)
                nvme_info[device] = {
                    'mount_point': mount_point,
                    'numa_node': numa_node
                }

        if selected_paths and not nvme_info:
            raise ValueError(f"None of the specified paths {selected_paths} matched NVMe devices")

        return nvme_info

    def _create_test_files(self):
        """Create test files if they don't exist."""
        for device, info in self.nvme_info.items():
            mount = info['mount_point']
            test_file = Path(mount) / "fio_test_file"
            if not test_file.exists():
                print(f"Creating test file for {device} at {test_file}")
                subprocess.run([
                    'dd',
                    'if=/dev/zero',
                    f'of={test_file}',
                    'bs=1M',
                    'count=1024'  # 1GB file
                ])

    def _generate_fio_config(self) -> str:
        """Generate FIO configuration for all NVMe drives."""
        fio_config = "[global]\n"
        fio_config += "time_based=1\n"
        fio_config += "runtime=300\n"
        fio_config += "group_reporting=0\n"
        fio_config += f"numjobs={self.num_threads}\n"  # Use multiple threads per drive
        fio_config += "thread=1\n"  # Use threads instead of processes
        fio_config += "direct=1\n"
        fio_config += "ioengine=io_uring\n"
        fio_config += "fixedbufs=1\n"  # Use fixed buffers for io_uring
        fio_config += "registerfiles=1\n"
        fio_config += "numa_mem_policy=local\n"

        numa_devices = {}
        for device, info in self.nvme_info.items():
            numa_node = info['numa_node']
            if numa_node not in numa_devices:
                numa_devices[numa_node] = []
            numa_devices[numa_node].append(device)

        # Create job sections grouped by NUMA node
        for numa_node, devices in numa_devices.items():
            for device in devices:
                mount = self.nvme_info[device]['mount_point']
                fio_config += f"\n[{device}]\n"
                fio_config += f"directory={mount}\n"
                fio_config += f"filename={mount}/fio_test_file\n"
                fio_config += f"numa_cpu_nodes={numa_node}\n"
                fio_config += "rw=read\n"
                fio_config += "size=1G\n"
                fio_config += "direct=1\n"
                fio_config += "ioengine=io_uring\n"
                fio_config += "bs=1M\n"
                fio_config += "iodepth=32\n"

        config_path = self.output_dir / "fio_config.ini"
        with open(config_path, 'w') as f:
            f.write(fio_config)

        return str(config_path)

    def _start_iostat_collection(self) -> subprocess.Popen:
        """Start iostat collection process."""
        # run iostat once to clear any previous data
        subprocess.run(['iostat'], check=True)
        iostat_output = self.output_dir / "iostat_output.json"
        return subprocess.Popen(
            ['iostat', '-xm', '1', '-o', 'JSON'],
            stdout=open(iostat_output, 'w'),
            stderr=subprocess.PIPE
        )

    def _run_fio_test(self, config_path: str):
        """Run FIO test with the generated configuration."""
        fio_output = self.output_dir / "fio_output.json"
        subprocess.run(
            [f'{self.fio_bin}', '--output-format=json', f'--output={fio_output}', config_path],
            check=True
        )

    def _parse_iostat_output(self) -> pd.DataFrame:
        """Parse iostat JSON output into a DataFrame."""
        iostat_file = self.output_dir / "iostat_output.json"
        data = []
        timestamp = 0

        with open(iostat_file, 'r') as f:
            iostat_data = json.load(f)

        # Extract all timestamps and sysstat data
        for entry in iostat_data['sysstat']['hosts'][0]['statistics']:
            timestamp += 1
            # skip start up time
            if timestamp < 5:
                continue

            for disk in entry['disk']:
                if 'nvme' in disk['disk_device']:
                    if disk['disk_device'] not in self.nvme_info:
                        continue
                    data.append({
                        'timestamp': timestamp,
                        'device': disk['disk_device'],
                        'r_await': float(disk['r_await']),
                        'rmb_s': float(disk['rMB/s']),
                        'w_await': float(disk['w_await']),
                        'wmb_s': float(disk['wMB/s']),
                        'util': float(disk['util']),
                        'aqu-sz': float(disk['aqu-sz'])
                    })

        df = pd.DataFrame(data)

        # Convert timestamp to relative seconds from start of test
        df['timestamp'] = df['timestamp'] - df['timestamp'].min()

        return df

    def _analyze_performance(self, df: pd.DataFrame) -> Dict[str, List[str]]:
        """Analyze drive performance for anomalies with enhanced metrics."""
        anomalies = {
            'high_latency': [],
            'increasing_latency': [],
            'low_bandwidth': [],
            'high_util': []
        }

        # Group by device and calculate statistics
        device_stats = df.groupby('device').agg({
            'r_await': ['mean', 'std', 'median'],
            'rmb_s': ['mean', 'std', 'median'],
            'util': ['mean', 'max'],
            'aqu-sz': ['mean', 'max']
        })

        # Calculate global statistics for comparison
        global_mean_latency = device_stats[('r_await', 'mean')].mean()
        global_std_latency = device_stats[('r_await', 'std')].mean()
        global_mean_bandwidth = device_stats[('rmb_s', 'mean')].mean()

        for device in df['device'].unique():
            device_data = df[df['device'] == device]

            # Check for consistently high latency
            mean_latency = device_stats.loc[device, ('r_await', 'mean')]
            if mean_latency > global_mean_latency + 1.75 * global_std_latency:
                anomalies['high_latency'].append(device)
                self.send_slack_message(f"High latency detected on {device}, mounted at {self.nvme_info[device]['mount_point']} on numa node {self.nvme_info[device]['numa_node']} with mean latency of {mean_latency} ms")

            # Check for increasing latency trend
            if len(device_data) > 10:
                # Use more sophisticated trend analysis
                z = np.polyfit(device_data['timestamp'], device_data['r_await'], 1)
                slope = z[0]
                if slope > 0.01:  # .1 ms/second increase
                    anomalies['increasing_latency'].append(device)

            # Check for low bandwidth
            mean_bandwidth = device_stats.loc[device, ('rmb_s', 'mean')]
            if mean_bandwidth < global_mean_bandwidth * 0.9:  # Below 70% of average
                anomalies['low_bandwidth'].append(device)

        return anomalies



    def _generate_plots(self, df: pd.DataFrame):
        """Generate performance plots with enhanced metrics."""
        plt.style.use('seaborn-v0_8-whitegrid')
        numa_colors = {
            0: plt.cm.Reds,
            1: plt.cm.Blues,
            2: plt.cm.Greens,
            3: plt.cm.Purples,
            4: plt.cm.Oranges,
            5: plt.cm.GnBu,
            6: plt.cm.YlOrBr,
            7: plt.cm.PuRd,
        }

        # Define different line styles to increase distinction
        line_styles = ['-', '--', '-.', ':']

        def get_device_style(device, numa_node, device_index_in_numa):
            """Get consistent style for a device based on its NUMA node."""
            # Get color palette for this NUMA node
            color_map = numa_colors.get(numa_node, plt.cm.Greys)

            # Calculate color from palette
            # Spread colors between 0.3 and 0.9 to avoid too light/dark colors
            color_val = 0.3 + (0.6 * (device_index_in_numa / (len(df['device'].unique()) + 1)))
            color = color_map(color_val)

            # Cycle through line styles
            line_style = line_styles[device_index_in_numa % len(line_styles)]

            return color, line_style

        def create_plot(metric, ylabel, title, filename):
            plt.figure(figsize=(15, 8))

            # Group devices by NUMA node
            numa_devices = {}
            NVME_tuple = namedtuple('NVME_tuple', ['device', 'mount_path'])
            for device in df['device'].unique():
                # the case when a user specified specific NVMe drives to test
                if device not in self.nvme_info:
                    continue
                numa_node = self.nvme_info[device]['numa_node']
                mount_path = self.nvme_info[device]['mount_point']
                if numa_node not in numa_devices:
                    numa_devices[numa_node] = []
                numa_devices[numa_node].append(NVME_tuple(device, mount_path))

            # Sort devices within each NUMA node
            for numa_node in numa_devices:
                numa_devices[numa_node].sort()

            # Plot devices grouped by NUMA node
            for numa_node in sorted(numa_devices.keys()):
                nvmes = numa_devices[numa_node]

                # Create a section in the legend for this NUMA node
                plt.plot([], [], ' ', label=f'NUMA Node {numa_node}')

                for idx, nvme in enumerate(nvmes):
                    device_data = df[df['device'] == nvme.device]
                    color, line_style = get_device_style(nvme.device, numa_node, idx)

                    line = plt.plot(device_data['timestamp'], device_data[metric],
                                    linestyle=line_style,
                                    color=color,
                                    label=f"  {nvme.mount_path}",  # Indent device names in legend
                                    linewidth=2,
                                    alpha=0.8)

            plt.xlabel('Time (seconds)')
            plt.ylabel(ylabel)
            plt.title(f'{title}\nGrouped by NUMA Node')

            # Customize legend
            plt.legend(bbox_to_anchor=(1.05, 1),
                       loc='upper left',
                       borderaxespad=0.,
                       frameon=True,
                       shadow=True)

            # Add grid with different z-order to ensure it doesn't override the lines
            plt.grid(True, linestyle='--', alpha=0.7, zorder=0)

            # Adjust layout to prevent label cutoff
            plt.tight_layout()

            # Add a light gray background to the plot area
            plt.gca().set_facecolor('#f8f8f8')

            # Save with high DPI for better quality
            plt.savefig(self.output_dir / filename,
                        dpi=150,
                        bbox_inches='tight',
                        facecolor='white',
                        edgecolor='none')
            plt.close()
            if self.slack_client is not None:
                try:
                    response = self.slack_client.files_upload_v2(
                        channel=self.channel_id,
                        file=str(self.output_dir / filename),
                        title=filename,
                        thread_ts=self.slack_thread_ts,
                        initial_comment=f"f{filename}"
                    )
                    permalink = response['files'][0]['permalink']
                    self.send_slack_message(permalink)
                except SlackApiError as e:
                    print(f"Error uploading file to slack: {e}")

        # Generate all plots
        create_plot('r_await',
                    'Read Latency (ms)',
                    'Read Latency Over Time',
                    'read_latency.png')

        create_plot('rmb_s',
                    'Read Bandwidth (mB/s)',
                    'Read Bandwidth Over Time',
                    'read_bandwidth.png')

        create_plot('util',
                    'Utilization (%)',
                    'Device Utilization Over Time',
                    'utilization.png')

        create_plot('aqu-sz',
                    'Average Queue Size',
                    'Device Queue Size Over Time',
                    'queue_size.png')

    def _generate_report(self, anomalies: Dict[str, List[str]]):
        """Generate a text report of the findings."""
        report_path = self.output_dir / "report.txt"

        # Group devices by NUMA node
        numa_devices = {}
        for device, info in self.nvme_info.items():
            numa_node = info['numa_node']
            if numa_node not in numa_devices:
                numa_devices[numa_node] = []
            numa_devices[numa_node].append((device, info))

        with open(report_path, 'w') as f:
            f.write(f"NVMe Health Report - {self.timestamp}\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Test Configuration:\n")
            f.write("-" * 20 + "\n")
            f.write(f"- IO Engine: io_uring\n")
            f.write(f"- Number of CPU threads per drive: {self.num_threads}\n")
            # NUMA Configuration section
            f.write("NUMA Configuration:\n")
            f.write("-" * 20 + "\n")
            for numa_node in sorted(numa_devices.keys()):
                devices = numa_devices[numa_node]
                f.write(f"\nNUMA Node {numa_node}:\n")
                f.write(f"  Devices [{len(devices)}]:\n")

                # Create a formatted table for devices
                f.write("  {:<15} {:<50}\n".format("Device", "Mount Point"))
                f.write("  " + "-" * 65 + "\n")
                for device, info in sorted(devices):
                    f.write("  {:<15} {:<50}\n".format(
                        device,
                        info['mount_point']
                    ))
            f.write("\n")

            if anomalies['high_latency']:
                f.write("Drives with consistently high latency:\n")
                for device in anomalies['high_latency']:
                    numa_node = self.nvme_info[device]['numa_node']
                    mount_point = self.nvme_info[device]['mount_point']
                    f.write(f"- {device} (NUMA node {numa_node} mounted at {mount_point})\n")
                f.write("\n")

            if anomalies['increasing_latency']:
                f.write("Drives with increasing latency:\n")
                for device in anomalies['increasing_latency']:
                    numa_node = self.nvme_info[device]['numa_node']
                    mount_point = self.nvme_info[device]['mount_point']
                    f.write(f"- {device} (NUMA node {numa_node} mounted at {mount_point})\n")
                f.write("\n")

            if anomalies['low_bandwidth']:
                f.write("Drives with below-average bandwidth:\n")
                for device in anomalies['low_bandwidth']:
                    numa_node = self.nvme_info[device]['numa_node']
                    mount_point = self.nvme_info[device]['mount_point']
                    f.write(f"- {device} (NUMA node {numa_node} mounted at {mount_point})\n")
                f.write("\n")

            if not any(anomalies.values()):
                f.write("No anomalies detected. All drives are performing normally.\n")
        if self.slack_client is not None:
            try:
                response = self.slack_client.files_upload_v2(
                    channel=self.channel_id,
                    file=str(report_path),
                    thread_ts=self.slack_thread_ts
                )
                permalink = response['files'][0]['permalink']
                self.send_slack_message(permalink)
            except SlackApiError as e:
                print(f"Error uploading file to slack: {e}")

    def run_health_check(self):
        """Run the complete health check process."""
        print(f"Starting NVMe health check - {self.timestamp}")

        # Create test files if they don't exist
        self._create_test_files()

        # Generate and run FIO test
        config_path = self._generate_fio_config()
        iostat_process = self._start_iostat_collection()

        try:
            self._run_fio_test(config_path)
        finally:
            # sigint so iostat can finish writing its output file in correct json
            iostat_process.send_signal(subprocess.signal.SIGINT)
            time.sleep(1)
            iostat_process.terminate()

        # Analyze results
        df = self._parse_iostat_output()
        anomalies = self._analyze_performance(df)

        # Generate outputs
        self._generate_plots(df)
        self._generate_report(anomalies)

        print(f"Health check complete. Results saved to {self.output_dir}")


def main():
    parser = argparse.ArgumentParser(
        description='NVMe Health Check. Evaluates if NVMe performance is stable. Requires iostat to be installed. Depends on pandas, matplotlib and numpy python packages.')
    parser.add_argument('--paths', type=str, default=None,
                        help='Comma-separated list of NVMe mount points to test (e.g., /nvme1,/nvme13,/nvme14)')
    parser.add_argument('--output-dir', type=str, default="/home/nicholso/nvme_health_reports",
                        help='Directory to store test results')
    parser.add_argument('--fio-bin', type=str, default="/home/nicholso/fio/fio", help='Path to a fio binary')

    args = parser.parse_args()

    # Process paths argument
    paths = None
    if args.paths:
        paths = [p.strip() for p in args.paths.split(',')]
        print(f"Testing specific NVMe drives: {paths}")

    monitor = NVMeHealthMonitor(fio_bin=args.fio_bin, paths=paths, output_dir=args.output_dir)
    monitor.run_health_check()

    return 0


if __name__ == "__main__":
    exit(main())
