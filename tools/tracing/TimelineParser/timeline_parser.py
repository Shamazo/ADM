import logging
import functools
import pandas as pd
import json
from datetime import datetime

from .trace_generator import TraceGenerator, Group, NormalTrack, CounterTrack
from pathlib import Path
from typing import Dict
import pytz


def parse_timeline(input_path: Path, output_path: Path):
    logging.info(f"Parsing trace logs in `{input_path}` and writing Perfetto trace to `{output_path}`")
    tgen = TraceGenerator(str(output_path))
    parse_adm_timestamps(input_path, tgen)
    min_raw_time_stamp, start_dt = parse_timeline_ranges(input_path, tgen)
    parse_iostat(input_path, tgen, start_dt)
    parse_counters(input_path, tgen, min_raw_time_stamp)
    # TODO parse events if we start using them in proteus


def parse_iostat(input_path: Path, tgen: TraceGenerator, start_dt: datetime):
    """Parse iostat data. iostat must be recorded with the '-o JSON'  and '-t' flags]"""

    io_stat_path = input_path / "iostat_output.json"
    logging.info(f"parsing counters from {io_stat_path}")

    if not io_stat_path.exists():
        logging.warning(f"iostat output not found at {io_stat_path}")
        return
    bw_group = tgen.create_group("nvme_read_bw")
    lat_group = tgen.create_group("nvme_read_lat")
    with open(io_stat_path, 'r') as f:
        try:
            iostat_data = json.load(f)
        except Exception as E:
            logging.warning(f"Failed to parse io stat json from {io_stat_path} with {E}")
            return
        # Extract all timestamps and sysstat data
        nvme_bw_tracks: Dict[str, CounterTrack] = dict()
        nvme_lat_tracks: Dict[str, CounterTrack] = dict()

        for entry in iostat_data['sysstat']['hosts'][0]['statistics']:
            # we replace the TZ infor because you cannot diff a DT without a timezone from a DT with a timezone
            ts = datetime.fromisoformat(entry['timestamp']).astimezone(pytz.utc)
            time_since_start_ns = (ts - start_dt).total_seconds() * 1e9
            if (time_since_start_ns < 0):
                # proteus hasn't started yet
                continue
            time_since_start_ns = int(time_since_start_ns)
            for disk in entry['disk']:
                if disk['disk_device'] not in nvme_bw_tracks:
                    nvme_bw_tracks[disk['disk_device']] = bw_group.create_counter_track(
                        f"{disk['disk_device']} - R MB/s")
                    nvme_lat_tracks[disk['disk_device']] = lat_group.create_counter_track(
                        f"{disk['disk_device']} - r_await")
                bw_track = nvme_bw_tracks[disk['disk_device']]
                bw_track.count(time_since_start_ns, round(float(disk['rMB/s'])))
                lat_track = nvme_lat_tracks[disk['disk_device']]
                lat_track.count(time_since_start_ns, round(float(disk['r_await'])))


def parse_adm_timestamps(input_path: Path, tgen: TraceGenerator):
    group = tgen.create_group("global_times")
    adm_stamps_path = input_path / "adm-timestamps.csv"
    if not adm_stamps_path.exists():
        logging.warning(f"adm timestamps not found at {adm_stamps_path}")
        return
    try:
        timestamps = pd.read_csv(adm_stamps_path, quotechar='"')
    except Exception as E:
        logging.warning(f"Failed to parse adm timestamps from {adm_stamps_path} with {E}")
        return
    # all timestamps are relative for this one
    timestamps['timestamp_start'] = timestamps['timestamp_start'].astype(int)
    timestamps['timestamp_end'] = timestamps['timestamp_end'].astype(int)

    for row in timestamps.itertuples():
        try:
            group.open(row.timestamp_start, row.name, json.loads(row.extra))
        except:
            logging.warning(f"failed to parse extra {row.extra}")
            group.open(row.timestamp_start, row.name)

        group.close(row.timestamp_end)


def parse_counters(input_path: Path, tgen: TraceGenerator, min_raw_time_stamp: int):
    """
    Parse absolute value counters.
    It is likely possible to also support delta counts. See CounterDescriptor.is_incremental in the proto
    """
    timeline_counters_path = input_path / "timeline-counters.csv"
    if not timeline_counters_path.exists():
        logging.fatal(f"timelime counters not found at {timeline_counters_path}")
    logging.info(f"parsing counters from {timeline_counters_path}")
    rdtsc_freq = get_rdtsc_frequency(input_path)
    timeline_counters = pd.read_csv(timeline_counters_path)
    unique_operators = timeline_counters['operator'].unique()
    counter_id_to_str = parse_counter_legend(input_path)

    for operator in unique_operators:
        group = tgen.create_group(operator, None, 1)
        operator_c_tracks: Dict[(str, int), CounterTrack] = dict()
        operator_counters = timeline_counters[timeline_counters['operator'] == operator]
        unique_idxs = operator_counters['counter_index'].unique()
        unique_idxs.sort()
        for idx in unique_idxs:
            operator_id_rows = operator_counters[operator_counters['counter_index'] == idx]
            for row in operator_id_rows.itertuples():
                if (row.counter, row.counter_index) not in operator_c_tracks:
                    operator_c_tracks[(row.counter, row.counter_index)] = group.create_counter_track(
                        f"{counter_id_to_str[row.counter]}-{row.counter_index:02}")
                track: CounterTrack = operator_c_tracks[(row.counter, row.counter_index)]
                # convert to ns
                track.count(round((row.timestamp - min_raw_time_stamp) * rdtsc_freq), row.value)


def parse_counter_legend(input_path: Path) -> dict[int, str]:
    op_legend_path = input_path / "timeline-counters-oplegend.csv"
    if not op_legend_path.exists():
        logging.fatal(f"timelime counters op legend not found at {op_legend_path}")
    df = pd.read_csv(op_legend_path)
    df['counter_name'] = df['counter_name'].astype(str)
    df['value'] = df['value'].astype(int)
    return dict(zip(df['value'], df['counter_name']))


@functools.lru_cache(maxsize=128)
def get_rdtsc_frequency(input_path: Path) -> float:
    """
    :param input_path: path to the directory containing "timeline-ranges.csv"
    :return: nanoseconds per rdtsc tick
    """
    timeline_ranges_path = input_path / "timeline-ranges.csv"
    if not timeline_ranges_path.exists():
        logging.fatal(f"timelime ranges not found at {timeline_ranges_path}")

    opcode_dict = parse_timeline_ranges_op_legend(input_path)
    timeline_ranges = pd.read_csv(timeline_ranges_path)
    LOGGER_TIMESTAMP_OP_CODE = None
    for k, v in opcode_dict.items():
        if v == 'LOGGER_TIMESTAMP':
            LOGGER_TIMESTAMP_OP_CODE = k
            break
    if LOGGER_TIMESTAMP_OP_CODE is None:
        logging.fatal("Failed to find LOGGER_TIMESTAMP in timeline ranges op legend!")

    logger_ts = timeline_ranges[timeline_ranges['op'] == LOGGER_TIMESTAMP_OP_CODE]
    start_row = logger_ts[logger_ts['timestamp_start'] == min(logger_ts['timestamp_start'])]
    end_row = logger_ts[logger_ts['timestamp_start'] == max(logger_ts['timestamp_start'])]

    start_ts = int(start_row.iloc[0].instance_id)  # ns
    end_ts = int(end_row.iloc[0].instance_id)  # ns
    start_tick = int(start_row.iloc[0].timestamp_start)  # rdtsc
    end_tick = int(end_row.iloc[0].timestamp_start)  # rdtsc
    ns_per_tick = (end_ts - start_ts) / (end_tick - start_tick)
    return ns_per_tick


def parse_timeline_ranges(input_path: Path, tgen: TraceGenerator) -> (int, datetime):
    """
    :return: minimum raw rdtsc timestamp in timeline-ranges.csv
    """
    get_rdtsc_frequency(input_path)
    default_group = tgen.create_group("threads")
    thread_dict: Dict[int, NormalTrack] = dict()
    thread_mapping: Dict[str, int] = dict()
    thread_count = 0
    rdtsc_freq = get_rdtsc_frequency(input_path)

    timeline_ranges_path = input_path / "timeline-ranges.csv"
    if not timeline_ranges_path.exists():
        logging.fatal(f"timeline ranges not found at {timeline_ranges_path}")

    timeline_ranges = pd.read_csv(timeline_ranges_path)
    timeline_ranges['thread_id'] = timeline_ranges['thread_id'].astype(int)
    timeline_ranges['op'] = timeline_ranges['op'].astype(int)
    timeline_ranges['timestamp_start'] = timeline_ranges['timestamp_start'].astype(int)
    timeline_ranges['timestamp_end'] = timeline_ranges['timestamp_end'].astype(int)
    start_rdtsc_tick = min(timeline_ranges['timestamp_start'])

    opcode_dict = parse_timeline_ranges_op_legend(input_path)
    logger_timestamp_op_code = list(opcode_dict.keys())[list(opcode_dict.values()).index("LOGGER_TIMESTAMP")]
    # see tracing.cpp
    first_row = timeline_ranges[timeline_ranges['op'] == logger_timestamp_op_code].sort_values('timestamp_start').iloc[
        0]
    start_nanos_since_epoch = first_row['instance_id']
    dt = datetime.fromtimestamp(start_nanos_since_epoch / 1e9, tz=pytz.utc)

    for row in timeline_ranges.itertuples():
        if row.thread_id not in thread_mapping:
            thread_mapping[row.thread_id] = thread_count
            thread_count += 1
        thread_index = thread_mapping[row.thread_id]

        if row.thread_id not in thread_dict:
            thread_dict[row.thread_id] = default_group.create_track(str(row.thread_id), thread_index)
        track = thread_dict[row.thread_id]

        # convert to ns
        track.open(round((row.timestamp_start - start_rdtsc_tick) * rdtsc_freq), opcode_dict[row.op],
                   {"pipeline": row.pipeline_id, "instance": row.instance_id, "uuid": row.operator})
        track.close(round((row.timestamp_end - start_rdtsc_tick) * rdtsc_freq))

    return start_rdtsc_tick, dt


def parse_timeline_ranges_op_legend(input_path: Path) -> Dict[int, str]:
    op_legend_path = input_path / "timeline-ranges-oplegend.csv"
    if not op_legend_path.exists():
        logging.fatal(f"timelime ranges op legend not found at {op_legend_path}")
    df = pd.read_csv(op_legend_path)
    df['op'] = df['op'].astype(str)
    df['value'] = df['value'].astype(int)
    return dict(zip(df['value'], df['op']))
