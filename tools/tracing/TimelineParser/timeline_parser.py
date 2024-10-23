import logging
import functools
import pandas as pd

from .trace_generator import TraceGenerator, Group, NormalTrack, CounterTrack
from pathlib import Path
from typing import Dict


def parse_timeline(input_path: Path, output_path: Path):
    logging.info(f"Parsing trace logs in `{input_path}` and writing Perfetto trace to `{output_path}`")
    tgen = TraceGenerator(str(output_path))
    min_raw_time_stamp = parse_timeline_ranges(input_path, tgen)
    parse_counters(input_path, tgen, min_raw_time_stamp)
    # TODO parse events if we start using them in proteus


def parse_counters(input_path: Path, tgen: TraceGenerator, min_raw_time_stamp: int):
    """
    Parse absolute value counters.
    It is likely possible to also support delta counts. See CounterDescriptor.is_incremental in the proto
    """
    timeline_counters_path = input_path / "timeline-counters.csv"
    if not timeline_counters_path.exists():
        logging.fatal(f"timelime counters not found at {timeline_counters_path}")

    rdtsc_freq = get_rdtsc_frequency(input_path)
    timeline_counters = pd.read_csv(timeline_counters_path)
    unique_operators = timeline_counters['operator'].unique()
    counter_id_to_str = parse_counter_legend(input_path)

    for operator in unique_operators:
        group = tgen.create_group(operator)
        operator_c_tracks: Dict[(str, int), CounterTrack] = dict()
        operator_counters = timeline_counters[timeline_counters['operator'] == operator]
        for row in operator_counters.itertuples():
            if (row.counter, row.counter_index) not in operator_c_tracks:
                operator_c_tracks[(row.counter, row.counter_index)] = group.create_counter_track(
                    f"{counter_id_to_str[row.counter]}-{row.counter_index}")
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


def parse_timeline_ranges(input_path: Path, tgen: TraceGenerator) -> int:
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

    return start_rdtsc_tick


def parse_timeline_ranges_op_legend(input_path: Path) -> Dict[int, str]:
    op_legend_path = input_path / "timeline-ranges-oplegend.csv"
    if not op_legend_path.exists():
        logging.fatal(f"timelime ranges op legend not found at {op_legend_path}")
    df = pd.read_csv(op_legend_path)
    df['op'] = df['op'].astype(str)
    df['value'] = df['value'].astype(int)
    return dict(zip(df['value'], df['op']))
