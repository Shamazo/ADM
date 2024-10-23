import argparse
from TimelineParser.timeline_parser import parse_timeline
import logging
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Script to convert Proteus trace logs into the Perfetto format")
    parser.add_argument("input_dir", type=Path,
                        help="Directory containing timeline files. Usually opt/pelago or copied from there")
    parser.add_argument("output_trace", type=Path, help="Path to write the output trace in perfetto format")

    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(levelname)-8s | %(asctime)s | %(filename)s.py:%(lineno)d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    if not Path(args.input_dir).exists():
        logging.fatal("input directory does not exist")

    parse_timeline(args.input_dir, args.output_trace)


if __name__ == "__main__":
    main()
