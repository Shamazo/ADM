from perfetto.trace_processor import TraceProcessor
import argparse
import glob
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Quick stats from a perfetto trace")
    # Add path arguments with defaults
    parser.add_argument("-i", "--input-path", type=str)
    args = parser.parse_args()

    files = [str(p) for p in Path(args.input_path).glob('**/*.trace.gz')]
    print(f"analyzing: {files}")

    for file in files:
        print("=" * 80)
        print("Processing file: ", file)
        tp = TraceProcessor(file)
        print("queries")

        query_result = tp.query("""INCLUDE PERFETTO MODULE slices.slices;
        SELECT ts, dur, name
        FROM slice
        WHERE name = 'QUERY_EXECUTE'
        ORDER BY ts ASC
        """
        )
        for query_num, query_time_range in enumerate(query_result):
            if (query_num == 0):
                # skip warm up query
                continue
            print("-" * 80)
            print(
                f"query: {query_num}. time range {query_time_range.ts} - {query_time_range.ts + query_time_range.dur}")

            print("grouter")
            query_string = f"""INCLUDE PERFETTO MODULE slices.slices;
            INCLUDE PERFETTO MODULE time.conversion;
        
            SELECT
              args_1.display_value AS arg_debug__uuid,
              _slice_with_thread_and_process_info_0.name AS name,
              COUNT(_slice_with_thread_and_process_info_0.name) AS count,
              time_to_ms(SUM(_slice_with_thread_and_process_info_0.dur)) AS sum_dur_ms,
              AVG(time_to_ns(_slice_with_thread_and_process_info_0.dur)) AS avg_dur_ns,
              time_to_ms(MIN(_slice_with_thread_and_process_info_0.dur)) AS min_dur_ms,
              time_to_ms(MAX(_slice_with_thread_and_process_info_0.dur)) AS max_dur_ms
            FROM _slice_with_thread_and_process_info AS _slice_with_thread_and_process_info_0
            LEFT JOIN args AS args_1 ON args_1.arg_set_id = _slice_with_thread_and_process_info_0.arg_set_id AND args_1.key = 'debug.uuid'
            LEFT JOIN thread AS thread_2 ON thread_2.id = _slice_with_thread_and_process_info_0.utid
            WHERE ((_slice_with_thread_and_process_info_0.name = 'ROUTER_WAITING_FOR_TASK') 
                OR (_slice_with_thread_and_process_info_0.name = 'ROUTER_ACQUIRING_BUFF') 
                OR (_slice_with_thread_and_process_info_0.name = 'GROUTER_WAITING_FOR_TASK') 
                OR (_slice_with_thread_and_process_info_0.name = 'GROUTER_CONSUME')
                OR (_slice_with_thread_and_process_info_0.name = 'ROUTER_CONSUME'))
                AND _slice_with_thread_and_process_info_0.ts AND (ts BETWEEN {query_time_range.ts} AND {query_time_range.ts + query_time_range.dur}
            OR (ts + dur BETWEEN {query_time_range.ts} AND {query_time_range.ts + query_time_range.dur}))
            GROUP BY args_1.display_value,  _slice_with_thread_and_process_info_0.name
            ORDER BY sum_dur_ms DESC"""
            try:
                query_result = tp.query(query_string)
                qr_df = query_result.as_pandas_dataframe()
                print(qr_df.to_string())
            except Exception as e:
                print(query_string)
                print(e)
            print("")
            print("memmove")
            query_result = tp.query(f"""INCLUDE PERFETTO MODULE slices.slices;
        INCLUDE PERFETTO MODULE time.conversion;

        SELECT 
          args_1.display_value AS arg_debug__uuid,
              _slice_with_thread_and_process_info_0.name AS name,
              COUNT(_slice_with_thread_and_process_info_0.name) AS count,
            time_to_ms(SUM(_slice_with_thread_and_process_info_0.dur)) AS sum_dur_ms,
              AVG(time_to_ns(_slice_with_thread_and_process_info_0.dur)) AS avg_dur_ns,
              time_to_ms(MIN(_slice_with_thread_and_process_info_0.dur)) AS min_dur_ms,
              time_to_ms(MAX(_slice_with_thread_and_process_info_0.dur)) AS max_dur_ms
        FROM _slice_with_thread_and_process_info AS _slice_with_thread_and_process_info_0
        LEFT JOIN args AS args_1 ON args_1.arg_set_id = _slice_with_thread_and_process_info_0.arg_set_id AND args_1.key = 'debug.uuid'
        LEFT JOIN thread AS thread_2 ON thread_2.id = _slice_with_thread_and_process_info_0.utid
        WHERE ((_slice_with_thread_and_process_info_0.name = 'MEMMOVE_PROPAGATE')
            OR (_slice_with_thread_and_process_info_0.name = 'MEMMOVE_GET_PROPAGATED')
            OR (_slice_with_thread_and_process_info_0.name = 'MEMMOVE_CONSUME')
            OR (_slice_with_thread_and_process_info_0.name = 'MEMMOVE_WAITING_FOR_TRANSFER'))
        AND _slice_with_thread_and_process_info_0.ts AND (ts BETWEEN {query_time_range.ts} AND {query_time_range.ts + query_time_range.dur}
            OR (ts + dur BETWEEN {query_time_range.ts} AND {query_time_range.ts + query_time_range.dur}))
            GROUP BY args_1.display_value,  _slice_with_thread_and_process_info_0.name
            ORDER BY sum_dur_ms DESC""")
            qr_df = query_result.as_pandas_dataframe()
            print(qr_df.to_string())
            print("")
if __name__ == "__main__":
    main()
