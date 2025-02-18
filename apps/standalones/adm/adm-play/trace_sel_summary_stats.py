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

    for file in files:
        print("=" * 80)
        print("Processing file: ", file)
        tp = TraceProcessor(file)
        print("queries")

        query_result = tp.query("""INCLUDE PERFETTO MODULE slices.slices;
        SELECT
          _slice_with_thread_and_process_info_0.ts AS ts,
          _slice_with_thread_and_process_info_0.dur AS dur,
          _slice_with_thread_and_process_info_0.name AS name,
          args_4.display_value AS selectivity
            FROM _slice_with_thread_and_process_info AS _slice_with_thread_and_process_info_0
        LEFT JOIN args AS args_4 ON args_4.arg_set_id = _slice_with_thread_and_process_info_0.arg_set_id AND args_4.key = 'debug.selectivity'
            WHERE
         args_4.display_value = '0.5'
         ORDER BY ts ASC
        """
                                )
        for selectivity_time_range in query_result:
            print("-" * 80)
            print(f"queries at selectivity: {selectivity_time_range.selectivity}")
            query_result = tp.query(f"""
    INCLUDE PERFETTO MODULE time.conversion;

    SELECT ts, dur FROM slice 
    WHERE name = 'QUERY_EXECUTE'
    AND (ts BETWEEN {selectivity_time_range.ts} AND {selectivity_time_range.ts + selectivity_time_range.dur}
        OR (ts + dur BETWEEN {selectivity_time_range.ts} AND {selectivity_time_range.ts + selectivity_time_range.dur}))
    ORDER BY ts ASC""")
            columns = """"""
            duration_columns = []
            for idx, row in enumerate(list(query_result)[1:]):
                columns += f"""
                SUM(CASE WHEN _slice_with_thread_and_process_info_0.ts BETWEEN {row.ts} AND {row.ts + row.dur} THEN 1 ELSE 0 END) as count_{idx},
                time_to_ms(SUM(CASE WHEN _slice_with_thread_and_process_info_0.ts BETWEEN {row.ts} AND {row.ts + row.dur} THEN _slice_with_thread_and_process_info_0.dur ELSE 0 END)) AS sum_dur_ms_{idx},
                 """
                duration_columns.append(f"sum_dur_ms_{idx}")
                # SQRT(AVG(CASE WHEN _slice_with_thread_and_process_info_0.ts BETWEEN {row.ts} AND {row.ts + row.dur} THEN time_to_ms(_slice_with_thread_and_process_info_0.dur) * time_to_ms(_slice_with_thread_and_process_info_0.dur) ELSE NULL END) -
                # POWER(AVG(CASE WHEN _slice_with_thread_and_process_info_0.ts BETWEEN {row.ts} AND {row.ts + row.dur} THEN time_to_ms(_slice_with_thread_and_process_info_0.dur) ELSE NULL END), 2)) AS duration_stddev_{idx},

            columns = columns.rstrip().rstrip(",") + "\n"

            print("grouter")
            try:
                query_string = f"""INCLUDE PERFETTO MODULE slices.slices;
            INCLUDE PERFETTO MODULE time.conversion;
        
            SELECT
              args_1.display_value AS arg_debug__uuid,
              _slice_with_thread_and_process_info_0.name AS name,
              {columns}
            FROM _slice_with_thread_and_process_info AS _slice_with_thread_and_process_info_0
            LEFT JOIN args AS args_1 ON args_1.arg_set_id = _slice_with_thread_and_process_info_0.arg_set_id AND args_1.key = 'debug.uuid'
            LEFT JOIN thread AS thread_2 ON thread_2.id = _slice_with_thread_and_process_info_0.utid
            WHERE ((_slice_with_thread_and_process_info_0.name = 'ROUTER_WAITING_FOR_TASK') 
                OR (_slice_with_thread_and_process_info_0.name = 'GROUTER_WAITING_FOR_TASK') 
                OR (_slice_with_thread_and_process_info_0.name = 'GROUTER_CONSUME')
                OR (_slice_with_thread_and_process_info_0.name = 'ROUTER_CONSUME'))
                AND _slice_with_thread_and_process_info_0.ts AND (ts BETWEEN {selectivity_time_range.ts} AND {selectivity_time_range.ts + selectivity_time_range.dur}
            OR (ts + dur BETWEEN {selectivity_time_range.ts} AND {selectivity_time_range.ts + selectivity_time_range.dur}))
            GROUP BY args_1.display_value,  _slice_with_thread_and_process_info_0.name
            ORDER BY ({" + ".join(duration_columns)}) DESC"""
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
          {columns}
        FROM _slice_with_thread_and_process_info AS _slice_with_thread_and_process_info_0
        LEFT JOIN args AS args_1 ON args_1.arg_set_id = _slice_with_thread_and_process_info_0.arg_set_id AND args_1.key = 'debug.uuid'
        LEFT JOIN thread AS thread_2 ON thread_2.id = _slice_with_thread_and_process_info_0.utid
        WHERE ((_slice_with_thread_and_process_info_0.name = 'MEMMOVE_PROPAGATE') 
            OR (_slice_with_thread_and_process_info_0.name = 'MEMMOVE_GET_PROPAGATED') 
            OR (_slice_with_thread_and_process_info_0.name = 'MEMMOVE_CONSUME')
            OR (_slice_with_thread_and_process_info_0.name = 'MEMMOVE_WAITING_FOR_TRANSFER'))     
        AND (ts BETWEEN {selectivity_time_range.ts} AND {selectivity_time_range.ts + selectivity_time_range.dur}
            OR (ts + dur BETWEEN {selectivity_time_range.ts} AND {selectivity_time_range.ts + selectivity_time_range.dur}))
            GROUP BY args_1.display_value,  _slice_with_thread_and_process_info_0.name
            ORDER BY ({" + ".join(duration_columns)}) DESC""")
            qr_df = query_result.as_pandas_dataframe()
            print(qr_df.to_string())
            print("")


if __name__ == "__main__":
    main()
