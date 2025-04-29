from pathlib import Path
from typing import List, Dict
import duckdb
import json
import numpy as np
from datetime import datetime

DuckdbTypeToProteusType: Dict[str, str] = {
    "BIGINT": "int64",
    "LONG": "int64",
    "INT8": "int64",
    "BOOLEAN": "int",  # needs fixing at somepoint
    "INTEGER": "int",
    "TIMESTAMP": "datetime",
    "DOUBLE": "float",
    "DATE": "date",
}


class ParquetRelationConverter:
    def __init__(self, parquet_files: List[str], schema_name: str, rel_name: str, output_dir: Path = Path("inputs")):
        """
        Convert a single relation schema from a set of parquet files
        :param parquet_files: List of parquet files to convert. Assumes that all files have the same schema
        :param output_dir: Top level output directory. Outputs will be written to output_dir/schema
        :param schema_name: name for schema containing this relation (does not impact data conversion, only output names)
        :param rel_name: name for the relation (does not impact data conversion, only output names)
        """
        for file in parquet_files:
            if not Path(file).exists():
                raise FileNotFoundError(f"File not found: {file}")
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / schema_name).mkdir(parents=True, exist_ok=True)
        self.parquet_files_string = ",".join([f'\'{x}\'' for x in parquet_files])
        self.output_dir = output_dir
        self.con = duckdb.connect(':memory:')
        self.schema_name = schema_name
        self.rel_name = rel_name

    def _write_int64_column(self, output_attr_bin_path, data):
        if data.dtype != np.dtype('int64'):
            raise EncodingWarning(f"Expected np.int64 for {output_attr_bin_path} got {data.dtype}")
        with open(f"{output_attr_bin_path}", 'wb') as f:
            f.write(data.tobytes())

    def _write_int32_column(self, output_attr_bin_path, data):
        if data.dtype != np.dtype('int32'):
            raise EncodingWarning(f"Expected np.int32 for {output_attr_bin_path} got {data.dtype}")
        with open(f"{output_attr_bin_path}", 'wb') as f:
            f.write(data.tobytes())

    def _write_double_column(self, output_attr_bin_path, data):
        if data.dtype != np.dtype('float64'):
            raise EncodingWarning(f"Expected np.float64 for {output_attr_bin_path} got {data.dtype}")
        # data = data.byteswap()
        with open(f"{output_attr_bin_path}", 'wb') as f:
            f.write(data.tobytes())

    def _write_datetime64_column(self, output_attr_bin_path, data):
        """
        numpy datetime64 is an 8 byte count of time units since Unix epoch (UTC)
        We expect it in us, and Proteus operates in ms
        """
        if data.dtype != np.dtype('datetime64[us]'):
            raise EncodingWarning(f"Expected datetime64[us]for {output_attr_bin_path} got {data.dtype}")
        ms_data = (data.astype('int64') // 1000).astype('int64')
        with open(f"{output_attr_bin_path}", 'wb') as f:
            f.write(ms_data.tobytes())

    def _convert_attribute(self, relPathName: str, output_path_prefix: str, duckdb_type: str, attrName: str,
                           attrNo: int):
        """
        :param relPathName: path to and schema/name of the relation from point of view of the catalog.json e.g. inputs/taxi/yellow_tripdata.csv
        :param output_path_prefix: prefix of path to write outputs to, e.g. /my_specific/path/for/outputs/of/this/script/taxi/yellow_tripdata.csv
        :param duckdb_type: DuckDB type. Not all types yet supported. One issue is that all strings in duckdb are
        varchar, regardless if the underlying data in the parquet files might be fixed length.
        :param attrName: Name of the attribute
        :param attrNo: Number of the attribute, 1-indexed
        :return: Metadata object for the attribute, to be used for writing out the catalog.json
        """
        if duckdb_type not in DuckdbTypeToProteusType:
            raise LookupError(f"DuckDB type not supported: {duckdb_type} for attr {attrName}")

        data = self.con.execute(
            f"select {attrName} from read_parquet([{self.parquet_files_string}])").fetchnumpy()
        attr_bin_file = f"{output_path_prefix}.{attrName}"
        if Path(attr_bin_file).exists():
            print(f"WARNING {attr_bin_file} already exists. Overwriting")
        else:
            print(f"Writing to {attr_bin_file}")
        proteus_type = DuckdbTypeToProteusType[duckdb_type]
        if proteus_type == "int64":
            self._write_int64_column(attr_bin_file, data[f'{attrName}'])

        if proteus_type == "int":
            self._write_int32_column(attr_bin_file, data[f'{attrName}'])

        if proteus_type == "float":
            self._write_double_column(attr_bin_file, data[f'{attrName}'])

        if proteus_type == "datetime":
            self._write_datetime64_column(attr_bin_file, data[f'{attrName}'])

        return ({
            "type": {
                "type": proteus_type
            },
            "relName": relPathName,
            "attrName": attrName,
            "attrNo": attrNo
        })

    def convert_relation(self, skip_columns: List[str] = []) -> None:
        """
        Perform the conversion of a single relation schema
        :param skip_columns: Columns in the parquet files to skip
        """
        schema_info = self.con.sql(f"DESCRIBE SELECT * FROM read_parquet([{self.parquet_files_string}]);")
        schema_rows = schema_info.fetchall()

        # for legacy reasons, Proteus expects relation names to end in .csv
        # In more detail: query shaper appends .csv to the relation name when you scan the relation
        # It isn't really a core issue, but changing it breaks existing catalog.json files
        rel_path_name = f"inputs/{self.schema_name}/{self.rel_name}.csv"
        attr_output_prefix = f"{self.output_dir}/{self.schema_name}/{self.rel_name}.csv"
        attr_num = 1
        attr_meta = []
        for row_tuple in schema_rows:
            column_name = row_tuple[0]
            column_type = row_tuple[1]
            if column_name not in skip_columns:
                print(f"Converting column {attr_num} of {len(schema_rows)}. DuckDB type: {column_type}")
                attr_meta.append(
                    self._convert_attribute(rel_path_name, attr_output_prefix, column_type, column_name, attr_num))
            attr_num += 1

        row_count_res = self.con.execute(
            f"select COUNT(*) as row_count from read_parquet([{self.parquet_files_string}])").fetchnumpy()
        row_count = int(row_count_res['row_count'][0])

        time_now = datetime.now().strftime("%Y-%m-%d %H:%M")
        catalog = {
            f"inputs_{self.schema_name}_{self.rel_name}": {
                "_comment": f"Prepared by ParquetRelationConverter on {time_now} from {self.parquet_files_string}",
                "path": rel_path_name,
                "type": {
                    "type": "bag",
                    "inner": {
                        "type": "record",
                        "attributes": attr_meta
                    }
                },
                "plugin": {
                    "type": "block",
                    "linehint": row_count
                }
            }
        }
        with open(f'{self.output_dir}/{self.schema_name}/catalog.json', 'w') as out:
            out.write(json.dumps(catalog, indent=4))
            out.write('\n')
