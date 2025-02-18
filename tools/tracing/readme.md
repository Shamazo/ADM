## Timeline Parser
This directory contains a python script (`cli.py`) for parsing proteus trace logs and converting them into the [Perfetto](https://perfetto.dev/) trace format.
Converted trace files can be viewed in the Perfetto trace viewer: [https://ui.perfetto.dev](https://ui.perfetto.dev).
This format is a protobuf format defined [here](https://github.com/google/perfetto/blob/main/protos/perfetto/trace/perfetto_trace.proto).
The proto file is stored here along with the compiled python bindings. 
If the proto file is updated, please recompile (` cd TimelineParser && protoc  --python_out=. perfetto_trace.proto
`) and commit the updated python and compiled bindings in the same commit. 