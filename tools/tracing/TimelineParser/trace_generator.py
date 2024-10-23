from . import perfetto_trace_pb2 as pb2
from typing import Optional

# Set this to true if you want to dump the protobuf results to stdout.
print_proto = False

"""
Based on https://github.com/ihavnoid/tg4perfetto/tree/main
"""


class CounterTrack:
    def __init__(self, name, parent, uuid):
        self._parent = parent
        self._uuid = uuid
        self._name = name

    def count(self, ts, value):
        """ Add a count value on the track. """
        self._parent._track_count(self._uuid, ts, value)
        return self


class NormalTrack:
    def __init__(self, name, parent, uuid):
        self._parent = parent
        self._uuid = uuid
        self._name = name

    def open(self, ts, annotation, kwargs=None, flow: Optional[list[int]] = None):
        """ Open a track. """
        self._parent._track_open(self._uuid, ts, annotation, kwargs, flow)
        return self

    def close(self, ts, flow: Optional[list[int]] = None):
        """ Close a track.  The last 'open' call is closed """
        self._parent._track_close(self._uuid, ts, flow)
        return self

    def instant(self, ts, annotation, kwargs=None, flow: Optional[list[int]] = None):
        """ Record an instant event. """
        if flow is None:
            flow = []
        self._parent._track_instant(self._uuid, ts, annotation, kwargs, flow)
        return self


class GroupTrack:
    def __init__(self, name, parent, uuid):
        self._parent = parent
        self._uuid = uuid
        self._name = name

    def create_track(self) -> NormalTrack:
        """ Create a child track of this group track."""
        return self._parent._create_track(self._uuid, self._name, 0)


class Group:
    def __init__(self, name, parent, uuid):
        self._parent = parent
        self._uuid = uuid

    def create_track(self, track_name: str, thread_id: Optional[int] = None) -> NormalTrack:
        """ Create a normal track for this track."""
        return self._parent._create_track(self._uuid, track_name, 0, thread_id)

    def create_counter_track(self, track_name: str) -> CounterTrack:
        """ Create a counter track.  Counter tracks can be used for recording int values."""
        return self._parent._create_track(self._uuid, track_name, 1)

    def create_group(self, track_name: str) -> GroupTrack:
        """ Create a group track.  Group tracks can be used for grouping normal tracks."""
        return self._parent._create_track(self._uuid, track_name, 2)

    def open(self, ts: int, annotation: str, kwargs: dict = None, flow: Optional[list[int]] = None):
        """ Open track. """
        self._parent._track_open(self._uuid, ts, annotation, kwargs, flow)

    def close(self, ts: int, flow: Optional[list[int]] = None):
        """ Close a track.  The last 'open' call is closed """
        self._parent._track_close(self._uuid, ts, flow)

    def instant(self, ts: int, annotation: str, kwargs: dict = None, flow: Optional[list[int]] = None):
        """ Record an instant event. """
        self._parent._track_instant(self._uuid, ts, annotation, kwargs, flow)


class TraceGenerator:
    def __init__(self, filename: str):
        """ Create a trace """
        self.__uuid__ = 1234567
        self.interned_data = {}
        self.interned_source = {}
        self.flush_threshold = 10000
        self.list_max_size = 16
        self.__gid__ = 1

        self.trace = pb2.Trace()
        self.file = open(filename, "wb")

        pkt = self.trace.packet.add()
        pkt.trusted_packet_sequence_id = 1
        clk = pkt.clock_snapshot.clocks.add()
        clk.clock_id = 1
        clk.timestamp = 0
        clk = pkt.clock_snapshot.clocks.add()
        clk.clock_id = 2
        clk.timestamp = 0
        clk = pkt.clock_snapshot.clocks.add()
        clk.clock_id = 3
        clk.timestamp = 0
        clk = pkt.clock_snapshot.clocks.add()
        clk.clock_id = 4
        clk.timestamp = 0
        clk = pkt.clock_snapshot.clocks.add()
        clk.clock_id = 5
        clk.timestamp = 0
        clk = pkt.clock_snapshot.clocks.add()
        clk.clock_id = 6
        clk.timestamp = 0
        pkt.clock_snapshot.primary_trace_clock = pb2.BUILTIN_CLOCK_BOOTTIME

        pkt = self.trace.packet.add()
        pkt.trusted_packet_sequence_id = 1
        pkt.trace_config.buffers.add().size_kb = 1024
        pkt.trace_config.data_sources.add().config.name = "track_event"

        pkt = self.trace.packet.add()
        pkt.trusted_packet_sequence_id = 2
        pkt.trace_packet_defaults.track_event_defaults.track_uuid = 1
        pkt.trace_packet_defaults.timestamp_clock_id = 1
        pkt.sequence_flags = 1

    def flush(self):
        """ Flush trace.  This creates a perfetto trace packet and writes to disk. """
        if print_proto:
            print(self.trace)
        self.file.write(self.trace.SerializeToString())
        self.file.flush()
        self.trace = pb2.Trace()

    def __del__(self):
        self.flush()
        self.file.close()

    def _gid_packet(self, gid, process_name: str, track_name: str = None):
        """ Create a group.  Each "group" comes with a default normal track (named track_name)."""
        uuid = self.__uuid__
        pkt = self.trace.packet.add()
        pkt.timestamp = 0
        pkt.track_descriptor.uuid = uuid
        pkt.track_descriptor.process.pid = 1  # only parsing for one process
        pkt.track_descriptor.process.process_name = process_name
        pkt.trusted_packet_sequence_id = 2
        pkt.sequence_flags = 2
        if track_name is None:
            pkt.track_descriptor.name = process_name
        else:
            pkt.track_descriptor.name = track_name

        self.__uuid__ += 1

        self._flush_if_necessary()

        return uuid

    def _flush_if_necessary(self):
        if len(self.trace.packet) > self.flush_threshold:
            self.flush()

    def _tid_packet(self, my_gid, parent_uuid, track_name, track_type, thread_id=None):
        pkt = self.trace.packet.add()

        pkt.timestamp = 0
        pkt.trusted_packet_sequence_id = 2
        pkt.sequence_flags = 2
        uuid = self.__uuid__

        pkt.track_descriptor.uuid = uuid
        self.__uuid__ += 1
        pkt.track_descriptor.name = track_name
        if thread_id is not None:
            pkt.track_descriptor.thread.tid = thread_id
            pkt.track_descriptor.thread.pid = 1

        if parent_uuid != 0:
            pkt.track_descriptor.parent_uuid = parent_uuid

        if track_type == 1:
            pkt.track_descriptor.counter.categories.append("dummy")
        self._flush_if_necessary()

        return uuid

    def _get_iid_for(self, pkt, name):
        if name in self.interned_data:
            return self.interned_data[name]

        ev = pkt.interned_data.event_names.add()
        ev.name = name
        ev.iid = len(self.interned_data) + 1

        self.interned_data[name] = ev.iid
        return ev.iid

    def _add_debug_annotation(self, d, kwargs):
        cnt = 0
        for k, v in kwargs.items():
            cnt += 1
            x = d.add()
            if cnt == self.list_max_size:
                x.name = "..."
                x.string_value = "({} more items)".format(len(kwargs) - cnt)
                break

            x.name = str(k)

            def set_single(x, v):
                if isinstance(v, str):
                    x.string_value = v
                elif isinstance(v, bool):
                    x.bool_value = v
                elif isinstance(v, int):
                    x.int_value = v
                elif isinstance(v, float):
                    x.double_value = v
                elif isinstance(v, dict):
                    if len(v) == 0:
                        x.string_value = "[empty]"
                    else:
                        self._add_debug_annotation(x.dict_entries, v)
                elif isinstance(v, list) or isinstance(v, tuple):
                    if len(v) == 0:
                        x.string_value = "[empty]"
                    else:
                        for i, vv in zip(range(len(v)), v):
                            if i == self.list_max_size:
                                set_single(x.array_values.add(), "... ({} more items)".format(len(v) - i))
                                break
                            # for some reason, perfetto ui crashes on nested lists.
                            # add a dummy dictionary here
                            if isinstance(vv, list) or isinstance(vv, tuple):
                                vv = {"array": vv}
                            set_single(x.array_values.add(), vv)
                else:
                    x.string_value = str(type(v))

            set_single(x, v)

    def _track_instant(self, uuid, ts, annotation, kwargs, flow: list[int], caller=None):
        pkt = self.trace.packet.add()

        pkt.timestamp = ts
        pkt.trusted_packet_sequence_id = 2
        pkt.sequence_flags = 2
        pkt.track_event.category_iids.append(1)
        pkt.track_event.type = pb2.TrackEvent.TYPE_INSTANT
        pkt.track_event.track_uuid = uuid
        pkt.track_event.name = annotation

        if kwargs is not None:
            self._add_debug_annotation(pkt.track_event.debug_annotations, kwargs)

        for x in flow:
            pkt.track_event.flow_ids.append(x)

        if caller is not None:
            file, line, name = caller
            iid = self._get_source_iid_for(pkt, file, name, line)
            pkt.track_event.source_location_iid = iid

        self._flush_if_necessary()

    def _get_source_iid_for(self, pkt, file, name, line):
        if (file, name, line) in self.interned_source:
            return self.interned_source[(file, name, line)]
        ev = pkt.interned_data.source_locations.add()
        ev.file_name = file
        ev.function_name = name
        ev.line_number = line
        ev.iid = len(self.interned_source) + 1
        self.interned_source[(file, name, line)] = ev.iid

        return ev.iid

    def _track_open(self, uuid, ts, annotation, kwargs, flow: Optional[list[int]], caller=None):
        pkt = self.trace.packet.add()

        pkt.timestamp = ts
        pkt.track_event.name_iid = self._get_iid_for(pkt, annotation)
        pkt.trusted_packet_sequence_id = 2
        pkt.sequence_flags = 2
        pkt.track_event.category_iids.append(1)
        pkt.track_event.type = pb2.TrackEvent.TYPE_SLICE_BEGIN
        pkt.track_event.track_uuid = uuid

        if kwargs is not None:
            self._add_debug_annotation(pkt.track_event.debug_annotations, kwargs)
        if flow is not None:
            for x in flow:
                pkt.track_event.flow_ids.append(x)

        if caller is not None:
            file, line, name = caller
            iid = self._get_source_iid_for(pkt, file, name, line)
            pkt.track_event.source_location_iid = iid

        self._flush_if_necessary()

    def _track_close(self, uuid, ts, flow: Optional[list[int]] = None):
        pkt = self.trace.packet.add()

        pkt.trusted_packet_sequence_id = 2
        pkt.sequence_flags = 2
        pkt.timestamp = ts
        pkt.track_event.track_uuid = uuid
        pkt.track_event.type = pb2.TrackEvent.TYPE_SLICE_END
        if flow is not None:
            for x in flow:
                pkt.track_event.flow_ids.append(x)

        self._flush_if_necessary()

    def _track_count(self, uuid, ts, value):
        pkt = self.trace.packet.add()

        pkt.timestamp = ts
        pkt.trusted_packet_sequence_id = 2
        pkt.sequence_flags = 2
        pkt.track_event.type = pb2.TrackEvent.TYPE_COUNTER
        pkt.track_event.track_uuid = uuid
        pkt.track_event.counter_value = value

        self._flush_if_necessary()

    def create_group(self, process_name: str, track_name: str = None) -> Group:
        """ Create a group.  Each "group" comes with a default normal track (named track_name)."""
        gid = self.__gid__
        self.__gid__ += 1

        uuid = self._gid_packet(gid, process_name, track_name)
        return Group(process_name, self, uuid)

    def _create_track(self, parent_uuid, track_name: str, ttype, thread_id: Optional[int] = None):
        tid = self.__gid__
        self.__gid__ += 1

        uuid = self._tid_packet(tid, parent_uuid, track_name, ttype, thread_id)

        if ttype == 0:
            return NormalTrack(track_name, self, uuid)
        elif ttype == 1:
            return CounterTrack(track_name, self, uuid)
        elif ttype == 2:
            return GroupTrack(track_name, self, uuid)
        else:
            assert False

    def create_counter_track(self, track_name: str):
        """ Create a global counter track """
        return self._create_track(0, track_name, 1)
