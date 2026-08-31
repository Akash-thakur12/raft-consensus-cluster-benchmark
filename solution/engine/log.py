"""Replicated State Machine Log Manager."""

class LogEntry:
    def __init__(self, index: int, term: int, command: bytes):
        self.index = index
        self.term = term
        self.command = command

    def serialize(self) -> bytes:
        import struct
        cmd_len = len(self.command)
        return struct.pack(">QQH", self.index, self.term, cmd_len) + self.command

    @staticmethod
    def deserialize(data: bytes, offset: int = 0) -> tuple["LogEntry", int]:
        import struct
        idx, term, cmd_len = struct.unpack_from(">QQH", data, offset)
        cmd = data[offset + 18 : offset + 18 + cmd_len]
        return LogEntry(idx, term, cmd), 18 + cmd_len


class RaftLog:
    def __init__(self):
        self.entries: list[LogEntry] = []

    def append(self, term: int, command: bytes) -> int:
        idx = len(self.entries) + 1
        entry = LogEntry(idx, term, command)
        self.entries.append(entry)
        return idx

    def get_last_log_info(self) -> tuple[int, int]:
        if not self.entries:
            return 0, 0
        return self.entries[-1].index, self.entries[-1].term

    def truncate_from(self, start_index: int):
        self.entries = [e for e in self.entries if e.index < start_index]

    def get_entries_from(self, start_index: int) -> list[LogEntry]:
        return [e for e in self.entries if e.index >= start_index]
