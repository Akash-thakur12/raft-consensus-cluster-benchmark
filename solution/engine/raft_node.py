"""Core Raft Consensus Node State Machine."""
from enum import Enum
from engine.log import RaftLog
from engine.rpc_codec import (
    MSG_REQUEST_VOTE, MSG_REQUEST_VOTE_RESP,
    MSG_APPEND_ENTRIES, MSG_APPEND_ENTRIES_RESP,
    encode_frame, decode_frame
)


class NodeRole(Enum):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"
    STOPPED = "STOPPED"


class RaftNode:
    def __init__(self, node_id: int, peer_ids: list[int]):
        self.node_id = node_id
        self.peer_ids = peer_ids
        self.current_term = 0
        self.voted_for = None
        self.log = RaftLog()
        self.commit_index = 0
        self.last_applied = 0
        self.role = NodeRole.FOLLOWER
        self.votes_received = set()

        # Leader tracking state
        self.next_index: dict[int, int] = {}
        self.match_index: dict[int, int] = {}

    def start(self):
        self.role = NodeRole.FOLLOWER

    def stop(self):
        self.role = NodeRole.STOPPED

    def start_election(self) -> list[tuple[int, bytes]]:
        self.current_term += 1
        self.role = NodeRole.CANDIDATE
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}

        last_idx, last_term = self.log.get_last_log_info()
        outbox = []
        for peer in self.peer_ids:
            import struct
            payload = struct.pack(">QQ", last_idx, last_term)
            frame = encode_frame(MSG_REQUEST_VOTE, self.node_id, self.current_term, payload)
            outbox.append((peer, frame))
        return outbox

    def handle_message(self, raw_frame: bytes) -> list[tuple[int, bytes]]:
        decoded = decode_frame(raw_frame)
        if decoded is None:
            return []

        msg_type, sender_id, term, payload = decoded
        outbox = []

        if term > self.current_term:
            self.current_term = term
            self.role = NodeRole.FOLLOWER
            self.voted_for = None

        if msg_type == MSG_REQUEST_VOTE:
            import struct
            last_idx, last_term = struct.unpack(">QQ", payload)
            my_last_idx, my_last_term = self.log.get_last_log_info()

            log_ok = (last_term > my_last_term) or (last_term == my_last_term and last_idx >= my_last_idx)
            vote_granted = False

            if (self.voted_for is None or self.voted_for == sender_id) and log_ok and term >= self.current_term:
                self.voted_for = sender_id
                vote_granted = True

            resp_payload = struct.pack(">B", 1 if vote_granted else 0)
            resp = encode_frame(MSG_REQUEST_VOTE_RESP, self.node_id, self.current_term, resp_payload)
            outbox.append((sender_id, resp))

        elif msg_type == MSG_REQUEST_VOTE_RESP:
            if self.role == NodeRole.CANDIDATE and term == self.current_term:
                import struct
                granted = struct.unpack(">B", payload)[0] == 1
                if granted:
                    self.votes_received.add(sender_id)
                    total_cluster = len(self.peer_ids) + 1
                    if len(self.votes_received) > total_cluster // 2:
                        self.role = NodeRole.LEADER
                        for p in self.peer_ids:
                            self.next_index[p] = len(self.log.entries) + 1
                            self.match_index[p] = 0

        elif msg_type == MSG_APPEND_ENTRIES:
            import struct
            prev_idx, prev_term, leader_commit = struct.unpack_from(">QQQ", payload, 0)
            success = False

            if term >= self.current_term:
                self.role = NodeRole.FOLLOWER
                # Check prev log consistency
                if prev_idx == 0:
                    success = True
                elif prev_idx <= len(self.log.entries) and self.log.entries[prev_idx - 1].term == prev_term:
                    success = True

                if success:
                    # Append new entries if any
                    offset = 24
                    while offset < len(payload):
                        from engine.log import LogEntry
                        entry, read_bytes = LogEntry.deserialize(payload, offset)
                        if entry.index <= len(self.log.entries):
                            if self.log.entries[entry.index - 1].term != entry.term:
                                self.log.truncate_from(entry.index)
                                self.log.entries.append(entry)
                        else:
                            self.log.entries.append(entry)
                        offset += read_bytes

                    if leader_commit > self.commit_index:
                        self.commit_index = min(leader_commit, len(self.log.entries))

            resp_payload = struct.pack(">BQ", 1 if success else 0, len(self.log.entries))
            resp = encode_frame(MSG_APPEND_ENTRIES_RESP, self.node_id, self.current_term, resp_payload)
            outbox.append((sender_id, resp))

        elif msg_type == MSG_APPEND_ENTRIES_RESP:
            if self.role == NodeRole.LEADER and term == self.current_term:
                import struct
                success, match_len = struct.unpack(">BQ", payload)
                if success:
                    self.match_index[sender_id] = match_len
                    self.next_index[sender_id] = match_len + 1

                    # Check majority match for commit index
                    total_nodes = len(self.peer_ids) + 1
                    for idx in range(len(self.log.entries), self.commit_index, -1):
                        match_count = 1  # leader itself
                        for p in self.peer_ids:
                            if self.match_index.get(p, 0) >= idx:
                                match_count += 1
                        if match_count > total_nodes // 2 and self.log.entries[idx - 1].term == self.current_term:
                            self.commit_index = idx
                            break

        return outbox
