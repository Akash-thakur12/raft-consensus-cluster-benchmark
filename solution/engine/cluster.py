"""Discrete Cluster Simulator and Raft Coordinator."""
from engine.raft_node import RaftNode, NodeRole
from engine.rpc_codec import MSG_APPEND_ENTRIES, encode_frame


class RaftCluster:
    def __init__(self, node_ids: list[int], data_dir: str = None):
        self.node_ids = node_ids
        self.data_dir = data_dir
        self.nodes: dict[int, RaftNode] = {}
        for nid in node_ids:
            peers = [p for p in node_ids if p != nid]
            self.nodes[nid] = RaftNode(nid, peers)
        self.isolated_nodes = set()

    def start(self):
        for node in self.nodes.values():
            node.start()

    def stop(self):
        for node in self.nodes.values():
            node.stop()

    def isolate_node(self, node_id: int):
        self.isolated_nodes.add(node_id)

    def heal_partition(self):
        self.isolated_nodes.clear()

    def get_leader_id(self) -> int | None:
        for nid, node in self.nodes.items():
            if nid not in self.isolated_nodes and node.role == NodeRole.LEADER:
                return nid
        return None

    def trigger_election(self, candidate_id: int):
        if candidate_id in self.isolated_nodes:
            return
        node = self.nodes[candidate_id]
        outbox = node.start_election()
        self._route_messages(outbox)

    def propose(self, command: bytes) -> bool:
        leader_id = self.get_leader_id()
        if leader_id is None:
            return False

        leader = self.nodes[leader_id]
        idx = leader.log.append(leader.current_term, command)

        # Broadcast append entries to all peers
        outbox = []
        for p in leader.peer_ids:
            prev_idx = idx - 1
            prev_term = leader.log.entries[prev_idx - 1].term if prev_idx > 0 else 0
            import struct
            entry_bytes = leader.log.entries[idx - 1].serialize()
            payload = struct.pack(">QQQ", prev_idx, prev_term, leader.commit_index) + entry_bytes
            frame = encode_frame(MSG_APPEND_ENTRIES, leader.node_id, leader.current_term, payload)
            outbox.append((p, frame))

        self._route_messages(outbox)
        return leader.commit_index >= idx

    def _route_messages(self, outbox: list[tuple[int, bytes]]):
        queue = list(outbox)
        while queue:
            dest, frame = queue.pop(0)
            if dest in self.isolated_nodes:
                continue
            dest_node = self.nodes[dest]
            replies = dest_node.handle_message(frame)
            for reply_dest, reply_frame in replies:
                if reply_dest not in self.isolated_nodes:
                    queue.append((reply_dest, reply_frame))

    def get_committed_entries(self) -> list[bytes]:
        leader_id = self.get_leader_id()
        if leader_id is not None:
            leader = self.nodes[leader_id]
            return [e.command for e in leader.log.entries[: leader.commit_index]]
        # Fallback to majority consensus entries
        for node in self.nodes.values():
            if node.commit_index > 0:
                return [e.command for e in node.log.entries[: node.commit_index]]
        return []
