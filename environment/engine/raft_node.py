"""Core Raft Consensus Node State Machine."""
from enum import Enum

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
        self.role = NodeRole.FOLLOWER

    def start(self):
        pass

    def stop(self):
        pass
