"""Discrete Cluster Simulator and Raft Coordinator."""

class RaftCluster:
    def __init__(self, node_ids: list[int], data_dir: str = None):
        self.node_ids = node_ids
        self.data_dir = data_dir

    def start(self):
        pass

    def stop(self):
        pass

    def isolate_node(self, node_id: int):
        pass

    def heal_partition(self):
        pass

    def get_leader_id(self):
        return None

    def trigger_election(self, candidate_id: int):
        pass

    def propose(self, command: bytes) -> bool:
        return False

    def get_committed_entries(self) -> list[bytes]:
        return []
