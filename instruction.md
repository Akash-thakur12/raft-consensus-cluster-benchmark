# Raft Distributed Consensus Engine Benchmark

## Objective
Implement a fully functional, fault-tolerant **Raft Distributed Consensus Replicated State Machine** adhering to a strict binary RPC wire protocol, dynamic leader election, quorum log replication, and asymmetric network partition recovery across a **1,000-state combinatorial permutation matrix**.

---

## Technical Specifications & Binary Wire Protocol

### 1. Binary RPC Wire Format
All inter-node network packets must be serialized into fixed binary frames using big-endian byte order:
* **Magic Header (4 Bytes):** `0x52414654` (ASCII `"RAFT"`)
* **Message Type (1 Byte `uint8`):**
  * `0x01`: `REQUEST_VOTE`
  * `0x02`: `REQUEST_VOTE_RESP`
  * `0x03`: `APPEND_ENTRIES`
  * `0x04`: `APPEND_ENTRIES_RESP`
* **Sender ID (2 Bytes `uint16`):** Unique numerical ID of the sending node.
* **Term (8 Bytes `uint64`):** Current logical term counter.
* **Payload Length (2 Bytes `uint16`):** Byte length of the encapsulated payload $L_p$.
* **Payload ($L_p$ Bytes):** Serialized message arguments or log entries.
* **Checksum (1 Byte `uint8`):** CRC-8 (polynomial `0x07`) calculated over `Type + Sender ID + Term + Payload Length + Payload`.

### 2. Protocol Invariants & State Machine
1. **Node Roles:** Each node transitions between `FOLLOWER`, `CANDIDATE`, and `LEADER`.
2. **Leader Election:** 
   * Randomized election timeouts.
   * A candidate requests votes from all peers and becomes leader upon receiving a strict majority quorum ($> N/2$).
   * A vote is granted only if the candidate's term is greater than or equal to the receiver's term, the candidate has not voted in this term, and the candidate's log is at least as up-to-date as the receiver's log.
3. **Log Replication:**
   * Leaders broadcast periodic heartbeats (`APPEND_ENTRIES` with empty entries).
   * Follower checks `prev_log_index` and `prev_log_term`. If mismatched, it rejects the append.
   * Leader increments `commit_index` once a log entry has been replicated to a majority quorum.
4. **Partition Tolerance:**
   * Split-brain partitions must isolate minority nodes from committing entries until healed and reconciled.

---

## Public API Contract

The candidate implementation must expose the following coordinator classes under `engine.cluster` and `engine.raft_node`:

### `RaftCluster(node_ids: list[int], data_dir: str = None)`
* `start()`: Initializes and starts all nodes in the cluster.
* `stop()`: Gracefully halts all nodes.
* `propose(command: bytes) -> bool`: Submits a client command to the current leader for quorum consensus.
* `get_committed_entries() -> list[bytes]`: Returns all state-machine applied entries in committed index order.
* `isolate_node(node_id: int)`: Simulates complete network partition for a specific node.
* `heal_partition()`: Reconnects all partitioned nodes and bridges network communications.
* `get_leader_id() -> int | None`: Returns the active leader node ID or `None`.

---

## Grading, Scoring & Partial Credit

The evaluation pipeline (`tests/test_outputs.py`) executes a 4-tier evaluation suite:

| Tier | Component | Weight | Criteria |
|:---|:---|:---:|:---|
| **Tier 1** | **In-Memory RPC & Vote Protocol** | `0.200` | Validates CRC-8, binary wire framing, and majority vote tallying. |
| **Tier 2** | **Quorum Log Replication** | `0.300` | Validates leader election, log agreement, and commit index advancement. |
| **Tier 3** | **Network Partition Recovery** | `0.300` | Validates split-brain isolation and historical log divergence healing. |
| **Tier 4** | **1,000-State Combinatorial Matrix** | `0.200` | Stress tests 1,000 randomized permutations of drops, term churn, and delayed packets. |

* **Total Score:** $\sum 	ext{Tiers} = \mathbf{1.000}$
* **Passing Threshold:** $	ext{Score} \ge \mathbf{0.500}$
