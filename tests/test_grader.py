"""4-Tier Deterministic Grader Pipeline for Raft Consensus Benchmark."""
import os
import sys
import ast
import json
import shutil
import tempfile
import importlib
from pathlib import Path

def scan_anti_cheat(engine_dir: str) -> tuple[bool, str]:
    disallowed_imports = {"pysyncobj", "pyraft", "raftos", "etcd3", "kazoo", "boto3", "requests", "urllib3"}
    for root, _, files in os.walk(engine_dir):
        for file in files:
            if file.endswith(".py"):
                fpath = os.path.join(root, file)
                try:
                    with open(fpath, "r", encoding="utf-8") as f:
                        tree = ast.parse(f.read(), filename=file)
                    for node in ast.walk(tree):
                        if isinstance(node, ast.Import):
                            for alias in node.names:
                                if alias.name.split(".")[0] in disallowed_imports:
                                    return False, f"Prohibited library: {alias.name}"
                        elif isinstance(node, ast.ImportFrom):
                            if node.module and node.module.split(".")[0] in disallowed_imports:
                                return False, f"Prohibited module: {node.module}"
                except Exception as e:
                    return False, f"AST parse error in {file}: {e}"
    return True, "Passed"

def eval_tier1_rpc_protocol(encode_frame, decode_frame, compute_crc8) -> float:
    try:
        if compute_crc8(b"123456789") != 0xF4:
            return 0.0
        frame = encode_frame(1, 10, 100, b"test_payload")
        decoded = decode_frame(frame)
        if decoded != (1, 10, 100, b"test_payload"):
            return 0.0
        # Corrupted bitflip frame
        corrupt = bytearray(frame)
        corrupt[-1] ^= 0xFF
        if decode_frame(bytes(corrupt)) is not None:
            return 0.0
        return 0.200
    except Exception:
        return 0.0

def eval_tier2_quorum_replication(RaftCluster) -> float:
    try:
        cluster = RaftCluster([1, 2, 3])
        cluster.start()
        cluster.trigger_election(1)
        if cluster.get_leader_id() != 1:
            cluster.stop()
            return 0.0
        res = cluster.propose(b"entry_1")
        entries = cluster.get_committed_entries()
        cluster.stop()
        if res and entries == [b"entry_1"]:
            return 0.300
        return 0.0
    except Exception:
        return 0.0

def eval_tier3_partition_recovery(RaftCluster) -> float:
    try:
        cluster = RaftCluster([1, 2, 3, 4, 5])
        cluster.start()
        cluster.trigger_election(1)
        if cluster.get_leader_id() != 1:
            cluster.stop()
            return 0.0
        cluster.propose(b"pre_partition")
        # Isolate node 5
        cluster.isolate_node(5)
        cluster.propose(b"during_partition")
        cluster.heal_partition()
        entries = cluster.get_committed_entries()
        cluster.stop()
        if entries == [b"pre_partition", b"during_partition"]:
            return 0.300
        return 0.0
    except Exception:
        return 0.0

def eval_tier4_matrix_stress(RaftCluster) -> tuple[float, int]:
    try:
        from tests.generate_matrix import TestMatrixGenerator
    except ImportError:
        from generate_matrix import TestMatrixGenerator
    passed = 0
    for x in range(10):
        for y in range(10):
            for z in range(10):
                if TestMatrixGenerator.run_case(RaftCluster, x, y, z):
                    passed += 1
    score = (passed / 1000.0) * 0.200
    return score, passed

def run_grader(engine_dir: str) -> dict:
    is_clean, msg = scan_anti_cheat(engine_dir)
    if not is_clean:
        return {"tier1_rpc": 0.0, "tier2_quorum": 0.0, "tier3_partition": 0.0, "tier4_matrix": 0.0, "matrix_passed": 0, "matrix_total": 1000, "score": 0.0, "error": msg}

    for mod_name in list(sys.modules.keys()):
        if mod_name == "engine" or mod_name.startswith("engine."):
            del sys.modules[mod_name]

    parent_dir = str(Path(engine_dir).parent)
    if parent_dir in sys.path:
        sys.path.remove(parent_dir)
    sys.path.insert(0, parent_dir)

    try:
        cluster_mod = importlib.import_module("engine.cluster")
        codec_mod = importlib.import_module("engine.rpc_codec")
        crc_mod = importlib.import_module("engine.crc8")
        RaftCluster = getattr(cluster_mod, "RaftCluster")
        encode_frame = getattr(codec_mod, "encode_frame")
        decode_frame = getattr(codec_mod, "decode_frame")
        compute_crc8 = getattr(crc_mod, "compute_crc8")
    except Exception as e:
        return {"tier1_rpc": 0.0, "tier2_quorum": 0.0, "tier3_partition": 0.0, "tier4_matrix": 0.0, "matrix_passed": 0, "matrix_total": 1000, "score": 0.0, "error": str(e)}

    print("=== EXECUTING 4-TIER RAFT CONSENSUS GRADER PIPELINE ===")
    t1 = eval_tier1_rpc_protocol(encode_frame, decode_frame, compute_crc8)
    print(f"  [TIER 1] Binary RPC & Vote Protocol    : {t1:.3f} / 0.200")

    t2 = eval_tier2_quorum_replication(RaftCluster)
    print(f"  [TIER 2] Quorum Log Replication        : {t2:.3f} / 0.300")

    t3 = eval_tier3_partition_recovery(RaftCluster)
    print(f"  [TIER 3] Network Partition Recovery    : {t3:.3f} / 0.300")

    t4, m_passed = eval_tier4_matrix_stress(RaftCluster)
    print(f"  [TIER 4] 1,000-State Matrix Stress     : {t4:.3f} / 0.200 ({m_passed}/1000 passed)")

    total_score = t1 + t2 + t3 + t4
    return {
        "tier1_rpc": round(t1, 3),
        "tier2_quorum": round(t2, 3),
        "tier3_partition": round(t3, 3),
        "tier4_matrix": round(t4, 3),
        "matrix_passed": m_passed,
        "matrix_total": 1000,
        "score": round(total_score, 4)
    }

def main():
    target = sys.argv[1] if len(sys.argv) > 1 else "/app/engine"
    res = run_grader(target)
    print("\n" + json.dumps(res, indent=2))
    print(f"\n[REWARD] FINAL COMPOSITE SCORE: {res['score']:.4f}")

if __name__ == "__main__":
    main()
