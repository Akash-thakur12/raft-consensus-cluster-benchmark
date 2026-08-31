"""Harbor Benchmark Verification Test for Raft Consensus Cluster."""
import os
import sys
import pytest
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))

from tests.test_grader import run_grader

def test_raft_cluster_benchmark():
    target_engine = os.environ.get("TARGET_ENGINE", "/app/engine")
    if not os.path.exists(target_engine):
        target_engine = str(SCRIPT_DIR.parent / "environment" / "engine")

    for mod_name in list(sys.modules.keys()):
        if mod_name == "engine" or mod_name.startswith("engine."):
            del sys.modules[mod_name]

    res = run_grader(target_engine)
    score = res.get("score", 0.0)

    reward_val = f"{score:.4f}\n"
    for rpath in ["/logs/verifier/reward.txt", "/app/reward.txt", "reward.txt"]:
        try:
            os.makedirs(os.path.dirname(rpath), exist_ok=True) if os.path.dirname(rpath) else None
            with open(rpath, "w", encoding="utf-8") as rf:
                rf.write(reward_val)
        except Exception:
            pass

    assert score >= 0.50, f"Benchmark score {score:.4f} below passing threshold 0.50 (Matrix passed: {res.get('matrix_passed', 0)}/1000)"
