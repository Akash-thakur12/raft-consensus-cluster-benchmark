"""1,000-State Combinatorial Vector Generator for Raft Consensus Cluster."""

class TestMatrixGenerator:
    @staticmethod
    def get_command_variant(x: int, seq: int) -> bytes:
        if x == 0:
            return f"cmd_{seq}".encode("ascii")
        elif x == 1:
            primes = [17, 31, 47, 79, 101]
            p = primes[seq % len(primes)]
            return (f"prime_{seq}_" + "X" * p)[:p].encode("latin1")
        elif x == 2:
            return b"null\x00byte\x00cmd_" + str(seq).encode("ascii")
        elif x == 3:
            return f"🔑_tx_{seq}_🚀".encode("utf-8")
        elif x == 4:
            return bytes([(seq + i) % 256 for i in range(32)])
        elif x == 5:
            return f"pad_{seq}".encode("ascii") + b"\x00" * 16
        elif x == 6:
            return (f"large_cmd_{seq}_" + "A" * 256)[:256].encode("ascii")
        elif x == 7:
            return b""
        elif x == 8:
            return f"asc_cmd_{seq:08d}".encode("ascii")
        else:
            return f"desc_cmd_{100000 - seq:08d}".encode("ascii")

    @staticmethod
    def run_case(cluster_cls, x: int, y: int, z: int) -> bool:
        try:
            nodes = [1, 2, 3] if z < 5 else [1, 2, 3, 4, 5]
            cluster = cluster_cls(nodes)
            cluster.start()

            # Election
            cand = (y % len(nodes)) + 1
            cluster.trigger_election(cand)
            leader = cluster.get_leader_id()
            if leader is None:
                cluster.stop()
                return False

            # Proposal
            c1 = TestMatrixGenerator.get_command_variant(x, 10)
            res = cluster.propose(c1)
            if not res:
                cluster.stop()
                return False

            # Partition injection
            if y >= 4:
                non_leader = [n for n in nodes if n != leader][0]
                cluster.isolate_node(non_leader)
                c2 = TestMatrixGenerator.get_command_variant(x, 20)
                cluster.propose(c2)
                cluster.heal_partition()

            entries = cluster.get_committed_entries()
            if not isinstance(entries, list) or len(entries) == 0:
                cluster.stop()
                return False
            if entries[0] != c1:
                cluster.stop()
                return False

            cluster.stop()
            return True
        except Exception:
            return False
