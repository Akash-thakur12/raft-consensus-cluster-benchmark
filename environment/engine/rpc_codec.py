"""Binary RPC Wire Encoder/Decoder for Raft Protocol."""

def encode_frame(msg_type: int, sender_id: int, term: int, payload: bytes) -> bytes:
    return b""

def decode_frame(raw_data: bytes):
    return None
