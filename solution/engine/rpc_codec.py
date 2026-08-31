"""Binary RPC Wire Encoder/Decoder for Raft Protocol."""
import struct
from engine.crc8 import compute_crc8

MAGIC = 0x52414654  # "RAFT"
MSG_REQUEST_VOTE = 0x01
MSG_REQUEST_VOTE_RESP = 0x02
MSG_APPEND_ENTRIES = 0x03
MSG_APPEND_ENTRIES_RESP = 0x04

HEADER_SIZE = 17  # magic(4) + type(1) + sender(2) + term(8) + payload_len(2)


def encode_frame(msg_type: int, sender_id: int, term: int, payload: bytes) -> bytes:
    """Encodes message into binary frame: Header(17B) + Payload + CRC8(1B)."""
    payload_len = len(payload)
    header = struct.pack(">IBHQH", MAGIC, msg_type, sender_id, term, payload_len)
    body = header[4:] + payload  # CRC over type + sender + term + payload_len + payload
    crc = compute_crc8(body)
    return header + payload + bytes([crc])


def decode_frame(raw_data: bytes) -> tuple[int, int, int, bytes] | None:
    """Decodes and validates binary frame, returning (msg_type, sender_id, term, payload) or None."""
    if len(raw_data) < HEADER_SIZE + 1:
        return None

    magic, msg_type, sender_id, term, payload_len = struct.unpack_from(">IBHQH", raw_data, 0)
    if magic != MAGIC:
        return None

    total_len = HEADER_SIZE + payload_len + 1
    if len(raw_data) < total_len:
        return None

    payload = raw_data[HEADER_SIZE : HEADER_SIZE + payload_len]
    expected_crc = raw_data[HEADER_SIZE + payload_len]

    body = raw_data[4 : HEADER_SIZE + payload_len]
    if compute_crc8(body) != expected_crc:
        return None

    return msg_type, sender_id, term, payload
