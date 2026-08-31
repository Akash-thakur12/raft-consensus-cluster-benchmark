# Binary Raft Protocol Specification
- Magic Header: 0x52414654 (4B)
- Type (1B) + Sender (2B) + Term (8B) + Payload Length (2B) + Payload + CRC8 (1B)
