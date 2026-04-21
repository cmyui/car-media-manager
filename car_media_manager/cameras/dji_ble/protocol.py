"""DJI R SDK protocol frame encoding and command builders.

Frame layout (all little-endian):
    SOF        (1)  = 0xAA
    Ver/Length (2)  = bits 15:10 version (0), bits 9:0 total frame length
    CmdType    (1)  = bits 4:0 response type, bit 5 frame type (0=cmd, 1=resp)
    ENC        (1)  = encryption (0 = none)
    RES        (3)  = reserved
    SEQ        (2)  = sequence number
    CRC-16     (2)  = over SOF..SEQ
    DATA       (n)  = CmdSet(1) + CmdID(1) + payload(n-2)
    CRC-32     (4)  = over SOF..DATA
"""

from __future__ import annotations

import struct
from dataclasses import dataclass

from car_media_manager.cameras.dji_ble.crc import crc16
from car_media_manager.cameras.dji_ble.crc import crc32

SOF = 0xAA
VERSION = 0
HEADER_BEFORE_CRC16_SIZE = 10
CRC16_SIZE = 2
CRC32_SIZE = 4

# Frame type flag in CmdType byte
FRAME_TYPE_CMD = 0x00
FRAME_TYPE_RESP = 0x20

# Response type values (low 5 bits of CmdType)
RESP_NONE = 0
RESP_OPTIONAL = 1
RESP_REQUIRED = 2

# CmdSet/CmdID pairs
CMDSET_GENERAL = 0x00
CMDSET_CAMERA = 0x1D

CMDID_VERSION_QUERY = 0x00
CMDID_CONNECTION_REQUEST = 0x19
CMDID_KEY_REPORT = 0x11

CMDID_CAMERA_STATUS_PUSH = 0x02
CMDID_RECORD_CONTROL = 0x03
CMDID_MODE_SWITCH = 0x04

# Device IDs
DEVICE_ID_OSMO_360 = 0xFF66

# Connection request verify modes
VERIFY_MODE_NO_CHECK = 0      # Camera uses saved pairing history
VERIFY_MODE_REQUIRE = 1       # Camera shows confirmation popup
VERIFY_MODE_RESULT = 2        # Contains verification result

# Record control values
RECORD_CTRL_START = 0
RECORD_CTRL_STOP = 1


@dataclass(frozen=True, slots=True)
class ParsedFrame:
    seq: int
    cmd_set: int
    cmd_id: int
    is_response: bool
    payload: bytes


def encode_frame(
    *,
    seq: int,
    cmd_set: int,
    cmd_id: int,
    payload: bytes,
    response_type: int = RESP_OPTIONAL,
    is_response: bool = False,
) -> bytes:
    data_segment = bytes([cmd_set, cmd_id]) + payload
    total_length = HEADER_BEFORE_CRC16_SIZE + CRC16_SIZE + len(data_segment) + CRC32_SIZE

    ver_length = ((VERSION & 0x3F) << 10) | (total_length & 0x3FF)
    cmd_type = (response_type & 0x1F)
    if is_response:
        cmd_type |= FRAME_TYPE_RESP

    header = struct.pack(
        "<BHBBBBBH",
        SOF,
        ver_length,
        cmd_type,
        0,        # ENC: no encryption
        0, 0, 0,  # RES: 3 reserved bytes
        seq,
    )

    assert len(header) == HEADER_BEFORE_CRC16_SIZE
    crc16_value = crc16(header)
    frame_without_crc32 = header + struct.pack("<H", crc16_value) + data_segment
    crc32_value = crc32(frame_without_crc32)
    return frame_without_crc32 + struct.pack("<I", crc32_value)


def parse_frame(frame: bytes) -> ParsedFrame:
    if len(frame) < HEADER_BEFORE_CRC16_SIZE + CRC16_SIZE + 2 + CRC32_SIZE:
        raise ValueError(f"Frame too short: {len(frame)} bytes")

    if frame[0] != SOF:
        raise ValueError(f"Invalid SOF: 0x{frame[0]:02x}")

    ver_length = struct.unpack("<H", frame[1:3])[0]
    total_length = ver_length & 0x3FF
    if len(frame) != total_length:
        raise ValueError(
            f"Frame length mismatch: got {len(frame)}, expected {total_length}",
        )

    cmd_type = frame[3]
    is_response = bool(cmd_type & FRAME_TYPE_RESP)

    seq = struct.unpack("<H", frame[8:10])[0]

    header = frame[:HEADER_BEFORE_CRC16_SIZE]
    expected_crc16 = crc16(header)
    got_crc16 = struct.unpack("<H", frame[10:12])[0]
    if expected_crc16 != got_crc16:
        raise ValueError(
            f"CRC-16 mismatch: got 0x{got_crc16:04x}, expected 0x{expected_crc16:04x}",
        )

    data_start = HEADER_BEFORE_CRC16_SIZE + CRC16_SIZE
    data_end = len(frame) - CRC32_SIZE
    data_segment = frame[data_start:data_end]

    expected_crc32 = crc32(frame[:data_end])
    got_crc32 = struct.unpack("<I", frame[data_end:])[0]
    if expected_crc32 != got_crc32:
        raise ValueError(
            f"CRC-32 mismatch: got 0x{got_crc32:08x}, expected 0x{expected_crc32:08x}",
        )

    if len(data_segment) < 2:
        raise ValueError("Data segment too short")
    cmd_set = data_segment[0]
    cmd_id = data_segment[1]
    payload = bytes(data_segment[2:])

    return ParsedFrame(
        seq=seq,
        cmd_set=cmd_set,
        cmd_id=cmd_id,
        is_response=is_response,
        payload=payload,
    )


def connection_request_payload(
    *,
    device_id: int,
    mac_bytes: bytes,
    verify_mode: int,
    verify_data: int,
    fw_version: int = 0,
    conidx: int = 0,
) -> bytes:
    if len(mac_bytes) > 16:
        raise ValueError("mac_bytes must be at most 16 bytes")
    mac_padded = mac_bytes + bytes(16 - len(mac_bytes))
    return struct.pack(
        "<IB16sIBBHBBBB",
        device_id,
        len(mac_bytes),
        mac_padded,
        fw_version,
        conidx,
        verify_mode,
        verify_data,
        0, 0, 0, 0,  # reserved[4]
    )


def connection_response_payload(
    *,
    device_id: int,
    ret_code: int,
    slot_number: int = 0,
) -> bytes:
    return struct.pack(
        "<IBI",
        device_id,
        ret_code,
        slot_number,
    )


def record_control_payload(*, device_id: int, record_ctrl: int) -> bytes:
    return struct.pack(
        "<IBBBBB",
        device_id,
        record_ctrl,
        0, 0, 0, 0,  # reserved[4]
    )
