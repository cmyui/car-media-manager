"""BLE client for DJI Osmo cameras using the DJI R SDK protocol."""

from __future__ import annotations

import asyncio
import logging
import random
import uuid
from dataclasses import dataclass

from bleak import BleakClient
from bleak import BleakScanner
from bleak.backends.device import BLEDevice
from bleak.backends.scanner import AdvertisementData

from car_media_manager.cameras.dji_ble import protocol

log = logging.getLogger(__name__)

SERVICE_UUID = "0000fff0-0000-1000-8000-00805f9b34fb"
NOTIFY_CHAR_UUID = "0000fff4-0000-1000-8000-00805f9b34fb"
WRITE_CHAR_UUID = "0000fff5-0000-1000-8000-00805f9b34fb"

DJI_MFG_DATA_PREFIX_BYTES = (0xAA, 0x08)
DJI_MFG_DATA_IDENT_BYTE = 0xFA

SCAN_TIMEOUT = 15.0
RESPONSE_TIMEOUT = 5.0
CONNECT_TIMEOUT = 10.0


class DJIProtocolError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class DiscoveredCamera:
    address: str
    name: str


@dataclass(frozen=True, slots=True)
class PairingInfo:
    address: str
    slot_number: int
    device_id: int


def _is_dji_camera(ad: AdvertisementData) -> bool:
    for mfg_id, mfg_data in ad.manufacturer_data.items():
        combined = mfg_id.to_bytes(2, "little") + mfg_data
        if len(combined) >= 5 and (
            combined[0] == DJI_MFG_DATA_PREFIX_BYTES[0]
            and combined[1] == DJI_MFG_DATA_PREFIX_BYTES[1]
            and combined[4] == DJI_MFG_DATA_IDENT_BYTE
        ):
            return True
    return False


async def scan_for_cameras(timeout: float = SCAN_TIMEOUT) -> list[DiscoveredCamera]:
    found: dict[str, DiscoveredCamera] = {}

    def detection_callback(device: BLEDevice, ad: AdvertisementData) -> None:
        if not _is_dji_camera(ad):
            return
        if device.address in found:
            return
        found[device.address] = DiscoveredCamera(
            address=device.address,
            name=device.name or "DJI Camera",
        )

    scanner = BleakScanner(detection_callback=detection_callback)
    await scanner.start()
    try:
        await asyncio.sleep(timeout)
    finally:
        await scanner.stop()
    return list(found.values())


def _local_mac_bytes() -> bytes:
    node = uuid.getnode()
    return node.to_bytes(6, "big")


class DJIBLESession:
    """Manages one BLE connection + the protocol state machine over it."""

    def __init__(self, address: str) -> None:
        self._address = address
        self._client = BleakClient(address, timeout=CONNECT_TIMEOUT)
        self._seq = random.randint(0, 0xFFFF)
        self._pending: dict[int, asyncio.Future[protocol.ParsedFrame]] = {}
        self._incoming_buffer = bytearray()
        self._camera_cmd_frames: asyncio.Queue[protocol.ParsedFrame] = asyncio.Queue()

    async def __aenter__(self) -> "DJIBLESession":
        await self._client.connect()
        await self._client.start_notify(NOTIFY_CHAR_UUID, self._on_notify)
        return self

    async def __aexit__(self, *args: object) -> None:
        try:
            await self._client.stop_notify(NOTIFY_CHAR_UUID)
        except Exception:
            pass
        await self._client.disconnect()

    def _next_seq(self) -> int:
        self._seq = (self._seq + 1) & 0xFFFF
        return self._seq

    def _on_notify(self, _sender: object, data: bytearray) -> None:
        self._incoming_buffer.extend(data)
        while len(self._incoming_buffer) >= 3:
            if self._incoming_buffer[0] != protocol.SOF:
                self._incoming_buffer.pop(0)
                continue
            ver_length = int.from_bytes(self._incoming_buffer[1:3], "little")
            total_length = ver_length & 0x3FF
            if total_length == 0 or total_length > 1024:
                self._incoming_buffer.pop(0)
                continue
            if len(self._incoming_buffer) < total_length:
                return
            frame_bytes = bytes(self._incoming_buffer[:total_length])
            del self._incoming_buffer[:total_length]
            try:
                frame = protocol.parse_frame(frame_bytes)
            except ValueError:
                log.exception("Failed to parse incoming frame")
                continue
            self._route_frame(frame)

    def _route_frame(self, frame: protocol.ParsedFrame) -> None:
        if frame.is_response:
            future = self._pending.pop(frame.seq, None)
            if future is not None and not future.done():
                future.set_result(frame)
            else:
                log.debug("Response for unknown seq %d", frame.seq)
        else:
            self._camera_cmd_frames.put_nowait(frame)

    async def _write(self, frame: bytes) -> None:
        await self._client.write_gatt_char(WRITE_CHAR_UUID, frame, response=False)

    async def _send_command(
        self,
        *,
        cmd_set: int,
        cmd_id: int,
        payload: bytes,
        response_required: bool = True,
        timeout: float = RESPONSE_TIMEOUT,
    ) -> protocol.ParsedFrame | None:
        seq = self._next_seq()
        response_type = (
            protocol.RESP_REQUIRED if response_required else protocol.RESP_NONE
        )
        frame = protocol.encode_frame(
            seq=seq,
            cmd_set=cmd_set,
            cmd_id=cmd_id,
            payload=payload,
            response_type=response_type,
            is_response=False,
        )

        if not response_required:
            await self._write(frame)
            return None

        future: asyncio.Future[protocol.ParsedFrame] = asyncio.Future()
        self._pending[seq] = future
        try:
            await self._write(frame)
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            self._pending.pop(seq, None)

    async def _send_response(
        self,
        *,
        seq: int,
        cmd_set: int,
        cmd_id: int,
        payload: bytes,
    ) -> None:
        frame = protocol.encode_frame(
            seq=seq,
            cmd_set=cmd_set,
            cmd_id=cmd_id,
            payload=payload,
            response_type=protocol.RESP_NONE,
            is_response=True,
        )
        await self._write(frame)

    async def pair(
        self,
        *,
        verify_data: int | None = None,
        wait_for_user: bool = True,
    ) -> PairingInfo:
        """First-time pairing — camera will show a confirmation popup.

        The user must tap 'accept' on the camera screen within ~30 seconds.
        """
        return await self._handshake(
            verify_mode=protocol.VERIFY_MODE_REQUIRE,
            verify_data=verify_data if verify_data is not None else random.randint(0, 0xFFFF),
            wait_timeout=30.0 if wait_for_user else RESPONSE_TIMEOUT,
        )

    async def reconnect(self) -> PairingInfo:
        """Reconnect to a previously-paired camera — no popup shown."""
        return await self._handshake(
            verify_mode=protocol.VERIFY_MODE_NO_CHECK,
            verify_data=random.randint(0, 0xFFFF),
            wait_timeout=RESPONSE_TIMEOUT,
        )

    async def _handshake(
        self,
        *,
        verify_mode: int,
        verify_data: int,
        wait_timeout: float,
    ) -> PairingInfo:
        # Step 1: remote → camera connection request
        payload = protocol.connection_request_payload(
            device_id=0,  # sender's ID; we're just a controller
            mac_bytes=_local_mac_bytes(),
            verify_mode=verify_mode,
            verify_data=verify_data,
        )
        response = await self._send_command(
            cmd_set=protocol.CMDSET_GENERAL,
            cmd_id=protocol.CMDID_CONNECTION_REQUEST,
            payload=payload,
            response_required=True,
            timeout=wait_timeout,
        )
        if response is None or len(response.payload) < 5:
            raise DJIProtocolError("Invalid response to connection request")

        # Step 3: camera → remote connection request (we must respond)
        camera_frame = await asyncio.wait_for(
            self._camera_cmd_frames.get(),
            timeout=wait_timeout,
        )
        if camera_frame.cmd_set != protocol.CMDSET_GENERAL or camera_frame.cmd_id != protocol.CMDID_CONNECTION_REQUEST:
            raise DJIProtocolError(
                f"Unexpected camera frame: cmdset=0x{camera_frame.cmd_set:02x} "
                f"cmdid=0x{camera_frame.cmd_id:02x}",
            )
        if len(camera_frame.payload) < 29:
            raise DJIProtocolError("Camera connection frame too short")
        camera_device_id = int.from_bytes(camera_frame.payload[0:4], "little")
        camera_verify_data = int.from_bytes(camera_frame.payload[27:29], "little")

        if camera_verify_data != 0:
            raise DJIProtocolError("Camera rejected pairing")

        # Step 4: remote → camera connection response
        slot_number = 0  # single camera, no index
        await self._send_response(
            seq=camera_frame.seq,
            cmd_set=protocol.CMDSET_GENERAL,
            cmd_id=protocol.CMDID_CONNECTION_REQUEST,
            payload=protocol.connection_response_payload(
                device_id=camera_device_id,
                ret_code=0,
                slot_number=slot_number,
            ),
        )

        return PairingInfo(
            address=self._address,
            slot_number=slot_number,
            device_id=camera_device_id,
        )

    async def start_recording(self, *, device_id: int = protocol.DEVICE_ID_OSMO_360) -> None:
        await self._record_control(device_id=device_id, record_ctrl=protocol.RECORD_CTRL_START)

    async def stop_recording(self, *, device_id: int = protocol.DEVICE_ID_OSMO_360) -> None:
        await self._record_control(device_id=device_id, record_ctrl=protocol.RECORD_CTRL_STOP)

    async def _record_control(self, *, device_id: int, record_ctrl: int) -> None:
        payload = protocol.record_control_payload(
            device_id=device_id,
            record_ctrl=record_ctrl,
        )
        response = await self._send_command(
            cmd_set=protocol.CMDSET_CAMERA,
            cmd_id=protocol.CMDID_RECORD_CONTROL,
            payload=payload,
            response_required=True,
        )
        if response is None or len(response.payload) < 1:
            raise DJIProtocolError("No response to record control")
        ret_code = response.payload[0]
        if ret_code != 0:
            raise DJIProtocolError(f"Record control failed: ret_code=0x{ret_code:02x}")
