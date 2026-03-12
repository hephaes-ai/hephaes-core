from __future__ import annotations

import gzip
import struct
from pathlib import Path
from typing import BinaryIO

from ..models import OutputConfig, TFRecordOutputConfig
from .base import BaseDatasetWriter, EpisodeContext, RecordBatch

_CRC32C_POLYNOMIAL = 0x82F63B78
_CRC32C_TABLE: tuple[int, ...] = ()


def _build_crc32c_table() -> tuple[int, ...]:
    table: list[int] = []
    for idx in range(256):
        crc = idx
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ _CRC32C_POLYNOMIAL
            else:
                crc >>= 1
        table.append(crc & 0xFFFFFFFF)
    return tuple(table)


def _crc32c(data: bytes) -> int:
    global _CRC32C_TABLE
    if not _CRC32C_TABLE:
        _CRC32C_TABLE = _build_crc32c_table()

    crc = 0xFFFFFFFF
    for byte in data:
        crc = _CRC32C_TABLE[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return (~crc) & 0xFFFFFFFF


def _masked_crc32c(data: bytes) -> int:
    crc = _crc32c(data)
    return (((crc >> 15) | ((crc << 17) & 0xFFFFFFFF)) + 0xA282EAD8) & 0xFFFFFFFF


def _encode_varint(value: int) -> bytes:
    if value < 0:
        value &= (1 << 64) - 1

    encoded = bytearray()
    while True:
        to_write = value & 0x7F
        value >>= 7
        if value:
            encoded.append(to_write | 0x80)
        else:
            encoded.append(to_write)
            return bytes(encoded)


def _encode_tag(field_number: int, wire_type: int) -> bytes:
    return _encode_varint((field_number << 3) | wire_type)


def _encode_length_delimited(field_number: int, payload: bytes) -> bytes:
    return _encode_tag(field_number, 2) + _encode_varint(len(payload)) + payload


def _encode_string_field(field_number: int, value: str) -> bytes:
    return _encode_length_delimited(field_number, value.encode("utf-8"))


def _encode_bytes_list(values: list[bytes]) -> bytes:
    payload = b"".join(_encode_length_delimited(1, value) for value in values)
    return _encode_length_delimited(1, payload)


def _encode_int64_list(values: list[int]) -> bytes:
    packed = b"".join(_encode_varint(value) for value in values)
    return _encode_length_delimited(3, _encode_length_delimited(1, packed))


def _encode_feature_entry(key: str, payload: bytes) -> bytes:
    entry = _encode_string_field(1, key) + _encode_length_delimited(2, payload)
    return _encode_length_delimited(1, entry)


def _encode_example(features: dict[str, tuple[str, list[bytes] | list[int]]]) -> bytes:
    entries = bytearray()
    for key, (kind, values) in features.items():
        if kind == "bytes":
            entries.extend(_encode_feature_entry(key, _encode_bytes_list(list(values))))
        elif kind == "int64":
            entries.extend(_encode_feature_entry(key, _encode_int64_list(list(values))))
        else:  # pragma: no cover - guarded by writer usage
            raise ValueError(f"Unsupported TFRecord feature kind: {kind}")

    return _encode_length_delimited(1, bytes(entries))


def _row_to_example(
    *,
    timestamp_ns: int,
    row_values: dict[str, str | None],
    field_names: list[str],
) -> bytes:
    features: dict[str, tuple[str, list[bytes] | list[int]]] = {
        "timestamp_ns": ("int64", [timestamp_ns]),
    }

    for field_name in field_names:
        value = row_values.get(field_name)
        present_key = f"{field_name}__present"
        if value is None:
            features[present_key] = ("int64", [0])
            continue

        features[present_key] = ("int64", [1])
        features[field_name] = ("bytes", [value.encode("utf-8")])

    return _encode_example(features)


class TFRecordDatasetWriter(BaseDatasetWriter):
    format_name = "tfrecord"

    def __init__(
        self,
        *,
        output_dir: str | Path,
        context: EpisodeContext,
        config: TFRecordOutputConfig,
    ) -> None:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        self.path = output_path / f"{context.episode_id}.tfrecord"
        self._field_names = list(context.field_names)
        self._handle: BinaryIO
        if config.compression == "gzip":
            self._handle = gzip.open(self.path, "wb")
        else:
            self._handle = self.path.open("wb")

    def write_batch(self, batch: RecordBatch) -> None:
        if batch.row_count == 0:
            return

        for row_index, timestamp_ns in enumerate(batch.timestamps):
            row_values = {
                field_name: values[row_index]
                for field_name, values in batch.field_data.items()
            }
            payload = _row_to_example(
                timestamp_ns=timestamp_ns,
                row_values=row_values,
                field_names=self._field_names,
            )
            self._write_record(payload)

    def _write_record(self, payload: bytes) -> None:
        length_bytes = struct.pack("<Q", len(payload))
        self._handle.write(length_bytes)
        self._handle.write(struct.pack("<I", _masked_crc32c(length_bytes)))
        self._handle.write(payload)
        self._handle.write(struct.pack("<I", _masked_crc32c(payload)))

    def close(self) -> None:
        self._handle.close()


def create_tfrecord_writer(
    *,
    output_dir: Path,
    context: EpisodeContext,
    config: OutputConfig,
) -> TFRecordDatasetWriter:
    if not isinstance(config, TFRecordOutputConfig):
        raise TypeError("TFRecord writer requires a TFRecordOutputConfig")

    return TFRecordDatasetWriter(
        output_dir=output_dir,
        context=context,
        config=config,
    )
