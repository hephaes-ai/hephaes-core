from __future__ import annotations

from pathlib import Path

from ..models import OutputConfig, ParquetOutputConfig
from ..parquet import WideParquetWriter
from .base import BaseDatasetWriter, EpisodeContext, RecordBatch


class ParquetDatasetWriter(BaseDatasetWriter):
    format_name = "parquet"

    def __init__(
        self,
        *,
        output_dir: str | Path,
        context: EpisodeContext,
        config: ParquetOutputConfig,
    ) -> None:
        self._writer = WideParquetWriter(
            output_dir=output_dir,
            episode_id=context.episode_id,
            field_names=context.field_names,
            compression=config.compression,
        )
        self.path = self._writer.path

    def write_batch(self, batch: RecordBatch) -> None:
        self._writer.write_table(
            timestamps=batch.timestamps,
            field_data=batch.field_data,
        )

    def close(self) -> None:
        self._writer.close()


def create_parquet_writer(
    *,
    output_dir: Path,
    context: EpisodeContext,
    config: OutputConfig,
) -> ParquetDatasetWriter:
    if not isinstance(config, ParquetOutputConfig):
        raise TypeError("Parquet writer requires a ParquetOutputConfig")

    return ParquetDatasetWriter(
        output_dir=output_dir,
        context=context,
        config=config,
    )

