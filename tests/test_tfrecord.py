"""Tests for TFRecord helpers and writers."""
from __future__ import annotations

from hephaes.models import TFRecordOutputConfig
from hephaes.outputs import EpisodeContext, RecordBatch
from hephaes.outputs.tfrecord import TFRecordDatasetWriter
from hephaes.tfrecord import stream_tfrecord_rows


class TestTFRecordDatasetWriter:
    def test_writes_rows_and_preserves_nulls(self, tmp_path):
        context = EpisodeContext(
            episode_id="ep001",
            source_path=tmp_path / "source.mcap",
            ros_version="ROS2",
            field_names=["cmd_vel", "odom"],
            resample=None,
            output=TFRecordOutputConfig(),
        )

        with TFRecordDatasetWriter(
            output_dir=tmp_path,
            context=context,
            config=TFRecordOutputConfig(),
        ) as writer:
            writer.write_batch(
                RecordBatch(
                    timestamps=[1, 2],
                    field_data={
                        "cmd_vel": ['{"v": 1}', None],
                        "odom": [None, '{"pose": 2}'],
                    },
                )
            )

        rows = list(stream_tfrecord_rows(tmp_path / "ep001.tfrecord"))
        assert rows == [
            {"timestamp_ns": 1, "cmd_vel": '{"v": 1}', "odom": None},
            {"timestamp_ns": 2, "cmd_vel": None, "odom": '{"pose": 2}'},
        ]

    def test_reads_gzip_compressed_tfrecord(self, tmp_path):
        config = TFRecordOutputConfig(compression="gzip")
        context = EpisodeContext(
            episode_id="ep_gzip",
            source_path=tmp_path / "source.mcap",
            ros_version="ROS2",
            field_names=["camera"],
            resample=None,
            output=config,
        )

        with TFRecordDatasetWriter(
            output_dir=tmp_path,
            context=context,
            config=config,
        ) as writer:
            writer.write_batch(
                RecordBatch(
                    timestamps=[5],
                    field_data={"camera": ['{"frame": 1}']},
                )
            )

        rows = list(stream_tfrecord_rows(tmp_path / "ep_gzip.tfrecord"))
        assert rows == [{"timestamp_ns": 5, "camera": '{"frame": 1}'}]
