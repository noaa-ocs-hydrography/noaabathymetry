"""End-to-end tests that hit real S3. Marked @pytest.mark.network (skipped by default).

Run with: pytest -m network
"""

import os

import pytest

from nbs.bluetopo._internal.config import get_config
from nbs.bluetopo._internal.download import _get_s3_client


@pytest.mark.network
class TestRealS3:
    """Smoke tests that verify connectivity and basic download from real S3."""

    def test_list_bluetopo_tile_scheme(self):
        """Verify the BlueTopo tile scheme prefix is accessible on S3."""
        client = _get_s3_client()
        paginator = client.get_paginator("list_objects_v2")
        cfg = get_config("bluetopo")
        result = paginator.paginate(
            Bucket=cfg["bucket"],
            Prefix=cfg["geom_prefix"],
        ).build_full_result()
        assert "Contents" in result
        assert len(result["Contents"]) > 0

    def test_list_bag_tile_scheme(self):
        """Verify the BAG tile scheme prefix is accessible on S3."""
        client = _get_s3_client()
        paginator = client.get_paginator("list_objects_v2")
        cfg = get_config("bag")
        result = paginator.paginate(
            Bucket=cfg["bucket"],
            Prefix=cfg["geom_prefix"],
        ).build_full_result()
        assert "Contents" in result

    def test_download_single_tile_scheme(self, tmp_path):
        """Download a tile scheme gpkg from real S3 and verify it's a valid file."""
        client = _get_s3_client()
        cfg = get_config("bluetopo")
        paginator = client.get_paginator("list_objects_v2")
        result = paginator.paginate(
            Bucket=cfg["bucket"],
            Prefix=cfg["geom_prefix"],
        ).build_full_result()
        assert "Contents" in result

        obj = result["Contents"][0]
        dest = str(tmp_path / os.path.basename(obj["Key"]))
        client.download_file(cfg["bucket"], obj["Key"], dest)
        assert os.path.isfile(dest)
        assert os.path.getsize(dest) > 0
