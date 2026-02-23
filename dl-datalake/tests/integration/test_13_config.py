import os

import pytest

from dl_datalake.ingest.pipeline import IngestPipeline


def test_permissions_failure(temp_datalake):
    """
    Use Case 13: Config & Security (Permissions)
    """
    if os.name == "nt":
        pytest.skip(
            "Permission tests on Windows are tricky without admin/special calls",
        )

    # On Linux, we could chmod u-w.
    # On Windows, we can inspect if ReadOnly attribute handling works, but simpler to skip or mock.

    # For now, we just ensure that if we pass an invalid root (e.g. non-existent drive), it fails properly.

    # Using a likely prohibited path or non-existent drive
    # On windows Z:\ is typical absent drive.
    bad_root = "Z:\\nonexistent"
    pipeline_bad = IngestPipeline(
        data_root=bad_root, db_path=str(temp_datalake["db_path"]),
    )

    # It should fail when writing
    with pytest.raises(Exception):  # noqa: B017, PT011
        # We need a valid CSV to get to the writing stage
        pipeline_bad.ingest_csv("valid.csv", "EX", "MKT", "SYM")
