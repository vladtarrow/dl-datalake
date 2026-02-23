

def test_audit_orphan_files(temp_datalake):
    """
    Use Case 9.2: Audit and Reconciliation
    Verify detection of orphan files (files on disk not in manifest).
    """
    data_root = temp_datalake["data_root"]

    # 1. Create a legitimate file
    legit_file = data_root / "legit.parquet"
    legit_file.touch()

    # 2. Add to manifest
    temp_datalake["manifest"].add_entry(
        exchange="TEST", market="SPOT", symbol="LEGIT", path=str(legit_file), type="raw",
    )

    # 3. Create an orphan file
    orphan_file = data_root / "orphan.parquet"
    orphan_file.touch()

    # 4. Run Audit
    # If there is no built-in audit function, we perform a manual check similar to what a script would do.
    # START AUDIT LOGIC
    db_paths = {e.path for e in temp_datalake["manifest"].list_entries()}
    disk_paths = {str(p) for p in data_root.rglob("*.parquet")}

    orphans = disk_paths - db_paths
    missing = db_paths - disk_paths
    # END AUDIT LOGIC

    assert str(orphan_file) in orphans
    assert len(missing) == 0


def test_audit_broken_links(temp_datalake):
    """
    Use Case 9.2: Broken Links
    Verify detection of manifest entries pointing to non-existent files.
    """
    # 1. Add entry for non-existent file
    fake_path = str(temp_datalake["data_root"] / "ghost.parquet")
    temp_datalake["manifest"].add_entry(
        exchange="TEST", market="SPOT", symbol="GHOST", path=fake_path, type="raw",
    )

    # 2. Run Audit
    db_paths = {e.path for e in temp_datalake["manifest"].list_entries()}
    disk_paths = {str(p) for p in temp_datalake["data_root"].rglob("*.parquet")}

    missing = db_paths - disk_paths

    assert fake_path in missing
