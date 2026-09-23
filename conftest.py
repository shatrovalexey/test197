import os
import sys

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(__file__))

import main


@pytest.fixture()
def client(tmp_path, monkeypatch):
    """Test client with a temporary SQLite database and reset runtime state."""
    db_path = tmp_path / "test_quotes.db"
    monkeypatch.setattr(main, "DB_PATH", str(db_path))
    monkeypatch.setattr(main, "catalog_source_url", None)
    monkeypatch.setattr(main, "served_local", 0)
    monkeypatch.setattr(main, "served_from_catalog", 0)
    monkeypatch.setattr(main, "catalog_reads", 0)
    monkeypatch.setattr(main, "evictions", 0)

    with TestClient(main.app) as test_client:
        yield test_client