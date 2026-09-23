import json
import sqlite3

from fastapi.testclient import TestClient

import main


def _db(client: TestClient) -> sqlite3.Connection:
    return sqlite3.connect(main.DB_PATH)


def _put_local(client: TestClient, quote_id: str = "q-0000001", author: str = "Марк Аврелий", text: str = "Текст"):
    return client.put(
        f"/catalog/{quote_id}",
        json={"author": author, "text": text},
    )


# --- GET /health ---


def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "healthy"}


# --- POST /catalog/source ---


def test_catalog_source_saves_url(client):
    resp = client.post("/catalog/source", json={"url": "http://catalog.internal:9000"})
    assert resp.status_code == 200
    assert resp.json() == {"source": "http://catalog.internal:9000"}
    assert main.catalog_source_url == "http://catalog.internal:9000"


def test_catalog_source_invalid_json(client):
    resp = client.post("/catalog/source", content=b"{invalid", headers={"Content-Type": "application/json"})
    assert resp.status_code == 400


# --- PUT /catalog/{quote_id} ---


def test_put_adds_local_quote(client):
    resp = _put_local(client)
    assert resp.status_code == 200
    assert resp.json() == {"id": "q-0000001", "author": "Марк Аврелий", "text": "Текст"}

    with _db(client) as conn:
        row = conn.execute("SELECT id, author, text, source, published FROM quotes WHERE id = ?", ("q-0000001",)).fetchone()
    assert row == ("q-0000001", "Марк Аврелий", "Текст", "LOCAL", 1)


def test_put_replaces_existing_quote(client):
    _put_local(client, quote_id="q-1", author="Автор", text="Старый текст")
    resp = _put_local(client, quote_id="q-1", author="Новый автор", text="Новый текст")
    assert resp.status_code == 200
    assert resp.json()["author"] == "Новый автор"

    with _db(client) as conn:
        row = conn.execute("SELECT author, text, source FROM quotes WHERE id = ?", ("q-1",)).fetchone()
    assert row == ("Новый автор", "Новый текст", "LOCAL")


def test_put_validation_bounds(client):
    resp = client.put("/catalog/q-1", json={"author": "", "text": "ok"})
    assert resp.status_code == 422

    resp = client.put("/catalog/q-1", json={"author": "a" * 201, "text": "ok"})
    assert resp.status_code == 422

    resp = client.put("/catalog/q-1", json={"author": "ok", "text": ""})
    assert resp.status_code == 422

    resp = client.put("/catalog/q-1", json={"author": "ok", "text": "x" * 16385})
    assert resp.status_code == 422


def test_put_invalid_json(client):
    resp = client.put("/catalog/q-1", content=b"{bad", headers={"Content-Type": "application/json"})
    assert resp.status_code == 400


# --- DELETE /catalog/{quote_id} ---


def test_delete_unpublishes_quote(client):
    _put_local(client)
    resp = client.delete("/catalog/q-0000001")
    assert resp.status_code == 200
    assert resp.json() == {"deleted": True}

    with _db(client) as conn:
        published = conn.execute("SELECT published FROM quotes WHERE id = ?", ("q-0000001",)).fetchone()[0]
    assert published == 0


def test_delete_missing_quote_404(client):
    resp = client.delete("/catalog/q-missing")
    assert resp.status_code == 404
    assert resp.json() == {"detail": "not found"}


# --- POST /import ---


def _import_body(*records, first='{"quotes": [', last="]}") -> bytes:
    lines = [first]
    for i, rec in enumerate(records):
        suffix = "," if i < len(records) - 1 else ""
        lines.append(json.dumps(rec, ensure_ascii=False) + suffix)
    lines.append(last)
    return ("\n".join(lines)).encode("utf-8")


def test_import_streaming_into_db(client):
    body = _import_body(
        {"id": "q-0000001", "author": "Марк Аврелий", "text": "Т1", "tags": ["стоицизм"], "year": 170, "lang": "ru"},
        {"id": "q-0000002", "author": "Автор2", "text": "Т2", "tags": [], "year": 0, "lang": "ru"},
    )
    resp = client.post("/import", content=body)
    assert resp.status_code == 200
    assert resp.json() == {"imported": 2, "dropped": 0}

    with _db(client) as conn:
        rows = conn.execute("SELECT id, author, text, tags, year, lang, source, published FROM quotes ORDER BY id").fetchall()
    assert rows == [
        ("q-0000001", "Марк Аврелий", "Т1", '["стоицизм"]', 170, "ru", "CATALOG", 1),
        ("q-0000002", "Автор2", "Т2", "[]", 0, "ru", "CATALOG", 1),
    ]


def test_import_replaces_and_drops(client):
    _put_local(client, quote_id="q-local", author="Локальный", text="Локальный текст")
    # Existing CATALOG quote that will disappear from the snapshot.
    body = _import_body({"id": "q-cat1", "author": "А", "text": "Т", "tags": [], "year": 0, "lang": "ru"})
    client.post("/import", content=body)

    body = _import_body(
        {"id": "q-cat1", "author": "Б", "text": "Т2", "tags": [], "year": 0, "lang": "ru"},
        {"id": "q-cat2", "author": "В", "text": "Т3", "tags": [], "year": 0, "lang": "ru"},
        {"id": "q-local", "author": "Каталог", "text": "Затёр локальную", "tags": [], "year": 0, "lang": "ru"},
    )
    resp = client.post("/import", content=body)
    assert resp.status_code == 200
    assert resp.json() == {"imported": 3, "dropped": 1}
    assert main.evictions == 1  # q-local was evicted (replaced by the snapshot)

    with _db(client) as conn:
        ids = {r[0] for r in conn.execute("SELECT id FROM quotes")}
        row = conn.execute("SELECT author, source FROM quotes WHERE id = ?", ("q-local",)).fetchone()
    assert ids == {"q-cat1", "q-cat2", "q-local"}
    assert row == ("Каталог", "CATALOG")


def test_import_invalid_structure(client):
    resp = client.post("/import", content=b'{"wrong": true}')
    assert resp.status_code == 400


def test_import_invalid_line(client):
    body = b'{"quotes": [\n{not json}\n]}'
    resp = client.post("/import", content=body)
    assert resp.status_code == 400


# --- GET /quotes/{quote_id} ---


def test_get_quote_local(client):
    _put_local(client, quote_id="q-1", author="Автор", text="Текст")
    resp = client.get("/quotes/q-1")
    assert resp.status_code == 200
    assert resp.json() == {"id": "q-1", "author": "Автор", "text": "Текст", "tags": None, "year": None, "lang": None}
    assert resp.headers["x-source"] == "LOCAL"
    assert main.served_local == 1


def test_get_quote_unpublished_is_not_served_locally(client):
    _put_local(client, quote_id="q-1")
    client.delete("/catalog/q-1")
    # No catalog configured -> not found.
    resp = client.get("/quotes/q-1")
    assert resp.status_code == 404


def test_get_quote_from_catalog(client, monkeypatch):
    captured = {}

    class FakeResponse:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return json.dumps(
                {"id": "q-cat", "author": "Каталог", "text": "Т", "tags": ["a"], "year": 1, "lang": "ru"}
            ).encode("utf-8")

    def fake_urlopen(url, timeout=5):
        captured["url"] = url
        return FakeResponse()

    monkeypatch.setattr(main.urllib.request, "urlopen", fake_urlopen)
    client.post("/catalog/source", json={"url": "http://catalog.internal:9000"})

    resp = client.get("/quotes/q-cat")
    assert resp.status_code == 200
    assert resp.json() == {"id": "q-cat", "author": "Каталог", "text": "Т", "tags": ["a"], "year": 1, "lang": "ru"}
    assert resp.headers["x-source"] == "CATALOG"
    assert captured["url"] == "http://catalog.internal:9000/quote/q-cat"
    assert main.catalog_reads == 1
    assert main.served_from_catalog == 1


def test_get_quote_catalog_404(client, monkeypatch):
    class FakeHTTPError(Exception):
        code = 404

    def fake_urlopen(url, timeout=5):
        raise FakeHTTPError()

    monkeypatch.setattr(main.urllib.request, "urlopen", fake_urlopen)
    client.post("/catalog/source", json={"url": "http://catalog.internal:9000"})

    resp = client.get("/quotes/q-missing")
    assert resp.status_code == 404
    assert resp.json() == {"detail": "not found"}
    assert main.catalog_reads == 1
    assert main.served_from_catalog == 0


def test_get_quote_catalog_unavailable(client, monkeypatch):
    def fake_urlopen(url, timeout=5):
        raise OSError("connection refused")

    monkeypatch.setattr(main.urllib.request, "urlopen", fake_urlopen)
    client.post("/catalog/source", json={"url": "http://catalog.internal:9000"})

    resp = client.get("/quotes/q-any")
    assert resp.status_code == 502


# --- GET /stats ---


def test_stats_empty(client):
    resp = client.get("/stats")
    assert resp.status_code == 200
    assert resp.json() == {
        "served_local": 0,
        "served_from_catalog": 0,
        "catalog_reads": 0,
        "evictions": 0,
        "local": 0,
        "bytes": 0,
        "catalog": 0,
    }


def test_stats_counters_and_metrics(client, monkeypatch):
    _put_local(client, quote_id="q-1", author="А", text="Текст")
    _put_local(client, quote_id="q-2", author="Б", text="Текст")
    client.delete("/catalog/q-2")  # still LOCAL but unpublished

    resp = client.get("/stats")
    data = resp.json()
    assert data["served_local"] == 0
    assert data["local"] == 2
    assert data["bytes"] > 0
    assert data["catalog"] == 0