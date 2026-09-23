import argparse
import json
import sqlite3
import urllib.error
import urllib.request
from contextlib import asynccontextmanager
from typing import List, Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

DB_PATH = "quotes.db"
DB_TABLE = "quotes"
BATCH_SIZE = 1000

SOURCE_CATALOG = "CATALOG"
SOURCE_LOCAL = "LOCAL"

catalog_source_url: str | None = None

# Runtime counters (since process start, only grow).
served_local = 0
served_from_catalog = 0
catalog_reads = 0
evictions = 0


class CatalogSourceRequest(BaseModel):
    """Тело запроса POST /catalog/source."""

    url: str


class QuoteUpdateRequest(BaseModel):
    """Тело запроса PUT /catalog/{quote_id}."""

    author: str = Field(min_length=1, max_length=200, description="Автор цитаты, от 1 до 200 знаков")
    text: str = Field(min_length=1, max_length=16384, description="Текст цитаты, от 1 до 16384 знаков")


class Quote(BaseModel):
    """Цитата целиком."""

    id: str
    author: Optional[str] = None
    text: Optional[str] = None
    tags: Optional[List[str]] = None
    year: Optional[int] = None
    lang: Optional[str] = None


class QuoteUpdateResponse(BaseModel):
    """Ответ PUT /catalog/{quote_id}."""

    id: str
    author: str
    text: str


class ImportResponse(BaseModel):
    """Ответ POST /import."""

    imported: int
    dropped: int


class StatsResponse(BaseModel):
    """Эксплуатационная сводка GET /stats."""

    served_local: int
    served_from_catalog: int
    catalog_reads: int
    evictions: int
    local: int
    bytes: int
    catalog: int


def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {DB_TABLE} (
                id TEXT PRIMARY KEY,
                author TEXT,
                text TEXT,
                tags TEXT,
                year INTEGER,
                lang TEXT,
                published INTEGER NOT NULL DEFAULT 1,
                source TEXT NOT NULL DEFAULT {SOURCE_CATALOG!r}
            )
            """
        )
        # Add missing columns for databases created before they were introduced.
        columns = {row[1] for row in conn.execute(f"PRAGMA table_info({DB_TABLE})")}
        if "published" not in columns:
            conn.execute(f"ALTER TABLE {DB_TABLE} ADD COLUMN published INTEGER NOT NULL DEFAULT 1")
        if "source" not in columns:
            conn.execute(f"ALTER TABLE {DB_TABLE} ADD COLUMN source TEXT NOT NULL DEFAULT {SOURCE_CATALOG!r}")


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="Quote Catalog Service",
    description="Сервис каталога цитат: витрина с потоковым импортом снимков каталога "
                "в SQLite, публикацией через витрину и чтением читателями.",
    version="1.0.0",
    lifespan=lifespan,
)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    for err in exc.errors():
        if err.get("type") in ("json_invalid", "json_decode", "value_error.jsondecode"):
            return JSONResponse(status_code=400, content={"detail": "Invalid JSON"})
    return JSONResponse(status_code=422, content={"detail": exc.errors()})


@app.get(
    "/health",
    status_code=200,
    tags=["Operations"],
    summary="Проверка здоровья",
    description="Всегда возвращает HTTP 200 со статусом healthy.",
    responses={200: {"description": "Сервис здоров", "content": {"application/json": {"example": {"status": "healthy"}}}}},
)
def health():
    return {"status": "healthy"}


@app.post(
    "/catalog/source",
    status_code=200,
    tags=["Catalog"],
    summary="Установить источник каталога",
    description="Сохраняет URL источника каталога в глобальную переменную.",
    response_model=dict,
    responses={
        200: {"description": "URL сохранён", "content": {"application/json": {"example": {"source": "http://catalog.internal:9000"}}}},
        400: {"description": "Невалидный JSON"},
        422: {"description": "Ошибка валидации"},
    },
)
def add_catalog_source(request: CatalogSourceRequest):
    global catalog_source_url
    catalog_source_url = request.url
    return {"source": catalog_source_url}


@app.post(
    "/import",
    tags=["Import"],
    summary="Импорт снимка каталога",
    description="Потоковый импорт большого JSON (до 120 МБ) в SQLite. "
                "Первая строка тела — '{\"quotes\": [', последняя — ']}', "
                "каждая строка между ними — JSON-объект цитаты; записи разделены символом ','. "
                "Записи перезаписываются по id, отсутствующие во входном наборе — удаляются.",
    response_model=ImportResponse,
    responses={
        200: {"description": "Импорт выполнен", "content": {"application/json": {"example": {"imported": 340000, "dropped": 120}}}},
        400: {"description": "Невалидный JSON или некорректная структура тела"},
    },
)
async def import_quotes(request: Request):
    global evictions
    seen_ids: set[str] = set()
    imported = 0
    buffer = b""
    batch = []
    first_line = None
    last_line = None

    with sqlite3.connect(DB_PATH) as conn:
        # Local records that may be evicted by the incoming snapshot.
        local_ids = {
            row[0]
            for row in conn.execute(f"SELECT id FROM {DB_TABLE} WHERE source = {SOURCE_LOCAL!r}")
        }

        def flush_batch():
            nonlocal imported
            if not batch:
                return
            conn.executemany(
                f"INSERT OR REPLACE INTO {DB_TABLE} "
                "(id, author, text, tags, year, lang, published, source) "
                f"VALUES (?, ?, ?, ?, ?, ?, 1, {SOURCE_CATALOG!r})",
                batch,
            )
            imported += len(batch)
            batch.clear()

        def add_record(record: dict):
            global evictions
            quote_id = record["id"]
            seen_ids.add(quote_id)
            if quote_id in local_ids:
                evictions += 1
                local_ids.remove(quote_id)
            tags = record.get("tags")
            batch.append(
                (
                    quote_id,
                    record.get("author"),
                    record.get("text"),
                    json.dumps(tags, ensure_ascii=False) if tags is not None else None,
                    record.get("year"),
                    record.get("lang"),
                )
            )
            if len(batch) >= BATCH_SIZE:
                flush_batch()

        def parse_data_line(line: bytes) -> dict:
            if line.endswith(b","):
                line = line[:-1]
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                raise HTTPException(status_code=400, detail="Invalid JSON in import body")
            if not isinstance(record, dict) or "id" not in record:
                raise HTTPException(status_code=400, detail="Invalid quote record")
            return record

        async for chunk in request.stream():
            buffer += chunk
            while b"\n" in buffer:
                line, buffer = buffer.split(b"\n", 1)
                line = line.strip()
                if not line:
                    continue
                if first_line is None:
                    if line != b'{"quotes": [':
                        raise HTTPException(status_code=400, detail="Invalid import body: expected opening line")
                    first_line = line
                    continue
                last_line = line
                if line == b"]}":
                    continue
                add_record(parse_data_line(line))

        if buffer:
            line = buffer.strip()
            if line:
                if first_line is None:
                    if line != b'{"quotes": [':
                        raise HTTPException(status_code=400, detail="Invalid import body: expected opening line")
                    first_line = line
                else:
                    last_line = line
                    if line != b"]}":
                        add_record(parse_data_line(line))

        if first_line != b'{"quotes": [' or last_line != b"]}":
            raise HTTPException(status_code=400, detail="Invalid import body")

        flush_batch()

        existing_ids = {row[0] for row in conn.execute(f"SELECT id FROM {DB_TABLE}")}
        dropped_ids = existing_ids - seen_ids
        conn.executemany(
            f"DELETE FROM {DB_TABLE} WHERE id = ?",
            [(qid,) for qid in dropped_ids],
        )
        dropped = len(dropped_ids)

    return {"imported": imported, "dropped": dropped}


@app.put(
    "/catalog/{quote_id}",
    status_code=200,
    tags=["Catalog"],
    summary="Добавить или заменить цитату",
    description="Добавляет или заменяет одну цитату через витрину. "
                "quote_id — id записи в таблице БД. Запись получает источник LOCAL.",
    response_model=QuoteUpdateResponse,
    responses={
        200: {"description": "Цитата сохранена", "content": {"application/json": {"example": {"id": "q-0000001", "author": "Марк Аврелий", "text": "…"}}}},
        400: {"description": "Невалидный JSON"},
        422: {"description": "Нарушение границ полей (author: 1-200, text: 1-16384)"},
    },
)
def update_catalog_quote(quote_id: str, request: QuoteUpdateRequest):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            f"INSERT OR REPLACE INTO {DB_TABLE} "
            "(id, author, text, tags, year, lang, published, source) "
            f"VALUES (?, ?, ?, NULL, NULL, NULL, 1, {SOURCE_LOCAL!r})",
            (quote_id, request.author, request.text),
        )
    return {"id": quote_id, "author": request.author, "text": request.text}


@app.delete(
    "/catalog/{quote_id}",
    status_code=200,
    tags=["Catalog"],
    summary="Снять цитату с публикации",
    description="Снимает цитату с публикации через витрину. Запись не удаляется, "
                "а помечается как снятая с публикации.",
    responses={
        200: {"description": "Цитата снята с публикации", "content": {"application/json": {"example": {"deleted": True}}}},
        404: {"description": "Запись не найдена", "content": {"application/json": {"example": {"detail": "not found"}}}},
    },
)
def delete_catalog_quote(quote_id: str):
    with sqlite3.connect(DB_PATH) as conn:
        exists = conn.execute(
            f"SELECT 1 FROM {DB_TABLE} WHERE id = ?", (quote_id,)
        ).fetchone()
        if exists is None:
            raise HTTPException(status_code=404, detail="not found")
        conn.execute(
            f"UPDATE {DB_TABLE} SET published = 0 WHERE id = ?", (quote_id,)
        )
    return {"deleted": True}


@app.get(
    "/quotes/{quote_id}",
    status_code=200,
    tags=["Reader"],
    summary="Чтение цитаты читателем",
    description="Возвращает цитату целиком. Сначала ищет у себя (только опубликованные), "
                "при промахе обращается в каталог. Ответ 200 содержит заголовок X-Source: "
                "LOCAL (из витрины) или CATALOG (из каталога).",
    response_model=Quote,
    responses={
        200: {
            "description": "Цитата найдена",
            "content": {
                "application/json": {
                    "example": {"id": "q-0000001", "author": "Марк Аврелий", "text": "…", "tags": ["стоицизм"], "year": 170, "lang": "ru"}
                }
            },
            "headers": {"X-Source": {"description": "LOCAL или CATALOG", "schema": {"type": "string"}}},
        },
        404: {"description": "Цитата не найдена ни у витрины, ни в каталоге", "content": {"application/json": {"example": {"detail": "not found"}}}},
        502: {"description": "Каталог недоступен или вернул неожиданный ответ"},
    },
)
def get_quote(quote_id: str):
    global served_local, served_from_catalog, catalog_reads

    # 1. Try the local showcase first (published quotes only).
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            f"SELECT id, author, text, tags, year, lang "
            f"FROM {DB_TABLE} WHERE id = ? AND published = 1",
            (quote_id,),
        ).fetchone()

    if row is not None:
        quote = {
            "id": row[0],
            "author": row[1],
            "text": row[2],
            "tags": json.loads(row[3]) if row[3] is not None else None,
            "year": row[4],
            "lang": row[5],
        }
        served_local += 1
        return JSONResponse(content=quote, headers={"X-Source": SOURCE_LOCAL})

    # 2. The showcase does not remember misses: ask the remote catalog.
    if catalog_source_url is not None:
        url = f"{catalog_source_url.rstrip('/')}/quote/{quote_id}"
        try:
            catalog_reads += 1
            with urllib.request.urlopen(url, timeout=5) as resp:
                if resp.status == 200:
                    data = json.loads(resp.read().decode("utf-8"))
                    served_from_catalog += 1
                    return JSONResponse(content=data, headers={"X-Source": SOURCE_CATALOG})
                raise HTTPException(status_code=502, detail="catalog unavailable")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise HTTPException(status_code=404, detail="not found")
            raise HTTPException(status_code=502, detail="catalog unavailable")
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
            # Catalog is unreachable or returned an unexpected response.
            raise HTTPException(status_code=502, detail="catalog unavailable")

    raise HTTPException(status_code=404, detail="not found")


@app.get(
    "/stats",
    tags=["Operations"],
    summary="Эксплуатационная сводка",
    description="Сводка с момента старта процесса. Все величины только растут, "
                "кроме local, bytes и catalog (текущие значения из БД).",
    response_model=StatsResponse,
    responses={
        200: {
            "description": "Сводка",
            "content": {
                "application/json": {
                    "example": {
                        "served_local": 10423,
                        "served_from_catalog": 512,
                        "catalog_reads": 96,
                        "evictions": 148,
                        "local": 63,
                        "bytes": 516159,
                        "catalog": 340000,
                    }
                }
            },
        }
    },
)
def get_stats():
    with sqlite3.connect(DB_PATH) as conn:
        local_count = conn.execute(
            f"SELECT COUNT(*) FROM {DB_TABLE} WHERE source = {SOURCE_LOCAL!r}"
        ).fetchone()[0]
        catalog_count = conn.execute(
            f"SELECT COUNT(*) FROM {DB_TABLE} WHERE source = {SOURCE_CATALOG!r}"
        ).fetchone()[0]
        local_rows = conn.execute(
            f"SELECT author, text, tags FROM {DB_TABLE} WHERE source = {SOURCE_LOCAL!r}"
        ).fetchall()

    bytes_total = 0
    for author, text, tags in local_rows:
        bytes_total += len((author or "").encode("utf-8"))
        bytes_total += len((text or "").encode("utf-8"))
        if tags is not None:
            bytes_total += len(tags.encode("utf-8"))

    return {
        "served_local": served_local,
        "served_from_catalog": served_from_catalog,
        "catalog_reads": catalog_reads,
        "evictions": evictions,
        "local": local_count,
        "bytes": bytes_total,
        "catalog": catalog_count,
    }


def main():
    parser = argparse.ArgumentParser(description="Quote Catalog Service")
    parser.add_argument("--host", default="0.0.0.0", help="Bind address")
    parser.add_argument("--port", type=int, default=8000, help="Bind port")
    args = parser.parse_args()

    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()