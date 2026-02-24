from __future__ import annotations

import math
import threading
import time
from typing import Iterable

from sqlalchemy import text
from sqlalchemy.orm import Session

_STATUS_LOCK = threading.Lock()
_STATUS_CACHE: dict[str, tuple[float, bool]] = {}
_STATUS_TTL_SECONDS = 60.0
_ALLOWED_TABLES = {"game_embeddings", "query_embedding_cache"}


def _resolve_bind(db_or_connection):
    if hasattr(db_or_connection, "get_bind"):
        return db_or_connection.get_bind()
    if hasattr(db_or_connection, "engine"):
        return db_or_connection.engine
    return db_or_connection


def _cache_key(db_or_connection) -> str:
    bind = _resolve_bind(db_or_connection)
    url = str(getattr(bind, "url", "unknown"))
    dialect = str(getattr(getattr(bind, "dialect", None), "name", "unknown"))
    return f"{dialect}:{url}"


def _to_float_vector(values: Iterable[float], dimension: int) -> list[float]:
    normalized: list[float] = []
    for value in values:
        try:
            normalized.append(float(value or 0.0))
        except Exception:
            normalized.append(0.0)
        if len(normalized) >= dimension:
            break
    if len(normalized) < dimension:
        normalized.extend([0.0] * (dimension - len(normalized)))
    return normalized


def vector_to_literal(values: Iterable[float], dimension: int) -> str:
    safe_values = _to_float_vector(values, max(1, int(dimension or 1)))
    rendered = ",".join(f"{value:.8f}" if math.isfinite(value) else "0.00000000" for value in safe_values)
    return f"[{rendered}]"


def is_pgvector_ready(db: Session) -> bool:
    bind = db.get_bind()
    dialect = str(getattr(getattr(bind, "dialect", None), "name", "")).lower()
    if dialect != "postgresql":
        return False

    key = _cache_key(db)
    now = time.time()
    with _STATUS_LOCK:
        cached = _STATUS_CACHE.get(key)
        if cached and (now - cached[0]) <= _STATUS_TTL_SECONDS:
            return bool(cached[1])

    ready = False
    try:
        ext_exists = bool(
            db.execute(
                text("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'vector')")
            ).scalar()
        )
        if ext_exists:
            rows = db.execute(
                text(
                    """
                    SELECT table_name
                    FROM information_schema.columns
                    WHERE table_schema = current_schema()
                      AND column_name = 'vector_v'
                      AND table_name IN ('game_embeddings', 'query_embedding_cache')
                    GROUP BY table_name
                    """
                )
            ).all()
            ready = len(rows) >= 2
    except Exception:
        ready = False

    with _STATUS_LOCK:
        _STATUS_CACHE[key] = (now, ready)
    return ready


def mark_pgvector_schema_changed(db_or_connection) -> None:
    key = _cache_key(db_or_connection)
    with _STATUS_LOCK:
        _STATUS_CACHE.pop(key, None)


def sync_vector_column(
    db: Session,
    *,
    table_name: str,
    row_id: str | None,
    vector: Iterable[float],
    dimension: int,
) -> None:
    if not row_id:
        return
    table = str(table_name or "").strip().lower()
    if table not in _ALLOWED_TABLES:
        return
    if not is_pgvector_ready(db):
        return
    literal = vector_to_literal(vector, dimension=max(1, int(dimension or 1)))
    try:
        db.execute(
            text(f"UPDATE {table} SET vector_v = CAST(:vector_value AS vector) WHERE id = :row_id"),
            {"vector_value": literal, "row_id": row_id},
        )
    except Exception:
        # Keep search/reco flows running even if vector column update fails.
        return


def semantic_search_game_embeddings(
    db: Session,
    *,
    query_vector: Iterable[float],
    model: str,
    source: str = "steam",
    limit: int = 200,
    dimension: int = 128,
    allowed_appids: set[str] | None = None,
) -> list[tuple[str, float]]:
    if not is_pgvector_ready(db):
        return []

    query_literal = vector_to_literal(query_vector, max(1, int(dimension or 1)))
    safe_limit = max(1, min(2000, int(limit or 200)))
    try:
        rows = db.execute(
            text(
                """
                SELECT app_id, (1 - (vector_v <=> CAST(:query_vector AS vector))) AS similarity
                FROM game_embeddings
                WHERE model = :model
                  AND source = :source
                  AND vector_v IS NOT NULL
                ORDER BY vector_v <=> CAST(:query_vector AS vector)
                LIMIT :limit
                """
            ),
            {
                "query_vector": query_literal,
                "model": model,
                "source": source,
                "limit": safe_limit,
            },
        ).all()
    except Exception:
        return []

    hits: list[tuple[str, float]] = []
    for row in rows:
        app_id = str(getattr(row, "app_id", "") or "").strip()
        if not app_id:
            continue
        if allowed_appids is not None and app_id not in allowed_appids:
            continue
        similarity_raw = getattr(row, "similarity", 0.0)
        try:
            similarity = float(similarity_raw or 0.0)
        except Exception:
            similarity = 0.0
        score = max(0.0, min(100.0, (similarity + 1.0) * 50.0))
        hits.append((app_id, score))
    return hits
