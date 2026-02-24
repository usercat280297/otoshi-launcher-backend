from __future__ import annotations

import hashlib
from collections import Counter
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from ..core.config import AI_SEARCH_MAX_CANDIDATES, AI_SEARCH_VECTOR_DIM
from ..models import Game, GameEmbedding, LibraryEntry, QueryEmbeddingCache, SearchInteraction
from .ai_gateway import cosine_similarity, hash_embedding
from .steam_catalog import get_catalog_page, get_hot_appids, search_store
from .steam_search import normalize_text, score_candidate, search_catalog
from .vector_store import semantic_search_game_embeddings, sync_vector_column

_SUPPORTED_MODES = {"lexical", "hybrid", "semantic"}


def _normalize_mode(mode: Optional[str]) -> str:
    normalized = str(mode or "").strip().lower()
    if normalized in _SUPPORTED_MODES:
        return normalized
    return "lexical"


def _query_hash(query: str, model: str) -> str:
    payload = f"{model}|{normalize_text(query)}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _resolve_query_embedding(db: Session, query: str, dimension: int) -> list[float]:
    model_name = f"hash-{dimension}"
    query_hash = _query_hash(query, model_name)
    row = db.query(QueryEmbeddingCache).filter(QueryEmbeddingCache.query_hash == query_hash).first()
    if row and isinstance(row.vector, list) and len(row.vector) == dimension:
        row.last_used_at = datetime.utcnow()
        db.flush()
        return [float(value or 0.0) for value in row.vector]

    vector = hash_embedding(query, dimension=dimension)
    if row is None:
        row = QueryEmbeddingCache(
            query_hash=query_hash,
            query_text=query,
            model=model_name,
            vector=vector,
            dimension=dimension,
        )
        db.add(row)
    else:
        row.query_text = query
        row.model = model_name
        row.vector = vector
        row.dimension = dimension
    db.flush()
    sync_vector_column(
        db,
        table_name="query_embedding_cache",
        row_id=row.id,
        vector=vector,
        dimension=dimension,
    )
    return vector


def _build_game_text(item: dict) -> str:
    genres = item.get("genres") or []
    genre_text = " ".join(str(entry) for entry in genres if entry)
    return " ".join(
        value
        for value in [
            str(item.get("name") or ""),
            str(item.get("short_description") or ""),
            genre_text,
        ]
        if value
    ).strip()


def _resolve_game_embedding(db: Session, item: dict, dimension: int) -> list[float]:
    app_id = str(item.get("app_id") or "").strip()
    if not app_id:
        return [0.0] * dimension

    model_name = f"hash-{dimension}"
    row = (
        db.query(GameEmbedding)
        .filter(
            GameEmbedding.app_id == app_id,
            GameEmbedding.model == model_name,
            GameEmbedding.source == "steam",
        )
        .first()
    )
    if row and isinstance(row.vector, list) and len(row.vector) == dimension:
        return [float(value or 0.0) for value in row.vector]

    text = _build_game_text(item)
    vector = hash_embedding(text, dimension=dimension)
    game = db.query(Game).filter(Game.slug == f"steam-{app_id}").first()
    if row is None:
        row = GameEmbedding(
            game_id=game.id if game else None,
            app_id=app_id,
            model=model_name,
            source="steam",
            vector=vector,
            dimension=dimension,
        )
        db.add(row)
    else:
        row.game_id = game.id if game else row.game_id
        row.vector = vector
        row.dimension = dimension
    db.flush()
    sync_vector_column(
        db,
        table_name="game_embeddings",
        row_id=row.id,
        vector=vector,
        dimension=dimension,
    )
    return vector


def _build_user_label_preferences(db: Session, user_id: Optional[str]) -> Counter:
    if not user_id:
        return Counter()
    counter: Counter = Counter()
    entries = (
        db.query(LibraryEntry)
        .filter(LibraryEntry.user_id == user_id)
        .all()
    )
    for entry in entries:
        game = entry.game
        if not game:
            continue
        weight = max(1.0, float(entry.playtime_hours or 0.0))
        for label in (game.genres or []):
            normalized = str(label or "").strip().lower()
            if normalized:
                counter[normalized] += weight
    return counter


def _build_user_app_preferences(db: Session, user_id: Optional[str]) -> Counter:
    if not user_id:
        return Counter()
    rows = (
        db.query(SearchInteraction)
        .filter(SearchInteraction.user_id == user_id)
        .order_by(SearchInteraction.created_at.desc())
        .limit(200)
        .all()
    )
    counter: Counter = Counter()
    for row in rows:
        if row.action not in {"click", "open", "install", "play", "detail"}:
            continue
        app_id = str(row.app_id or "").strip()
        if app_id:
            counter[app_id] += 1
    return counter


def _personalization_score(item: dict, label_pref: Counter, app_pref: Counter) -> float:
    app_id = str(item.get("app_id") or "").strip()
    score = min(100.0, float(app_pref.get(app_id, 0)) * 16.0)
    for label in item.get("genres") or []:
        normalized = str(label or "").strip().lower()
        if not normalized:
            continue
        score += min(35.0, float(label_pref.get(normalized, 0.0)) * 0.8)
    return min(100.0, score)


def _popularity_score(app_id: str, hot_rank: dict[str, int], hot_count: int) -> float:
    if hot_count <= 0:
        return 0.0
    if app_id not in hot_rank:
        return 15.0
    rank = hot_rank[app_id]
    ratio = 1.0 - min(1.0, rank / max(1.0, hot_count))
    return max(0.0, min(100.0, ratio * 100.0))


def _reason_codes(
    lexical_score: float,
    semantic_score: float,
    popularity_score_value: float,
    personalization_score_value: float,
) -> list[str]:
    reasons: list[str] = []
    if lexical_score >= 90:
        reasons.append("lexical_exact_or_prefix")
    elif lexical_score >= 70:
        reasons.append("lexical_relevance")
    if semantic_score >= 75:
        reasons.append("semantic_similarity")
    if popularity_score_value >= 70:
        reasons.append("popular_title")
    if personalization_score_value >= 50:
        reasons.append("personalized_match")
    if not reasons:
        reasons.append("general_relevance")
    return reasons


def search_catalog_ai(
    *,
    db: Session,
    query: str,
    allowed_appids: list[str],
    limit: int,
    offset: int,
    sort: Optional[str],
    mode: str,
    user_id: Optional[str],
    explain: bool,
) -> dict:
    effective_mode = _normalize_mode(mode)
    if effective_mode == "lexical":
        return search_catalog(query, allowed_appids, limit, offset, sort)

    candidate_limit = min(max(limit + offset + 48, 96), max(96, AI_SEARCH_MAX_CANDIDATES))
    lexical_payload = search_catalog(query, allowed_appids, candidate_limit, 0, sort)
    lexical_candidates = list(lexical_payload.get("items") or [])

    # If lexical candidates are sparse, include extra candidates from store search.
    if len(lexical_candidates) < 24:
        allowed_set = set(allowed_appids)
        extra_items: dict[str, dict] = {}
        for item in search_store(query):
            app_id = str(item.get("app_id") or "").strip()
            if app_id and app_id in allowed_set:
                extra_items[app_id] = item
            if len(extra_items) >= 120:
                break
        if extra_items:
            details = get_catalog_page(list(extra_items.keys()))
            for detail in details:
                app_id = str(detail.get("app_id") or "").strip()
                if app_id:
                    extra_items[app_id] = detail
            existing = {str(item.get("app_id") or "").strip() for item in lexical_candidates}
            for app_id, item in extra_items.items():
                if app_id not in existing:
                    lexical_candidates.append(item)
                    existing.add(app_id)

    hot_ids = get_hot_appids()
    hot_rank = {app_id: index for index, app_id in enumerate(hot_ids)}
    hot_count = len(hot_ids)
    dimension = max(16, int(AI_SEARCH_VECTOR_DIM or 128))
    model_name = f"hash-{dimension}"
    query_vector = _resolve_query_embedding(db, query, dimension)
    allowed_set = set(allowed_appids)
    semantic_hits = semantic_search_game_embeddings(
        db,
        query_vector=query_vector,
        model=model_name,
        source="steam",
        limit=min(max(candidate_limit * 4, 200), 1200),
        dimension=dimension,
        allowed_appids=allowed_set,
    )
    semantic_score_map: dict[str, float] = {}
    for app_id, score in semantic_hits:
        if app_id not in semantic_score_map:
            semantic_score_map[app_id] = score

    if semantic_hits:
        existing = {
            str(item.get("app_id") or "").strip()
            for item in lexical_candidates
            if str(item.get("app_id") or "").strip()
        }
        semantic_ids = [
            app_id
            for app_id, _ in semantic_hits
            if app_id and app_id not in existing
        ]
        if semantic_ids:
            semantic_details = get_catalog_page(semantic_ids[: max(candidate_limit, 80)])
            for detail in semantic_details:
                app_id = str(detail.get("app_id") or "").strip()
                if app_id and app_id not in existing and app_id in allowed_set:
                    lexical_candidates.append(detail)
                    existing.add(app_id)

    if not lexical_candidates:
        return {"total": 0, "items": []}

    user_label_pref = _build_user_label_preferences(db, user_id)
    user_app_pref = _build_user_app_preferences(db, user_id)

    scored_items: list[tuple[float, dict]] = []
    for item in lexical_candidates:
        app_id = str(item.get("app_id") or "").strip()
        lexical_score = float(score_candidate(query, item, hot_rank))
        semantic_score = semantic_score_map.get(app_id, 0.0)
        if semantic_score <= 0.0:
            embedding = _resolve_game_embedding(db, item, dimension)
            semantic_cos = cosine_similarity(query_vector, embedding)
            semantic_score = max(0.0, min(100.0, (semantic_cos + 1.0) * 50.0))
        popularity_value = _popularity_score(app_id, hot_rank, hot_count)
        personalization_value = _personalization_score(item, user_label_pref, user_app_pref)

        if effective_mode == "semantic":
            total_score = semantic_score * 0.75 + popularity_value * 0.15 + personalization_value * 0.10
        else:
            total_score = (
                lexical_score * 0.55
                + semantic_score * 0.35
                + popularity_value * 0.05
                + personalization_value * 0.05
            )

        reason_codes = _reason_codes(
            lexical_score=lexical_score,
            semantic_score=semantic_score,
            popularity_score_value=popularity_value,
            personalization_score_value=personalization_value,
        )
        payload = dict(item)
        payload["search_score"] = round(total_score, 3)
        if explain:
            payload["reason_codes"] = reason_codes
            payload["score_breakdown"] = {
                "mode": effective_mode,
                "lexical": round(lexical_score, 3),
                "semantic": round(semantic_score, 3),
                "popularity": round(popularity_value, 3),
                "personalization": round(personalization_value, 3),
            }
        scored_items.append((total_score, payload))

    scored_items.sort(key=lambda entry: (-entry[0], str(entry[1].get("name") or "")))
    ranked_items = [entry[1] for entry in scored_items]
    total = len(ranked_items)
    paged = ranked_items[offset : offset + limit]
    return {"total": total, "items": paged}
