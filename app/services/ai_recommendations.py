from __future__ import annotations

from collections import Counter
from typing import Optional

from sqlalchemy.orm import Session

from ..models import (
    Game,
    GameEmbedding,
    LibraryEntry,
    RecommendationFeedback,
    RecommendationImpression,
    SearchInteraction,
)
from .ai_gateway import cosine_similarity, hash_embedding
from .steam_catalog import get_catalog_page, get_hot_appids, get_lua_appids
from .vector_store import sync_vector_column

_POSITIVE_FEEDBACK = {"click", "play", "install", "open", "liked", "favorite"}
_NEGATIVE_FEEDBACK = {"skip", "dismiss", "dislike", "hide"}


def _resolve_game_id(db: Session, game_id: Optional[str], app_id: Optional[str]) -> Optional[str]:
    if game_id:
        return game_id
    resolved_app_id = str(app_id or "").strip()
    if not resolved_app_id:
        return None
    game = db.query(Game).filter(Game.slug == f"steam-{resolved_app_id}").first()
    return game.id if game else None


def create_recommendation_impression(
    db: Session,
    *,
    user_id: Optional[str],
    game_id: Optional[str],
    app_id: Optional[str],
    recommendation_id: Optional[str],
    rank_position: int,
    algorithm_version: str,
    context: str,
    payload: dict,
) -> RecommendationImpression:
    row = RecommendationImpression(
        user_id=user_id,
        game_id=_resolve_game_id(db, game_id, app_id),
        app_id=str(app_id or "").strip() or None,
        recommendation_id=recommendation_id,
        rank_position=max(0, int(rank_position or 0)),
        algorithm_version=(algorithm_version or "v2")[:40],
        context=(context or "discovery")[:80],
        payload=payload or {},
    )
    db.add(row)
    db.flush()
    return row


def create_recommendation_feedback(
    db: Session,
    *,
    user_id: Optional[str],
    impression_id: Optional[str],
    game_id: Optional[str],
    app_id: Optional[str],
    feedback_type: str,
    value: float,
    payload: dict,
) -> RecommendationFeedback:
    row = RecommendationFeedback(
        user_id=user_id,
        impression_id=impression_id,
        game_id=_resolve_game_id(db, game_id, app_id),
        app_id=str(app_id or "").strip() or None,
        feedback_type=(feedback_type or "unknown")[:40],
        value=float(value or 0.0),
        payload=payload or {},
    )
    db.add(row)
    db.flush()
    return row


def _hot_rank() -> dict[str, int]:
    hot_ids = get_hot_appids()
    return {app_id: index for index, app_id in enumerate(hot_ids)}


def _popularity_score(app_id: str, rank_map: dict[str, int]) -> float:
    if not rank_map:
        return 0.0
    if app_id not in rank_map:
        return 15.0
    rank = rank_map[app_id]
    ratio = 1.0 - min(1.0, rank / max(1.0, len(rank_map)))
    return max(0.0, min(100.0, ratio * 100.0))


def _load_user_genre_preferences(db: Session, user_id: Optional[str]) -> Counter:
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
        for label in game.genres or []:
            normalized = str(label or "").strip().lower()
            if normalized:
                counter[normalized] += weight
    return counter


def _load_feedback_preferences(db: Session, user_id: Optional[str]) -> tuple[Counter, set[str], set[str]]:
    positive_app_counter: Counter = Counter()
    negative_apps: set[str] = set()
    if not user_id:
        return positive_app_counter, negative_apps, set()

    rows = (
        db.query(RecommendationFeedback)
        .filter(RecommendationFeedback.user_id == user_id)
        .order_by(RecommendationFeedback.created_at.desc())
        .limit(300)
        .all()
    )
    positive_apps: set[str] = set()
    for row in rows:
        app_id = str(row.app_id or "").strip()
        feedback_type = str(row.feedback_type or "").strip().lower()
        if not app_id:
            continue
        if feedback_type in _NEGATIVE_FEEDBACK:
            negative_apps.add(app_id)
        if feedback_type in _POSITIVE_FEEDBACK:
            positive_app_counter[app_id] += max(1.0, float(row.value or 1.0))
            positive_apps.add(app_id)
    return positive_app_counter, negative_apps, positive_apps


def _load_recent_search_apps(db: Session, user_id: Optional[str]) -> Counter:
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
        if row.action not in {"click", "open", "detail", "install", "play"}:
            continue
        app_id = str(row.app_id or "").strip()
        if app_id:
            counter[app_id] += 1
    return counter


def _embedding_for_app(
    db: Session,
    app_id: str,
    *,
    fallback_text: str = "",
    dimension: int = 128,
) -> list[float]:
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

    vector = hash_embedding(fallback_text or app_id, dimension=dimension)
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


def _build_user_vector(
    db: Session,
    positive_apps: set[str],
    dimension: int,
) -> list[float]:
    if not positive_apps:
        return [0.0] * dimension
    vectors: list[list[float]] = []
    for app_id in positive_apps:
        vectors.append(_embedding_for_app(db, app_id, dimension=dimension))
    if not vectors:
        return [0.0] * dimension
    combined = [0.0] * dimension
    for vector in vectors:
        for i in range(min(dimension, len(vector))):
            combined[i] += float(vector[i] or 0.0)
    norm = sum(value * value for value in combined) ** 0.5
    if norm <= 1e-9:
        return [0.0] * dimension
    return [value / norm for value in combined]


def _score_reason_codes(genre_score: float, semantic_score: float, popularity_score: float, affinity_score: float) -> list[str]:
    reasons: list[str] = []
    if genre_score >= 40:
        reasons.append("genre_match")
    if semantic_score >= 65:
        reasons.append("behavioral_similarity")
    if popularity_score >= 70:
        reasons.append("trending")
    if affinity_score >= 40:
        reasons.append("recent_interest")
    if not reasons:
        reasons.append("baseline_candidate")
    return reasons


def recommend_v2(
    db: Session,
    *,
    user_id: Optional[str],
    limit: int = 12,
    offset: int = 0,
) -> dict:
    allowed_appids = get_lua_appids()
    if not allowed_appids:
        return {"total": 0, "offset": offset, "limit": limit, "items": []}
    allowed_set = set(allowed_appids)

    rank_map = _hot_rank()
    genre_pref = _load_user_genre_preferences(db, user_id)
    feedback_counter, negative_apps, positive_apps = _load_feedback_preferences(db, user_id)
    search_counter = _load_recent_search_apps(db, user_id)

    candidate_ids: list[str] = []
    seen: set[str] = set()
    for app_id in get_hot_appids():
        if app_id in seen or app_id not in allowed_set:
            continue
        seen.add(app_id)
        candidate_ids.append(app_id)
        if len(candidate_ids) >= 260:
            break
    for app_id in allowed_appids:
        if app_id in seen:
            continue
        seen.add(app_id)
        candidate_ids.append(app_id)
        if len(candidate_ids) >= 320:
            break

    if not candidate_ids:
        return {"total": 0, "offset": offset, "limit": limit, "items": []}

    summaries = get_catalog_page(candidate_ids)
    if not summaries:
        return {"total": 0, "offset": offset, "limit": limit, "items": []}

    user_vector = _build_user_vector(db, positive_apps, dimension=128)
    has_user_profile = bool(genre_pref or feedback_counter or search_counter or positive_apps)
    scored: list[tuple[float, dict]] = []

    for item in summaries:
        app_id = str(item.get("app_id") or "").strip()
        if not app_id or app_id in negative_apps:
            continue

        genre_score = 0.0
        for label in item.get("genres") or []:
            normalized = str(label or "").strip().lower()
            if normalized:
                genre_score += min(20.0, float(genre_pref.get(normalized, 0.0)))
        genre_score = min(100.0, genre_score)

        popularity_value = _popularity_score(app_id, rank_map)
        affinity_score = min(
            100.0,
            float(search_counter.get(app_id, 0)) * 15.0 + float(feedback_counter.get(app_id, 0)) * 8.0,
        )
        item_vector = _embedding_for_app(
            db,
            app_id,
            fallback_text=f"{item.get('name') or ''} {item.get('short_description') or ''}",
            dimension=128,
        )
        semantic_score = max(0.0, min(100.0, (cosine_similarity(user_vector, item_vector) + 1.0) * 50.0))

        if has_user_profile:
            total_score = (
                genre_score * 0.40
                + semantic_score * 0.35
                + popularity_value * 0.15
                + affinity_score * 0.10
            )
        else:
            total_score = popularity_value * 0.85 + genre_score * 0.15

        payload = dict(item)
        payload["search_score"] = round(total_score, 3)
        payload["reason_codes"] = _score_reason_codes(
            genre_score=genre_score,
            semantic_score=semantic_score,
            popularity_score=popularity_value,
            affinity_score=affinity_score,
        )
        payload["score_breakdown"] = {
            "genre": round(genre_score, 3),
            "semantic": round(semantic_score, 3),
            "popularity": round(popularity_value, 3),
            "affinity": round(affinity_score, 3),
        }
        scored.append((total_score, payload))

    scored.sort(key=lambda row: (-row[0], str(row[1].get("name") or "")))
    ranked_items = [item for _, item in scored]
    total = len(ranked_items)
    paged = ranked_items[offset : offset + limit]
    return {"total": total, "offset": offset, "limit": limit, "items": paged}
