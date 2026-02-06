from typing import List, Tuple
from collections import Counter

from sqlalchemy.orm import Session

from ..models import Game, LibraryEntry, WishlistEntry


def _game_labels(game: Game) -> List[str]:
    labels: List[str] = []
    for collection in (game.genres or [], game.tags or []):
        for entry in collection:
            if entry:
                labels.append(str(entry).lower())
    return labels


def build_user_preferences(db: Session, user_id: str) -> Counter:
    counter = Counter()
    entries = (
        db.query(LibraryEntry)
        .filter(LibraryEntry.user_id == user_id)
        .all()
    )
    for entry in entries:
        game = entry.game
        if not game:
            continue
        weight = max(1.0, entry.playtime_hours or 0.0)
        for label in _game_labels(game):
            counter[label] += weight
    return counter


def recommend_games(db: Session, user_id: str, limit: int = 12) -> List[Game]:
    preferences = build_user_preferences(db, user_id)
    owned_ids = {entry.game_id for entry in db.query(LibraryEntry).filter(LibraryEntry.user_id == user_id).all()}
    wishlist_ids = {entry.game_id for entry in db.query(WishlistEntry).filter(WishlistEntry.user_id == user_id).all()}

    candidates = (
        db.query(Game)
        .filter(Game.is_published == True)
        .all()
    )
    scored = []
    for game in candidates:
        if game.id in owned_ids:
            continue
        base = (game.rating or 0.0) * 2.0 + (game.total_downloads or 0) * 0.01
        if game.id in wishlist_ids:
            base += 2.0
        overlap = 0.0
        for label in _game_labels(game):
            overlap += preferences.get(label, 0.0)
        scored.append((base + overlap, game))

    scored.sort(key=lambda item: item[0], reverse=True)
    return [game for _, game in scored[:limit]]


def similar_games(db: Session, game_id: str, limit: int = 6) -> List[Game]:
    target = db.query(Game).filter(Game.id == game_id).first()
    if not target:
        return []
    target_labels = set(_game_labels(target))
    candidates = (
        db.query(Game)
        .filter(Game.is_published == True, Game.id != game_id)
        .all()
    )
    scored: List[Tuple[float, Game]] = []
    for game in candidates:
        labels = set(_game_labels(game))
        overlap = len(target_labels.intersection(labels))
        base = (game.rating or 0.0) + overlap * 1.5
        scored.append((base, game))
    scored.sort(key=lambda item: item[0], reverse=True)
    return [game for _, game in scored[:limit]]
