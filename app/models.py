import uuid
from datetime import datetime
from sqlalchemy import (
    Column,
    String,
    DateTime,
    Float,
    Integer,
    BigInteger,
    Boolean,
    ForeignKey,
    JSON,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from .db import Base


def generate_id() -> str:
    return str(uuid.uuid4())


class User(Base):
    __tablename__ = "users"

    id = Column(String(36), primary_key=True, default=generate_id)
    email = Column(String(255), unique=True, index=True, nullable=False)
    username = Column(String(50), unique=True, index=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    display_name = Column(String(120), nullable=True)
    avatar_url = Column(String(500), nullable=True)
    xp = Column(Integer, default=0)
    level = Column(Integer, default=1)
    wallet_balance = Column(Float, default=0.0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_login = Column(DateTime, nullable=True)
    is_active = Column(Boolean, default=True)
    is_verified = Column(Boolean, default=False)
    role = Column(String(20), default="user")
    membership_tier = Column(String(40), nullable=True)
    membership_expires_at = Column(DateTime, nullable=True)

    library = relationship("LibraryEntry", back_populates="user", cascade="all, delete")
    downloads = relationship("DownloadTask", back_populates="user", cascade="all, delete")
    telemetry_events = relationship("TelemetryEvent", back_populates="user", cascade="all, delete")
    payments = relationship("PaymentTransaction", back_populates="user", cascade="all, delete")
    support_donations = relationship("SupportDonation", back_populates="user", cascade="all, delete")
    licenses = relationship("License", back_populates="user", cascade="all, delete")
    friendships = relationship(
        "Friendship",
        back_populates="user",
        cascade="all, delete",
        foreign_keys="Friendship.user_id",
    )
    achievements = relationship("UserAchievement", back_populates="user", cascade="all, delete")
    cloud_saves = relationship("CloudSave", back_populates="user", cascade="all, delete")
    workshop_items = relationship("WorkshopItem", back_populates="creator", cascade="all, delete")
    workshop_subscriptions = relationship(
        "WorkshopSubscription", back_populates="user", cascade="all, delete"
    )
    workshop_ratings = relationship("WorkshopRating", back_populates="user", cascade="all, delete")
    wishlist_entries = relationship("WishlistEntry", back_populates="user", cascade="all, delete")
    inventory_items = relationship("InventoryItem", back_populates="user", cascade="all, delete")
    trade_offers_sent = relationship(
        "TradeOffer",
        foreign_keys="TradeOffer.from_user_id",
        back_populates="from_user",
        cascade="all, delete",
    )
    trade_offers_received = relationship(
        "TradeOffer",
        foreign_keys="TradeOffer.to_user_id",
        back_populates="to_user",
        cascade="all, delete",
    )
    profile = relationship("UserProfile", back_populates="user", uselist=False, cascade="all, delete")
    reviews = relationship("Review", back_populates="user", cascade="all, delete")
    review_votes = relationship("ReviewVote", back_populates="user", cascade="all, delete")
    screenshots = relationship("Screenshot", back_populates="user", cascade="all, delete")
    activity_events = relationship("ActivityEvent", back_populates="user", cascade="all, delete")
    p2p_peers = relationship("P2PPeer", back_populates="user", cascade="all, delete")
    remote_downloads = relationship("RemoteDownload", back_populates="user", cascade="all, delete")
    streaming_sessions = relationship("StreamingSession", back_populates="user", cascade="all, delete")
    preorders = relationship("Preorder", back_populates="user", cascade="all, delete")
    oauth_identities = relationship(
        "OAuthIdentity",
        back_populates="user",
        cascade="all, delete",
    )
    play_sessions = relationship("GamePlaySession", back_populates="user", cascade="all, delete")
    behavior_events = relationship("UserBehaviorEvent", back_populates="user", cascade="all, delete")
    search_interactions = relationship("SearchInteraction", back_populates="user", cascade="all, delete")
    recommendation_impressions = relationship(
        "RecommendationImpression",
        back_populates="user",
        cascade="all, delete",
    )
    recommendation_feedback = relationship(
        "RecommendationFeedback",
        back_populates="user",
        cascade="all, delete",
    )
    anti_cheat_signals = relationship("AntiCheatSignal", back_populates="user", cascade="all, delete")
    anti_cheat_cases = relationship("AntiCheatCase", back_populates="user", cascade="all, delete")
    support_sessions = relationship("SupportSession", back_populates="user", cascade="all, delete")
    support_suggestions = relationship("SupportSuggestion", back_populates="user", cascade="all, delete")
    privacy_consents = relationship("PrivacyConsent", back_populates="user", cascade="all, delete")
    privacy_deletion_requests = relationship(
        "PrivacyDeletionRequest",
        back_populates="user",
        cascade="all, delete",
    )


class OAuthIdentity(Base):
    __tablename__ = "oauth_identities"
    __table_args__ = (UniqueConstraint("provider", "provider_user_id", name="uq_oauth_identity"),)

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    provider = Column(String(32), nullable=False)
    provider_user_id = Column(String(200), nullable=False, index=True)
    email = Column(String(255), nullable=True)
    display_name = Column(String(120), nullable=True)
    avatar_url = Column(String(500), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("User", back_populates="oauth_identities")


class Game(Base):
    __tablename__ = "games"

    id = Column(String(36), primary_key=True, default=generate_id)
    slug = Column(String(120), unique=True, index=True, nullable=False)
    title = Column(String(200), nullable=False)
    tagline = Column(String(200), nullable=True)
    short_description = Column(String(300), nullable=True)
    description = Column(String(2000), nullable=True)
    studio = Column(String(120), nullable=True)
    developer = Column(String(120), nullable=True)
    publisher = Column(String(120), nullable=True)
    release_date = Column(String(20), nullable=True)
    genres = Column(JSON, default=list)
    tags = Column(JSON, default=list)
    platforms = Column(JSON, default=list)
    price = Column(Float, default=0.0)
    discount_percent = Column(Integer, default=0)
    rating = Column(Float, default=0.0)
    header_image = Column(String(500), nullable=True)
    hero_image = Column(String(500), nullable=True)
    background_image = Column(String(500), nullable=True)
    screenshots = Column(JSON, default=list)
    videos = Column(JSON, default=list)
    system_requirements = Column(JSON, default=dict)
    is_published = Column(Boolean, default=True)
    total_downloads = Column(Integer, default=0)
    average_rating = Column(Float, default=0.0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    library_entries = relationship("LibraryEntry", back_populates="game", cascade="all, delete")
    downloads = relationship("DownloadTask", back_populates="game", cascade="all, delete")
    payments = relationship("PaymentTransaction", back_populates="game", cascade="all, delete")
    licenses = relationship("License", back_populates="game", cascade="all, delete")
    achievements = relationship("Achievement", back_populates="game", cascade="all, delete")
    cloud_saves = relationship("CloudSave", back_populates="game", cascade="all, delete")
    workshop_items = relationship("WorkshopItem", back_populates="game", cascade="all, delete")
    reviews = relationship("Review", back_populates="game", cascade="all, delete")
    user_screenshots = relationship("Screenshot", back_populates="game", cascade="all, delete")
    wishlist_entries = relationship("WishlistEntry", back_populates="game", cascade="all, delete")
    inventory_items = relationship("InventoryItem", back_populates="game", cascade="all, delete")
    trading_cards = relationship("TradingCardDefinition", back_populates="game", cascade="all, delete")
    badges = relationship("BadgeDefinition", back_populates="game", cascade="all, delete")
    dlc_items = relationship("DlcItem", back_populates="base_game", cascade="all, delete")
    depots = relationship("DeveloperDepot", back_populates="game", cascade="all, delete")
    remote_downloads = relationship("RemoteDownload", back_populates="game", cascade="all, delete")
    preorders = relationship("Preorder", back_populates="game", cascade="all, delete")
    play_sessions = relationship("GamePlaySession", back_populates="game", cascade="all, delete")
    recommendation_impressions = relationship(
        "RecommendationImpression",
        back_populates="game",
        cascade="all, delete",
    )
    recommendation_feedback = relationship(
        "RecommendationFeedback",
        back_populates="game",
        cascade="all, delete",
    )
    embeddings = relationship("GameEmbedding", back_populates="game", cascade="all, delete")
    graphics_config = relationship(
        "GameGraphicsConfig",
        back_populates="game",
        uselist=False,
        cascade="all, delete",
    )


class GameGraphicsConfig(Base):
    __tablename__ = "game_graphics_configs"
    __table_args__ = (
        UniqueConstraint("game_id", name="uq_game_graphics_config_game_id"),
    )

    id = Column(String(36), primary_key=True, default=generate_id)
    game_id = Column(String(36), ForeignKey("games.id"), nullable=False)
    dx12_flags = Column(JSON, default=list)
    dx11_flags = Column(JSON, default=list)
    vulkan_flags = Column(JSON, default=list)
    overlay_enabled = Column(Boolean, default=True)
    recommended_api = Column(String(20), nullable=True)
    executable = Column(String(260), nullable=True)
    game_dir = Column(String(260), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    game = relationship("Game", back_populates="graphics_config")


class SteamGridDBCache(Base):
    __tablename__ = "steamgriddb_cache"

    id = Column(String(36), primary_key=True, default=generate_id)
    steam_app_id = Column(String(20), unique=True, index=True, nullable=False)
    title = Column(String(300), nullable=True)
    sgdb_game_id = Column(Integer, nullable=True)
    grid_url = Column(String(500), nullable=True)
    hero_url = Column(String(500), nullable=True)
    logo_url = Column(String(500), nullable=True)
    icon_url = Column(String(500), nullable=True)
    source = Column(String(30), default="steamgriddb")
    fetched_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class SteamTitle(Base):
    __tablename__ = "steam_titles"

    id = Column(String(36), primary_key=True, default=generate_id)
    app_id = Column(String(20), unique=True, index=True, nullable=False)
    name = Column(String(300), nullable=False)
    normalized_name = Column(String(300), index=True, nullable=True)
    title_type = Column(String(30), nullable=True)
    release_date = Column(String(64), nullable=True)
    developer = Column(String(200), nullable=True)
    publisher = Column(String(200), nullable=True)
    platform_flags = Column(JSON, default=dict)
    state = Column(String(30), default="active")
    source = Column(String(40), default="steam_api")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    metadata_row = relationship(
        "SteamTitleMetadata",
        back_populates="title",
        uselist=False,
        cascade="all, delete-orphan",
    )
    assets_row = relationship(
        "SteamTitleAsset",
        back_populates="title",
        uselist=False,
        cascade="all, delete-orphan",
    )
    aliases = relationship(
        "SteamTitleAlias",
        back_populates="title",
        cascade="all, delete-orphan",
    )
    steamdb_row = relationship(
        "SteamDbEnrichment",
        back_populates="title",
        uselist=False,
        cascade="all, delete-orphan",
    )


class SteamTitleMetadata(Base):
    __tablename__ = "steam_title_metadata"
    __table_args__ = (UniqueConstraint("steam_title_id", name="uq_steam_title_metadata_title_id"),)

    id = Column(String(36), primary_key=True, default=generate_id)
    steam_title_id = Column(String(36), ForeignKey("steam_titles.id"), nullable=False)
    short_description = Column(Text, nullable=True)
    long_description = Column(Text, nullable=True)
    genres = Column(JSON, default=list)
    tags = Column(JSON, default=list)
    platforms = Column(JSON, default=list)
    requirements = Column(JSON, default=dict)
    reviews = Column(JSON, default=dict)
    players = Column(JSON, default=dict)
    dlc_graph = Column(JSON, default=dict)
    summary_payload = Column(JSON, default=dict)
    detail_payload = Column(JSON, default=dict)
    media_payload = Column(JSON, default=dict)
    last_refreshed_at = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    title = relationship("SteamTitle", back_populates="metadata_row")


class SteamTitleAsset(Base):
    __tablename__ = "steam_title_assets"
    __table_args__ = (UniqueConstraint("steam_title_id", name="uq_steam_title_assets_title_id"),)

    id = Column(String(36), primary_key=True, default=generate_id)
    steam_title_id = Column(String(36), ForeignKey("steam_titles.id"), nullable=False)
    selected_source = Column(String(30), default="steam")
    sgdb_assets = Column(JSON, default=dict)
    epic_assets = Column(JSON, default=dict)
    steam_assets = Column(JSON, default=dict)
    selected_assets = Column(JSON, default=dict)
    quality_score = Column(Float, default=0.0)
    version = Column(Integer, default=1)
    fetched_at = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    title = relationship("SteamTitle", back_populates="assets_row")


class SteamTitleAlias(Base):
    __tablename__ = "steam_title_aliases"

    id = Column(String(36), primary_key=True, default=generate_id)
    steam_title_id = Column(String(36), ForeignKey("steam_titles.id"), nullable=False, index=True)
    alias = Column(String(300), nullable=False)
    normalized_alias = Column(String(300), nullable=False, index=True)
    locale = Column(String(12), default="en")
    source = Column(String(30), default="steam")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    title = relationship("SteamTitle", back_populates="aliases")


class SteamDbEnrichment(Base):
    __tablename__ = "steamdb_enrichment"
    __table_args__ = (UniqueConstraint("steam_title_id", name="uq_steamdb_enrichment_title_id"),)

    id = Column(String(36), primary_key=True, default=generate_id)
    steam_title_id = Column(String(36), ForeignKey("steam_titles.id"), nullable=False)
    price_history = Column(JSON, default=list)
    hidden_tags = Column(JSON, default=list)
    depots = Column(JSON, default=list)
    branch_map = Column(JSON, default=dict)
    payload = Column(JSON, default=dict)
    confidence = Column(Float, default=0.0)
    source = Column(String(30), default="steamdb_structured")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    title = relationship("SteamTitle", back_populates="steamdb_row")


class CrossStoreMapping(Base):
    __tablename__ = "cross_store_mapping"

    id = Column(String(36), primary_key=True, default=generate_id)
    steam_app_id = Column(String(20), index=True, nullable=False)
    epic_product_id = Column(String(120), nullable=False, index=True)
    confidence = Column(Float, default=0.0)
    evidence = Column(JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class IngestJob(Base):
    __tablename__ = "ingest_jobs"

    id = Column(String(36), primary_key=True, default=generate_id)
    job_type = Column(String(40), nullable=False, index=True)
    status = Column(String(20), nullable=False, default="pending", index=True)
    source = Column(String(40), nullable=True)
    processed_count = Column(Integer, default=0)
    success_count = Column(Integer, default=0)
    failure_count = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    meta = Column(JSON, default=dict)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class IngestCursor(Base):
    __tablename__ = "ingest_cursors"

    id = Column(String(36), primary_key=True, default=generate_id)
    cursor_key = Column(String(120), unique=True, index=True, nullable=False)
    cursor_value = Column(String(500), nullable=True)
    cursor_meta = Column(JSON, default=dict)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)


class AssetJob(Base):
    __tablename__ = "asset_jobs"

    id = Column(String(36), primary_key=True, default=generate_id)
    app_id = Column(String(20), index=True, nullable=False)
    status = Column(String(20), default="pending", index=True)
    priority = Column(Integer, default=100)
    retries = Column(Integer, default=0)
    last_error = Column(Text, nullable=True)
    result_source = Column(String(20), nullable=True)
    result_meta = Column(JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class SaveSyncState(Base):
    __tablename__ = "save_sync_state"
    __table_args__ = (UniqueConstraint("user_id", "app_id", name="uq_save_sync_state_user_app"),)

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=True, index=True)
    app_id = Column(String(20), nullable=False, index=True)
    version_vector = Column(JSON, default=dict)
    checksum_manifest = Column(JSON, default=dict)
    device_state = Column(JSON, default=dict)
    launch_options = Column(JSON, default=dict)
    last_sync_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class SaveSyncEvent(Base):
    __tablename__ = "save_sync_events"

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=True, index=True)
    app_id = Column(String(20), nullable=False, index=True)
    event_type = Column(String(40), nullable=False, index=True)
    payload = Column(JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)


class LibraryEntry(Base):
    __tablename__ = "library_entries"

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    game_id = Column(String(36), ForeignKey("games.id"), nullable=False)
    purchased_at = Column(DateTime, default=datetime.utcnow)
    installed_version = Column(String(20), nullable=True)
    playtime_hours = Column(Float, default=0.0)
    last_played_at = Column(DateTime, nullable=True)

    user = relationship("User", back_populates="library")
    game = relationship("Game", back_populates="library_entries")


class DownloadTask(Base):
    __tablename__ = "download_tasks"

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    game_id = Column(String(36), ForeignKey("games.id"), nullable=False)
    status = Column(String(20), default="queued")
    progress = Column(Integer, default=0)
    speed_mbps = Column(Float, default=0.0)
    eta_minutes = Column(Integer, default=0)
    downloaded_bytes = Column(BigInteger, default=0)
    total_bytes = Column(BigInteger, default=0)
    network_bps = Column(BigInteger, default=0)
    disk_read_bps = Column(BigInteger, default=0)
    disk_write_bps = Column(BigInteger, default=0)
    read_bytes = Column(BigInteger, default=0)
    written_bytes = Column(BigInteger, default=0)
    remaining_bytes = Column(BigInteger, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="downloads")
    game = relationship("Game", back_populates="downloads")


class GamePlaySession(Base):
    __tablename__ = "game_play_sessions"

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    game_id = Column(String(36), ForeignKey("games.id"), nullable=False)
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    ended_at = Column(DateTime, nullable=True)
    duration_sec = Column(Integer, default=0)
    exit_code = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("User", back_populates="play_sessions")
    game = relationship("Game", back_populates="play_sessions")


class TelemetryEvent(Base):
    __tablename__ = "telemetry_events"

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=True)
    event_name = Column(String(120), nullable=False)
    payload = Column(JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="telemetry_events")


class UserBehaviorEvent(Base):
    __tablename__ = "user_behavior_events"

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=True, index=True)
    event_type = Column(String(80), nullable=False, index=True)
    source = Column(String(40), nullable=False, default="launcher")
    payload = Column(JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    user = relationship("User", back_populates="behavior_events")


class SearchInteraction(Base):
    __tablename__ = "search_interactions"

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=True, index=True)
    query = Column(String(300), nullable=False, index=True)
    action = Column(String(40), nullable=False, default="submit", index=True)
    app_id = Column(String(20), nullable=True, index=True)
    dwell_ms = Column(Integer, default=0)
    payload = Column(JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    user = relationship("User", back_populates="search_interactions")


class RecommendationImpression(Base):
    __tablename__ = "recommendation_impressions"

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=True, index=True)
    game_id = Column(String(36), ForeignKey("games.id"), nullable=True, index=True)
    app_id = Column(String(20), nullable=True, index=True)
    recommendation_id = Column(String(64), nullable=True, index=True)
    rank_position = Column(Integer, default=0)
    algorithm_version = Column(String(40), default="v2")
    context = Column(String(80), default="discovery")
    payload = Column(JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    user = relationship("User", back_populates="recommendation_impressions")
    game = relationship("Game", back_populates="recommendation_impressions")
    feedback = relationship("RecommendationFeedback", back_populates="impression", cascade="all, delete")


class RecommendationFeedback(Base):
    __tablename__ = "recommendation_feedback"

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=True, index=True)
    impression_id = Column(String(36), ForeignKey("recommendation_impressions.id"), nullable=True, index=True)
    game_id = Column(String(36), ForeignKey("games.id"), nullable=True, index=True)
    app_id = Column(String(20), nullable=True, index=True)
    feedback_type = Column(String(40), nullable=False, index=True)
    value = Column(Float, default=1.0)
    payload = Column(JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    user = relationship("User", back_populates="recommendation_feedback")
    impression = relationship("RecommendationImpression", back_populates="feedback")
    game = relationship("Game", back_populates="recommendation_feedback")


class GameEmbedding(Base):
    __tablename__ = "game_embeddings"
    __table_args__ = (
        UniqueConstraint("app_id", "model", "source", name="uq_game_embedding_app_model_source"),
    )

    id = Column(String(36), primary_key=True, default=generate_id)
    game_id = Column(String(36), ForeignKey("games.id"), nullable=True, index=True)
    app_id = Column(String(20), nullable=False, index=True)
    model = Column(String(80), nullable=False, default="hash-128")
    source = Column(String(40), nullable=False, default="steam")
    vector = Column(JSON, default=list)
    dimension = Column(Integer, default=128)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)

    game = relationship("Game", back_populates="embeddings")


class QueryEmbeddingCache(Base):
    __tablename__ = "query_embedding_cache"

    id = Column(String(36), primary_key=True, default=generate_id)
    query_hash = Column(String(64), unique=True, nullable=False, index=True)
    query_text = Column(String(300), nullable=False)
    model = Column(String(80), nullable=False, default="hash-128")
    vector = Column(JSON, default=list)
    dimension = Column(Integer, default=128)
    last_used_at = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class AntiCheatSignal(Base):
    __tablename__ = "anti_cheat_signals"

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=True, index=True)
    device_id = Column(String(120), nullable=False, index=True)
    signal_type = Column(String(80), nullable=False, index=True)
    severity = Column(Integer, default=1, index=True)
    payload = Column(JSON, default=dict)
    observed_at = Column(DateTime, default=datetime.utcnow, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    user = relationship("User", back_populates="anti_cheat_signals")


class AntiCheatCase(Base):
    __tablename__ = "anti_cheat_cases"

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=True, index=True)
    device_id = Column(String(120), nullable=False, index=True)
    status = Column(String(40), default="open", index=True)
    risk_score = Column(Float, default=0.0, index=True)
    risk_level = Column(String(20), default="low")
    reason_codes = Column(JSON, default=list)
    recommended_action = Column(String(80), default="monitor")
    latest_signal_at = Column(DateTime, default=datetime.utcnow, index=True)
    payload = Column(JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("User", back_populates="anti_cheat_cases")


class SupportSession(Base):
    __tablename__ = "support_sessions"

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=True, index=True)
    channel = Column(String(40), default="launcher")
    topic = Column(String(120), nullable=True)
    status = Column(String(30), default="open", index=True)
    context_payload = Column(JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("User", back_populates="support_sessions")
    suggestions = relationship("SupportSuggestion", back_populates="session", cascade="all, delete")


class SupportSuggestion(Base):
    __tablename__ = "support_suggestions"

    id = Column(String(36), primary_key=True, default=generate_id)
    session_id = Column(String(36), ForeignKey("support_sessions.id"), nullable=False, index=True)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=True, index=True)
    provider = Column(String(40), nullable=False, default="none")
    model = Column(String(80), nullable=False, default="none")
    prompt_hash = Column(String(64), nullable=False, index=True)
    input_text = Column(Text, nullable=False)
    suggestion_text = Column(Text, nullable=False)
    confidence = Column(Float, default=0.0)
    cached = Column(Boolean, default=False)
    accepted = Column(Boolean, nullable=True)
    payload = Column(JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    session = relationship("SupportSession", back_populates="suggestions")
    user = relationship("User", back_populates="support_suggestions")


class PrivacyConsent(Base):
    __tablename__ = "privacy_consents"
    __table_args__ = (
        UniqueConstraint("user_id", "category", name="uq_privacy_consent_user_category"),
    )

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False, index=True)
    category = Column(String(50), nullable=False, index=True)
    granted = Column(Boolean, default=False, index=True)
    source = Column(String(40), default="settings")
    payload = Column(JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("User", back_populates="privacy_consents")


class PrivacyDeletionRequest(Base):
    __tablename__ = "privacy_deletion_requests"

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False, index=True)
    scope = Column(JSON, default=list)
    status = Column(String(30), default="pending", index=True)
    result_payload = Column(JSON, default=dict)
    requested_at = Column(DateTime, default=datetime.utcnow, index=True)
    processed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("User", back_populates="privacy_deletion_requests")


class License(Base):
    __tablename__ = "licenses"

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    game_id = Column(String(36), ForeignKey("games.id"), nullable=False)
    license_key = Column(String(120), unique=True, nullable=False)
    hardware_id = Column(String(120), nullable=True)
    issued_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime, nullable=True)
    max_activations = Column(Integer, default=1)
    current_activations = Column(Integer, default=0)
    status = Column(String(20), default="active")
    signature = Column(String(512), nullable=True)

    user = relationship("User", back_populates="licenses")
    game = relationship("Game", back_populates="licenses")


class PaymentTransaction(Base):
    __tablename__ = "payment_transactions"

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    game_id = Column(String(36), ForeignKey("games.id"), nullable=True)
    amount = Column(Float, default=0.0)
    currency = Column(String(10), default="USD")
    status = Column(String(20), default="completed")
    provider = Column(String(40), default="internal")
    created_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="payments")
    game = relationship("Game", back_populates="payments")


class SupportDonation(Base):
    __tablename__ = "support_donations"

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False, index=True)
    amount = Column(Float, default=0.0)
    currency = Column(String(10), default="USD")
    provider = Column(String(40), default="donation")
    note = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    user = relationship("User", back_populates="support_donations")


class Friendship(Base):
    __tablename__ = "friendships"

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    friend_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    status = Column(String(20), default="pending")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("User", foreign_keys=[user_id], back_populates="friendships")
    friend = relationship("User", foreign_keys=[friend_id])


class Achievement(Base):
    __tablename__ = "achievements"

    id = Column(String(36), primary_key=True, default=generate_id)
    game_id = Column(String(36), ForeignKey("games.id"), nullable=False)
    key = Column(String(120), nullable=False)
    title = Column(String(200), nullable=False)
    description = Column(String(500), nullable=True)
    points = Column(Integer, default=0)
    icon_url = Column(String(500), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    game = relationship("Game", back_populates="achievements")


class UserAchievement(Base):
    __tablename__ = "user_achievements"

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    achievement_id = Column(String(36), ForeignKey("achievements.id"), nullable=False)
    unlocked_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="achievements")
    achievement = relationship("Achievement")


class CloudSave(Base):
    __tablename__ = "cloud_saves"

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    game_id = Column(String(36), ForeignKey("games.id"), nullable=False)
    payload = Column(JSON, default=dict)
    version = Column(String(40), default="1")
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("User", back_populates="cloud_saves")
    game = relationship("Game", back_populates="cloud_saves")


class ChatMessage(Base):
    __tablename__ = "chat_messages"

    id = Column(String(36), primary_key=True, default=generate_id)
    sender_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    recipient_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    body = Column(String(1000), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    sender = relationship("User", foreign_keys=[sender_id])
    recipient = relationship("User", foreign_keys=[recipient_id])


class WorkshopItem(Base):
    __tablename__ = "workshop_items"

    id = Column(String(36), primary_key=True, default=generate_id)
    game_id = Column(String(36), ForeignKey("games.id"), nullable=False)
    creator_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    title = Column(String(200), nullable=False)
    description = Column(Text, nullable=True)
    item_type = Column(String(50), nullable=True)
    visibility = Column(String(20), default="public")
    total_downloads = Column(Integer, default=0)
    total_subscriptions = Column(Integer, default=0)
    rating_up = Column(Integer, default=0)
    rating_down = Column(Integer, default=0)
    tags = Column(JSON, default=list)
    preview_image_url = Column(String(500), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    game = relationship("Game", back_populates="workshop_items")
    creator = relationship("User", back_populates="workshop_items")
    versions = relationship("WorkshopVersion", back_populates="item", cascade="all, delete")
    subscriptions = relationship("WorkshopSubscription", back_populates="item", cascade="all, delete")
    ratings = relationship("WorkshopRating", back_populates="item", cascade="all, delete")


class WorkshopVersion(Base):
    __tablename__ = "workshop_versions"

    id = Column(String(36), primary_key=True, default=generate_id)
    workshop_item_id = Column(String(36), ForeignKey("workshop_items.id"), nullable=False)
    version = Column(String(40), nullable=False)
    changelog = Column(Text, nullable=True)
    file_size = Column(Integer, default=0)
    storage_path = Column(String(500), nullable=True)
    download_url = Column(String(500), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    item = relationship("WorkshopItem", back_populates="versions")


class WorkshopSubscription(Base):
    __tablename__ = "workshop_subscriptions"
    __table_args__ = (UniqueConstraint("user_id", "workshop_item_id", name="uq_workshop_subscription"),)

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    workshop_item_id = Column(String(36), ForeignKey("workshop_items.id"), nullable=False)
    subscribed_at = Column(DateTime, default=datetime.utcnow)
    auto_update = Column(Boolean, default=True)

    user = relationship("User", back_populates="workshop_subscriptions")
    item = relationship("WorkshopItem", back_populates="subscriptions")


class WorkshopRating(Base):
    __tablename__ = "workshop_ratings"
    __table_args__ = (UniqueConstraint("user_id", "workshop_item_id", name="uq_workshop_rating"),)

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    workshop_item_id = Column(String(36), ForeignKey("workshop_items.id"), nullable=False)
    rating = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="workshop_ratings")
    item = relationship("WorkshopItem", back_populates="ratings")


class WishlistEntry(Base):
    __tablename__ = "wishlist_entries"
    __table_args__ = (UniqueConstraint("user_id", "game_id", name="uq_wishlist_entry"),)

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    game_id = Column(String(36), ForeignKey("games.id"), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="wishlist_entries")
    game = relationship("Game", back_populates="wishlist_entries")


class Bundle(Base):
    __tablename__ = "bundles"

    id = Column(String(36), primary_key=True, default=generate_id)
    slug = Column(String(120), unique=True, nullable=False)
    title = Column(String(200), nullable=False)
    description = Column(Text, nullable=True)
    price = Column(Float, default=0.0)
    discount_percent = Column(Integer, default=0)
    game_ids = Column(JSON, default=list)
    created_at = Column(DateTime, default=datetime.utcnow)


class DlcItem(Base):
    __tablename__ = "dlc_items"

    id = Column(String(36), primary_key=True, default=generate_id)
    base_game_id = Column(String(36), ForeignKey("games.id"), nullable=False)
    title = Column(String(200), nullable=False)
    description = Column(Text, nullable=True)
    price = Column(Float, default=0.0)
    is_season_pass = Column(Boolean, default=False)
    release_date = Column(String(20), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    base_game = relationship("Game", back_populates="dlc_items")


class Preorder(Base):
    __tablename__ = "preorders"
    __table_args__ = (UniqueConstraint("user_id", "game_id", name="uq_preorder"),)

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    game_id = Column(String(36), ForeignKey("games.id"), nullable=False)
    preorder_at = Column(DateTime, default=datetime.utcnow)
    status = Column(String(20), default="preordered")
    preload_available = Column(Boolean, default=False)

    user = relationship("User", back_populates="preorders")
    game = relationship("Game", back_populates="preorders")


class UserProfile(Base):
    __tablename__ = "user_profiles"
    __table_args__ = (UniqueConstraint("user_id", name="uq_user_profile"),)

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    headline = Column(String(120), nullable=True)
    bio = Column(Text, nullable=True)
    location = Column(String(120), nullable=True)
    background_image = Column(String(500), nullable=True)
    social_links = Column(JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("User", back_populates="profile")


class Review(Base):
    __tablename__ = "reviews"
    __table_args__ = (UniqueConstraint("user_id", "game_id", name="uq_review"),)

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    game_id = Column(String(36), ForeignKey("games.id"), nullable=False)
    rating = Column(Integer, default=0)
    title = Column(String(200), nullable=True)
    body = Column(Text, nullable=True)
    recommended = Column(Boolean, default=True)
    helpful_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("User", back_populates="reviews")
    game = relationship("Game", back_populates="reviews")
    votes = relationship("ReviewVote", back_populates="review", cascade="all, delete")


class ReviewVote(Base):
    __tablename__ = "review_votes"
    __table_args__ = (UniqueConstraint("user_id", "review_id", name="uq_review_vote"),)

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    review_id = Column(String(36), ForeignKey("reviews.id"), nullable=False)
    helpful = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="review_votes")
    review = relationship("Review", back_populates="votes")


class Screenshot(Base):
    __tablename__ = "screenshots"

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    game_id = Column(String(36), ForeignKey("games.id"), nullable=False)
    image_url = Column(String(500), nullable=False)
    caption = Column(String(200), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="screenshots")
    game = relationship("Game", back_populates="user_screenshots")


class ActivityEvent(Base):
    __tablename__ = "activity_events"

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    event_type = Column(String(40), nullable=False)
    payload = Column(JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="activity_events")


class InventoryItem(Base):
    __tablename__ = "inventory_items"

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    game_id = Column(String(36), ForeignKey("games.id"), nullable=True)
    item_type = Column(String(40), nullable=False)
    name = Column(String(200), nullable=False)
    rarity = Column(String(40), default="common")
    quantity = Column(Integer, default=1)
    item_metadata = Column("metadata", JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="inventory_items")
    game = relationship("Game", back_populates="inventory_items")


class TradingCardDefinition(Base):
    __tablename__ = "trading_card_definitions"

    id = Column(String(36), primary_key=True, default=generate_id)
    game_id = Column(String(36), ForeignKey("games.id"), nullable=False)
    card_name = Column(String(200), nullable=False)
    series = Column(String(60), nullable=True)
    rarity = Column(String(40), default="common")
    created_at = Column(DateTime, default=datetime.utcnow)

    game = relationship("Game", back_populates="trading_cards")


class BadgeDefinition(Base):
    __tablename__ = "badge_definitions"

    id = Column(String(36), primary_key=True, default=generate_id)
    game_id = Column(String(36), ForeignKey("games.id"), nullable=False)
    badge_name = Column(String(200), nullable=False)
    level = Column(Integer, default=1)
    required_cards = Column(JSON, default=list)
    xp_reward = Column(Integer, default=100)

    game = relationship("Game", back_populates="badges")


class TradeOffer(Base):
    __tablename__ = "trade_offers"

    id = Column(String(36), primary_key=True, default=generate_id)
    from_user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    to_user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    offered_item_ids = Column(JSON, default=list)
    requested_item_ids = Column(JSON, default=list)
    status = Column(String(20), default="pending")
    created_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime, nullable=True)

    from_user = relationship("User", foreign_keys=[from_user_id], back_populates="trade_offers_sent")
    to_user = relationship("User", foreign_keys=[to_user_id], back_populates="trade_offers_received")


class DeveloperDepot(Base):
    __tablename__ = "developer_depots"

    id = Column(String(36), primary_key=True, default=generate_id)
    game_id = Column(String(36), ForeignKey("games.id"), nullable=False)
    name = Column(String(120), nullable=False)
    platform = Column(String(40), default="windows")
    branch = Column(String(40), default="main")
    created_at = Column(DateTime, default=datetime.utcnow)

    game = relationship("Game", back_populates="depots")
    builds = relationship("DeveloperBuild", back_populates="depot", cascade="all, delete")


class DeveloperBuild(Base):
    __tablename__ = "developer_builds"

    id = Column(String(36), primary_key=True, default=generate_id)
    depot_id = Column(String(36), ForeignKey("developer_depots.id"), nullable=False)
    version = Column(String(40), nullable=False)
    manifest_json = Column(JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow)

    depot = relationship("DeveloperDepot", back_populates="builds")


class DeveloperAnalyticsSnapshot(Base):
    __tablename__ = "developer_analytics"

    id = Column(String(36), primary_key=True, default=generate_id)
    game_id = Column(String(36), ForeignKey("games.id"), nullable=False)
    metrics = Column(JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow)


class RemoteDownload(Base):
    __tablename__ = "remote_downloads"

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    game_id = Column(String(36), ForeignKey("games.id"), nullable=False)
    target_device = Column(String(120), nullable=False)
    status = Column(String(20), default="queued")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("User", back_populates="remote_downloads")
    game = relationship("Game", back_populates="remote_downloads")


class StreamingSession(Base):
    __tablename__ = "streaming_sessions"

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    game_id = Column(String(36), ForeignKey("games.id"), nullable=True)
    status = Column(String(20), default="created")
    offer = Column(JSON, default=dict)
    answer = Column(JSON, default=dict)
    ice_candidates = Column(JSON, default=list)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("User", back_populates="streaming_sessions")


class LauncherArtifactRecord(Base):
    __tablename__ = "launcher_artifacts"
    __table_args__ = (
        UniqueConstraint("filename", "channel", name="uq_launcher_artifact_filename_channel"),
    )

    id = Column(String(36), primary_key=True, default=generate_id)
    kind = Column(String(32), nullable=False)
    version = Column(String(40), nullable=False)
    filename = Column(String(255), nullable=False)
    size_bytes = Column(BigInteger, nullable=False)
    sha256 = Column(String(64), nullable=False)
    download_url = Column(String(1000), nullable=False)
    platform = Column(String(32), nullable=True)
    channel = Column(String(32), nullable=False, default="stable")
    published_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class P2PPeer(Base):
    __tablename__ = "p2p_peers"
    __table_args__ = (
        UniqueConstraint("user_id", "device_id", name="uq_p2p_peer_user_device"),
    )

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    device_id = Column(String(120), nullable=False)
    port = Column(Integer, nullable=False, default=0)
    addresses = Column(JSON, default=list)
    share_enabled = Column(Boolean, default=True)
    upload_limit_bps = Column(BigInteger, default=0)
    last_seen_at = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("User", back_populates="p2p_peers")
