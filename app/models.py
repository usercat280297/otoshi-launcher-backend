import uuid
from datetime import datetime
from sqlalchemy import (
    Column,
    String,
    DateTime,
    Float,
    Integer,
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

    library = relationship("LibraryEntry", back_populates="user", cascade="all, delete")
    downloads = relationship("DownloadTask", back_populates="user", cascade="all, delete")
    telemetry_events = relationship("TelemetryEvent", back_populates="user", cascade="all, delete")
    payments = relationship("PaymentTransaction", back_populates="user", cascade="all, delete")
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
    remote_downloads = relationship("RemoteDownload", back_populates="user", cascade="all, delete")
    streaming_sessions = relationship("StreamingSession", back_populates="user", cascade="all, delete")
    preorders = relationship("Preorder", back_populates="user", cascade="all, delete")
    oauth_identities = relationship(
        "OAuthIdentity",
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
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="downloads")
    game = relationship("Game", back_populates="downloads")


class TelemetryEvent(Base):
    __tablename__ = "telemetry_events"

    id = Column(String(36), primary_key=True, default=generate_id)
    user_id = Column(String(36), ForeignKey("users.id"), nullable=True)
    event_name = Column(String(120), nullable=False)
    payload = Column(JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="telemetry_events")


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
