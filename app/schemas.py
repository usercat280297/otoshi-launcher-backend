from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel, EmailStr, Field, field_validator


class UserCreate(BaseModel):
    email: EmailStr
    username: str
    password: str
    display_name: Optional[str] = None
    birthdate: Optional[datetime] = None

    @field_validator("username")
    @classmethod
    def username_alphanumeric(cls, value: str) -> str:
        if not value.isalnum():
            raise ValueError("must be alphanumeric")
        if len(value) < 3:
            raise ValueError("must be at least 3 characters")
        return value

    @field_validator("password")
    @classmethod
    def password_strength(cls, value: str) -> str:
        if len(value) < 8:
            raise ValueError("must be at least 8 characters")
        if not any(char.isupper() for char in value):
            raise ValueError("must contain uppercase")
        if not any(char.islower() for char in value):
            raise ValueError("must contain lowercase")
        if not any(char.isdigit() for char in value):
            raise ValueError("must contain digit")
        return value


class UserLogin(BaseModel):
    email: Optional[EmailStr] = None
    username: Optional[str] = None
    email_or_username: Optional[str] = None
    password: str


class UserOut(BaseModel):
    id: str
    email: EmailStr
    username: str
    display_name: Optional[str]
    avatar_url: Optional[str] = None
    xp: Optional[int] = None
    level: Optional[int] = None
    wallet_balance: Optional[float] = None
    created_at: Optional[datetime] = None
    last_login: Optional[datetime] = None
    is_active: Optional[bool] = None
    is_verified: Optional[bool] = None
    role: Optional[str] = None

    class Config:
        from_attributes = True


class Token(BaseModel):
    access_token: str
    token_type: str
    user: UserOut
    refresh_token: Optional[str] = None


class TokenRefresh(BaseModel):
    refresh_token: str


class OAuthProviderOut(BaseModel):
    provider: str
    label: str
    enabled: bool


class OAuthExchangeIn(BaseModel):
    code: str


class LocaleSettingIn(BaseModel):
    locale: str


class LocaleSettingOut(BaseModel):
    locale: str
    source: str
    system_locale: str
    supported: List[str]


class UserUpdate(BaseModel):
    display_name: Optional[str] = None
    avatar_url: Optional[str] = None


class UserPublicOut(BaseModel):
    id: str
    username: str
    display_name: Optional[str]
    avatar_url: Optional[str] = None
    xp: Optional[int] = None
    level: Optional[int] = None

    class Config:
        from_attributes = True


class GameOut(BaseModel):
    id: str
    slug: str
    title: str
    tagline: Optional[str]
    short_description: Optional[str] = None
    description: Optional[str]
    studio: Optional[str]
    developer: Optional[str] = None
    publisher: Optional[str] = None
    release_date: Optional[str]
    genres: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    platforms: Optional[List[str]] = None
    price: float
    discount_percent: int
    rating: float
    header_image: Optional[str]
    hero_image: Optional[str]
    background_image: Optional[str] = None
    screenshots: Optional[List[str]] = None
    videos: Optional[List[dict]] = None
    system_requirements: Optional[dict] = None
    total_downloads: Optional[int] = None
    average_rating: Optional[float] = None

    class Config:
        from_attributes = True


class SteamGridDBAssetOut(BaseModel):
    game_id: int
    name: str
    grid: Optional[str] = None
    hero: Optional[str] = None
    logo: Optional[str] = None
    icon: Optional[str] = None


class SteamPriceOut(BaseModel):
    initial: Optional[int] = None
    final: Optional[int] = None
    discount_percent: Optional[int] = None
    currency: Optional[str] = None
    formatted: Optional[str] = None
    final_formatted: Optional[str] = None


class SteamCatalogItemOut(BaseModel):
    app_id: str
    name: str
    short_description: Optional[str] = None
    header_image: Optional[str] = None
    capsule_image: Optional[str] = None
    background: Optional[str] = None
    required_age: Optional[int] = None
    price: Optional[SteamPriceOut] = None
    genres: Optional[List[str]] = None
    release_date: Optional[str] = None
    platforms: Optional[List[str]] = None
    denuvo: Optional[bool] = None


class SteamGameDetailOut(SteamCatalogItemOut):
    about_the_game: Optional[str] = None
    about_the_game_html: Optional[str] = None
    detailed_description: Optional[str] = None
    detailed_description_html: Optional[str] = None
    developers: Optional[List[str]] = None
    publishers: Optional[List[str]] = None
    categories: Optional[List[str]] = None
    screenshots: Optional[List[str]] = None
    movies: Optional[List[dict]] = None
    pc_requirements: Optional[dict] = None
    metacritic: Optional[dict] = None
    recommendations: Optional[int] = None
    website: Optional[str] = None
    support_info: Optional[dict] = None


class SteamCatalogOut(BaseModel):
    total: int
    offset: int
    limit: int
    items: List[SteamCatalogItemOut]


class SearchHistoryIn(BaseModel):
    query: str = Field(min_length=1)


class SearchHistoryItemOut(BaseModel):
    query: str
    count: int
    last_used: Optional[str] = None


class SearchHistoryOut(BaseModel):
    items: List[SearchHistoryItemOut]


class AgeGateIn(BaseModel):
    year: int = Field(ge=1900, le=2100)
    month: int = Field(ge=1, le=12)
    day: int = Field(ge=1, le=31)
    required_age: int = Field(ge=0, le=99)


class AgeGateOut(BaseModel):
    allowed: bool
    age: int
    required_age: int


class FixOptionOut(BaseModel):
    link: str
    name: Optional[str] = None
    note: Optional[str] = None
    version: Optional[str] = None
    size: Optional[int] = None
    recommended: bool = False


class FixEntryOut(BaseModel):
    app_id: str
    name: str
    steam: Optional[SteamCatalogItemOut] = None
    options: List[FixOptionOut]
    denuvo: Optional[bool] = None


class FixCatalogOut(BaseModel):
    total: int
    offset: int
    limit: int
    items: List[FixEntryOut]


class DownloadMethodOut(BaseModel):
    id: str
    label: str
    description: Optional[str] = None
    recommended: bool = False
    enabled: bool = True


class DownloadVersionOut(BaseModel):
    id: str
    label: str
    is_latest: bool = False
    size_bytes: Optional[int] = None


class DownloadOptionsOut(BaseModel):
    app_id: str
    name: str
    size_bytes: Optional[int] = None
    size_label: Optional[str] = None
    methods: List[DownloadMethodOut]
    versions: List[DownloadVersionOut]
    online_fix: List[FixOptionOut]
    bypass: Optional[FixOptionOut] = None
    install_root: str
    install_path: str
    free_bytes: Optional[int] = None
    total_bytes: Optional[int] = None


class DownloadPrepareIn(BaseModel):
    method: str
    version: str
    install_path: str
    create_subfolder: bool = True


class LibraryEntryOut(BaseModel):
    id: str
    purchased_at: datetime
    installed_version: Optional[str]
    playtime_hours: float
    game: GameOut

    class Config:
        from_attributes = True


class DownloadTaskOut(BaseModel):
    id: str
    status: str
    progress: int
    speed_mbps: float
    eta_minutes: int
    game: GameOut

    class Config:
        from_attributes = True


class TelemetryEventIn(BaseModel):
    name: str
    payload: dict = Field(default_factory=dict)


class TelemetryEventOut(BaseModel):
    id: str
    event_name: str
    payload: dict
    created_at: datetime

    class Config:
        from_attributes = True


class PaymentOut(BaseModel):
    id: str
    amount: float
    currency: str
    status: str
    provider: str
    created_at: datetime
    game: Optional[GameOut] = None

    class Config:
        from_attributes = True


class PaymentIntentIn(BaseModel):
    items: List[str]
    currency: Optional[str] = "usd"


class LicenseIssueIn(BaseModel):
    game_id: str
    hardware_id: Optional[str] = None
    expires_at: Optional[datetime] = None
    max_activations: int = 1


class LicenseOut(BaseModel):
    id: str
    user_id: str
    game_id: str
    license_key: str
    hardware_id: Optional[str]
    issued_at: datetime
    expires_at: Optional[datetime]
    max_activations: int
    current_activations: int
    status: str
    signature: Optional[str] = None

    class Config:
        from_attributes = True


class SignedLicense(BaseModel):
    license_id: str
    user_id: str
    game_id: str
    issued_at: str
    expires_at: Optional[str] = None
    max_activations: int
    current_activations: int
    hardware_id: Optional[str] = None
    signature: str


class FriendRequestIn(BaseModel):
    target_username: str


class FriendshipOut(BaseModel):
    id: str
    user_id: str
    friend_id: str
    status: str
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class AchievementOut(BaseModel):
    id: str
    game_id: str
    key: str
    title: str
    description: Optional[str]
    points: int
    icon_url: Optional[str]

    class Config:
        from_attributes = True


class UserAchievementOut(BaseModel):
    id: str
    achievement: AchievementOut
    unlocked_at: datetime

    class Config:
        from_attributes = True


class AchievementUnlockIn(BaseModel):
    game_id: str
    achievement_key: str


class CloudSaveIn(BaseModel):
    game_id: str
    payload: dict = Field(default_factory=dict)
    version: Optional[str] = None


class CloudSaveOut(BaseModel):
    id: str
    user_id: str
    game_id: str
    payload: dict
    version: str
    updated_at: datetime

    class Config:
        from_attributes = True


class ChatMessageIn(BaseModel):
    recipient_id: str
    body: str


class ChatMessageOut(BaseModel):
    id: str
    sender_id: str
    recipient_id: str
    body: str
    created_at: datetime

    class Config:
        from_attributes = True


class WorkshopItemCreate(BaseModel):
    game_id: str
    title: str
    description: Optional[str] = None
    item_type: Optional[str] = None
    visibility: Optional[str] = "public"
    tags: List[str] = Field(default_factory=list)
    preview_image_url: Optional[str] = None


class WorkshopItemOut(BaseModel):
    id: str
    game_id: str
    creator_id: str
    title: str
    description: Optional[str]
    item_type: Optional[str]
    visibility: str
    total_downloads: int
    total_subscriptions: int
    rating_up: int
    rating_down: int
    tags: List[str]
    preview_image_url: Optional[str]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class WorkshopVersionOut(BaseModel):
    id: str
    workshop_item_id: str
    version: str
    changelog: Optional[str]
    file_size: int
    download_url: Optional[str]
    created_at: datetime

    class Config:
        from_attributes = True


class WorkshopSubscriptionOut(BaseModel):
    id: str
    workshop_item_id: str
    subscribed_at: datetime
    auto_update: bool
    item: Optional[WorkshopItemOut] = None

    class Config:
        from_attributes = True


class WorkshopRatingIn(BaseModel):
    rating: bool = True


class WishlistEntryOut(BaseModel):
    id: str
    created_at: datetime
    game: GameOut

    class Config:
        from_attributes = True


class BundleOut(BaseModel):
    id: str
    slug: str
    title: str
    description: Optional[str]
    price: float
    discount_percent: int
    game_ids: List[str]
    created_at: datetime

    class Config:
        from_attributes = True


class DlcOut(BaseModel):
    id: str
    base_game_id: str
    title: str
    description: Optional[str]
    price: float
    is_season_pass: bool
    release_date: Optional[str]
    created_at: datetime

    class Config:
        from_attributes = True


class PreorderOut(BaseModel):
    id: str
    status: str
    preorder_at: datetime
    preload_available: bool
    game: GameOut

    class Config:
        from_attributes = True


class UserProfileOut(BaseModel):
    user_id: str
    headline: Optional[str]
    bio: Optional[str]
    location: Optional[str]
    social_links: dict
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class UserProfileUpdate(BaseModel):
    headline: Optional[str] = None
    bio: Optional[str] = None
    location: Optional[str] = None
    social_links: Optional[dict] = None


class ReviewIn(BaseModel):
    rating: int = Field(ge=0, le=5)
    title: Optional[str] = None
    body: Optional[str] = None
    recommended: bool = True


class ReviewOut(BaseModel):
    id: str
    user: UserPublicOut
    game_id: str
    rating: int
    title: Optional[str]
    body: Optional[str]
    recommended: bool
    helpful_count: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class ScreenshotOut(BaseModel):
    id: str
    user_id: str
    game_id: str
    image_url: str
    caption: Optional[str]
    created_at: datetime

    class Config:
        from_attributes = True


class ActivityEventOut(BaseModel):
    id: str
    user_id: str
    event_type: str
    payload: dict
    created_at: datetime

    class Config:
        from_attributes = True


class InventoryItemOut(BaseModel):
    id: str
    user_id: str
    game_id: Optional[str]
    item_type: str
    name: str
    rarity: str
    quantity: int
    item_metadata: dict = Field(validation_alias="metadata", serialization_alias="metadata")
    created_at: datetime

    class Config:
        from_attributes = True
        populate_by_name = True


class InventoryGrantIn(BaseModel):
    game_id: Optional[str] = None
    item_type: str
    name: str
    rarity: Optional[str] = "common"
    quantity: int = 1
    item_metadata: dict = Field(
        default_factory=dict,
        validation_alias="metadata",
        serialization_alias="metadata",
    )

    class Config:
        populate_by_name = True


class TradeOfferIn(BaseModel):
    to_user_id: str
    offered_item_ids: List[str]
    requested_item_ids: List[str]


class TradeOfferOut(BaseModel):
    id: str
    from_user_id: str
    to_user_id: str
    offered_item_ids: List[str]
    requested_item_ids: List[str]
    status: str
    created_at: datetime
    expires_at: Optional[datetime]

    class Config:
        from_attributes = True


class TradingCardOut(BaseModel):
    id: str
    game_id: str
    card_name: str
    series: Optional[str]
    rarity: str

    class Config:
        from_attributes = True


class BadgeOut(BaseModel):
    id: str
    game_id: str
    badge_name: str
    level: int
    required_cards: List[str]
    xp_reward: int

    class Config:
        from_attributes = True


class DeveloperDepotOut(BaseModel):
    id: str
    game_id: str
    name: str
    platform: str
    branch: str
    created_at: datetime

    class Config:
        from_attributes = True


class DeveloperBuildOut(BaseModel):
    id: str
    depot_id: str
    version: str
    manifest_json: dict
    created_at: datetime

    class Config:
        from_attributes = True


class DeveloperAnalyticsOut(BaseModel):
    game_id: str
    metrics: dict
    created_at: datetime

    class Config:
        from_attributes = True


class RemoteDownloadIn(BaseModel):
    game_id: str
    target_device: str


class RemoteDownloadOut(BaseModel):
    id: str
    game: GameOut
    target_device: str
    status: str
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class StreamingSessionOut(BaseModel):
    id: str
    user_id: str
    game_id: Optional[str]
    status: str
    offer: dict
    answer: dict
    ice_candidates: List[dict]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
