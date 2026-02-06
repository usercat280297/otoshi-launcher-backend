"""
Distribute Routes - Developer distribution platform API endpoints
"""

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

from ..routes.deps import get_current_user, require_admin_user
from ..db import SessionLocal

router = APIRouter()


class DistributeStats(BaseModel):
    total_games: int
    total_downloads: int
    total_revenue: float
    pending_payouts: float
    active_users: int


class GameSubmission(BaseModel):
    title: str
    description: str
    genres: List[str]
    price: float
    platforms: List[str] = ["windows"]
    release_date: Optional[str] = None


class GameSubmissionResponse(BaseModel):
    id: str
    title: str
    status: str
    submitted_at: str
    message: str


class SDKDownload(BaseModel):
    name: str
    version: str
    platform: str
    download_url: str
    size_mb: float
    checksum: str


# Available SDK downloads
SDK_DOWNLOADS: List[SDKDownload] = [
    SDKDownload(
        name="OTOSHI SDK for Windows",
        version="2.1.0",
        platform="windows",
        download_url="https://sdk.otoshi-launcher.me/releases/otoshi-sdk-2.1.0-win64.zip",
        size_mb=45.2,
        checksum="sha256:abc123def456..."
    ),
    SDKDownload(
        name="OTOSHI SDK for Linux",
        version="2.1.0",
        platform="linux",
        download_url="https://sdk.otoshi-launcher.me/releases/otoshi-sdk-2.1.0-linux.tar.gz",
        size_mb=42.8,
        checksum="sha256:789xyz012..."
    ),
    SDKDownload(
        name="OTOSHI SDK for macOS",
        version="2.1.0",
        platform="macos",
        download_url="https://sdk.otoshi-launcher.me/releases/otoshi-sdk-2.1.0-macos.pkg",
        size_mb=48.1,
        checksum="sha256:456abc789..."
    ),
    SDKDownload(
        name="OTOSHI CLI Tool",
        version="1.5.0",
        platform="cross-platform",
        download_url="https://sdk.otoshi-launcher.me/releases/otoshi-cli-1.5.0.zip",
        size_mb=12.3,
        checksum="sha256:cli123abc..."
    ),
]


@router.get("/stats", response_model=DistributeStats)
async def get_distribute_stats(current_user: dict = Depends(require_admin_user)):
    """Get distribution statistics for the current developer"""
    # In a real implementation, this would query the database
    return DistributeStats(
        total_games=0,
        total_downloads=0,
        total_revenue=0.0,
        pending_payouts=0.0,
        active_users=0
    )


@router.post("/submit", response_model=GameSubmissionResponse)
async def submit_game(
    submission: GameSubmission,
    current_user: dict = Depends(require_admin_user)
):
    """Submit a new game for review"""
    # Validate submission
    if not submission.title or len(submission.title) < 3:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Game title must be at least 3 characters"
        )

    if submission.price < 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Price cannot be negative"
        )

    # In a real implementation, this would create a submission record
    submission_id = f"sub_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

    return GameSubmissionResponse(
        id=submission_id,
        title=submission.title,
        status="pending_review",
        submitted_at=datetime.utcnow().isoformat(),
        message="Your game has been submitted for review. We'll notify you within 2-5 business days."
    )


@router.get("/sdk", response_model=List[SDKDownload])
async def get_sdk_downloads():
    """Get available SDK downloads"""
    return SDK_DOWNLOADS


@router.get("/sdk/{platform}", response_model=SDKDownload)
async def get_sdk_for_platform(platform: str):
    """Get SDK download for a specific platform"""
    for sdk in SDK_DOWNLOADS:
        if sdk.platform == platform:
            return sdk

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"SDK not found for platform: {platform}"
    )


class RevenueReport(BaseModel):
    period: str
    gross_revenue: float
    platform_fee: float
    net_revenue: float
    downloads: int
    refunds: int


@router.get("/revenue", response_model=List[RevenueReport])
async def get_revenue_reports(
    current_user: dict = Depends(require_admin_user),
    months: int = 6
):
    """Get revenue reports for the last N months"""
    # In a real implementation, this would query revenue data
    return []


class AnalyticsData(BaseModel):
    date: str
    downloads: int
    active_users: int
    revenue: float
    refund_rate: float


@router.get("/analytics/{game_id}", response_model=List[AnalyticsData])
async def get_game_analytics(
    game_id: str,
    current_user: dict = Depends(require_admin_user),
    days: int = 30
):
    """Get analytics data for a specific game"""
    # In a real implementation, this would query analytics data
    return []
