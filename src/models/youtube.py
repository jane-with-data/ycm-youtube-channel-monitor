"""Pydantic models for YouTube API data."""

from pydantic import BaseModel, field_validator
from datetime import datetime
from typing import Optional


class VideoData(BaseModel):
    """YouTube Video data model."""
    
    video_id: str
    title: str
    description: Optional[str] = None
    channel_id: str
    channel_title: str
    published_at: datetime
    duration_seconds: int
    view_count: int
    like_count: int
    comment_count: int
    thumbnail_url: Optional[str] = None
    
    class Config:
        """Pydantic config."""
        json_schema_extra = {
            "example": {
                "video_id": "dQw4w9WgXcQ",
                "title": "Sample Video",
                "channel_id": "UCxxxx",
                "channel_title": "Sample Channel",
                "published_at": "2024-01-01T00:00:00Z",
                "duration_seconds": 300,
                "view_count": 1000000,
                "like_count": 50000,
                "comment_count": 5000,
            }
        }
    
    @field_validator('view_count', 'like_count', 'comment_count')
    @classmethod
    def non_negative(cls, v):
        """Ensure counts are non-negative."""
        if v < 0:
            raise ValueError('Count must be >= 0')
        return v
    
    @field_validator('duration_seconds')
    @classmethod
    def positive_duration(cls, v):
        """Ensure duration is positive."""
        if v <= 0:
            raise ValueError('Duration must be > 0')
        return v


class ChannelData(BaseModel):
    """YouTube Channel data model."""
    
    channel_id: str
    title: str
    description: Optional[str] = None
    subscriber_count: int
    view_count: int
    video_count: int
    created_at: datetime
    thumbnail_url: Optional[str] = None
    
    @field_validator('subscriber_count', 'view_count', 'video_count')
    @classmethod
    def non_negative(cls, v):
        """Ensure counts are non-negative."""
        if v < 0:
            raise ValueError('Count must be >= 0')
        return v


class AnalyticsData(BaseModel):
    """Analytics data model for aggregations."""
    
    channel_id: str
    date: str  # YYYY-MM-DD format
    total_views: int
    total_likes: int
    total_comments: int
    video_count: int
    avg_view_per_video: float
    
    @field_validator('total_views', 'total_likes', 'total_comments', 'video_count')
    @classmethod
    def non_negative(cls, v):
        """Ensure counts are non-negative."""
        if v < 0:
            raise ValueError('Count must be >= 0')
        return v
