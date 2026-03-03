"""Sample data for testing."""

from datetime import datetime
from src.models.youtube import VideoData, ChannelData, AnalyticsData


# Sample channel data
SAMPLE_CHANNEL = {
    "channel_id": "UCkRfArvrzheW1E7UOD-PHFA",
    "title": "Sample YouTube Channel",
    "description": "A sample channel for testing",
    "subscriber_count": 100000,
    "view_count": 5000000,
    "video_count": 250,
    "created_at": datetime(2015, 3, 15),
    "thumbnail_url": "https://example.com/image.jpg"
}

# Sample video data
SAMPLE_VIDEOS = [
    {
        "video_id": "dQw4w9WgXcQ",
        "title": "Sample Video 1",
        "description": "This is a sample video",
        "channel_id": "UCkRfArvrzheW1E7UOD-PHFA",
        "channel_title": "Sample YouTube Channel",
        "published_at": datetime(2024, 1, 1),
        "duration_seconds": 300,
        "view_count": 1000000,
        "like_count": 50000,
        "comment_count": 5000,
        "thumbnail_url": "https://example.com/video1.jpg"
    },
    {
        "video_id": "9bZkp7q19f0",
        "title": "Sample Video 2",
        "description": "Another sample video",
        "channel_id": "UCkRfArvrzheW1E7UOD-PHFA",
        "channel_title": "Sample YouTube Channel",
        "published_at": datetime(2024, 1, 2),
        "duration_seconds": 600,
        "view_count": 500000,
        "like_count": 25000,
        "comment_count": 2500,
        "thumbnail_url": "https://example.com/video2.jpg"
    }
]

# Sample analytics data
SAMPLE_ANALYTICS = {
    "channel_id": "UCkRfArvrzheW1E7UOD-PHFA",
    "date": "2024-01-15",
    "total_views": 1500000,
    "total_likes": 75000,
    "total_comments": 7500,
    "video_count": 2,
    "avg_view_per_video": 750000.0
}


def get_sample_video() -> VideoData:
    """Get a sample video data object."""
    return VideoData(**SAMPLE_VIDEOS[0])


def get_sample_videos() -> list[VideoData]:
    """Get a list of sample video data objects."""
    return [VideoData(**video) for video in SAMPLE_VIDEOS]


def get_sample_channel() -> ChannelData:
    """Get a sample channel data object."""
    return ChannelData(**SAMPLE_CHANNEL)


def get_sample_analytics() -> AnalyticsData:
    """Get a sample analytics data object."""
    return AnalyticsData(**SAMPLE_ANALYTICS)
