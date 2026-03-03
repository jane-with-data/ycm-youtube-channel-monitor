"""Unit tests for data models."""

import pytest
from datetime import datetime
from src.models.youtube import VideoData, ChannelData, AnalyticsData
from tests.fixtures.sample_data import (
    SAMPLE_VIDEOS,
    SAMPLE_CHANNEL,
    SAMPLE_ANALYTICS
)


class TestVideoData:
    """Test VideoData model validation."""
    
    def test_valid_video_data(self):
        """Test creating valid VideoData."""
        video = VideoData(**SAMPLE_VIDEOS[0])
        assert video.video_id == "dQw4w9WgXcQ"
        assert video.view_count == 1000000
    
    def test_negative_view_count_raises_error(self):
        """Test that negative view count raises validation error."""
        invalid_video = SAMPLE_VIDEOS[0].copy()
        invalid_video["view_count"] = -1
        
        with pytest.raises(ValueError):
            VideoData(**invalid_video)
    
    def test_zero_duration_raises_error(self):
        """Test that zero duration raises validation error."""
        invalid_video = SAMPLE_VIDEOS[0].copy()
        invalid_video["duration_seconds"] = 0
        
        with pytest.raises(ValueError):
            VideoData(**invalid_video)
    
    def test_negative_like_count_raises_error(self):
        """Test that negative like count raises validation error."""
        invalid_video = SAMPLE_VIDEOS[0].copy()
        invalid_video["like_count"] = -100
        
        with pytest.raises(ValueError):
            VideoData(**invalid_video)


class TestChannelData:
    """Test ChannelData model validation."""
    
    def test_valid_channel_data(self):
        """Test creating valid ChannelData."""
        channel = ChannelData(**SAMPLE_CHANNEL)
        assert channel.channel_id == "UCkRfArvrzheW1E7UOD-PHFA"
        assert channel.subscriber_count == 100000
    
    def test_negative_subscriber_count_raises_error(self):
        """Test that negative subscriber count raises validation error."""
        invalid_channel = SAMPLE_CHANNEL.copy()
        invalid_channel["subscriber_count"] = -1
        
        with pytest.raises(ValueError):
            ChannelData(**invalid_channel)


class TestAnalyticsData:
    """Test AnalyticsData model validation."""
    
    def test_valid_analytics_data(self):
        """Test creating valid AnalyticsData."""
        analytics = AnalyticsData(**SAMPLE_ANALYTICS)
        assert analytics.channel_id == "UCkRfArvrzheW1E7UOD-PHFA"
        assert analytics.total_views == 1500000
    
    def test_negative_total_views_raises_error(self):
        """Test that negative total views raises validation error."""
        invalid_analytics = SAMPLE_ANALYTICS.copy()
        invalid_analytics["total_views"] = -1000
        
        with pytest.raises(ValueError):
            AnalyticsData(**invalid_analytics)
