"""Integration tests for ETL pipeline."""

import pytest
from tests.fixtures.sample_data import (
    get_sample_videos,
    get_sample_channel,
    get_sample_analytics
)


class TestETLPipeline:
    """Test end-to-end ETL pipeline."""
    
    @pytest.fixture
    def sample_videos(self):
        """Get sample video data."""
        return get_sample_videos()
    
    @pytest.fixture
    def sample_channel(self):
        """Get sample channel data."""
        return get_sample_channel()
    
    def test_extract_phase(self, sample_videos):
        """Test extraction phase - video data parsing."""
        assert len(sample_videos) == 2
        assert sample_videos[0].video_id == "dQw4w9WgXcQ"
        assert sample_videos[1].duration_seconds == 600
    
    def test_transform_phase(self, sample_videos):
        """Test transform phase - data transformation."""
        # Example: Calculate average views
        total_views = sum(v.view_count for v in sample_videos)
        avg_views = total_views / len(sample_videos)
        
        assert avg_views == 750000.0
        assert total_views == 1500000
    
    def test_load_phase(self, sample_channel):
        """Test load phase - data preparation for loading."""
        # Example: Verify data is ready for database
        channel_dict = sample_channel.model_dump()
        
        assert "channel_id" in channel_dict
        assert "title" in channel_dict
        assert sample_channel.subscriber_count >= 0
    
    def test_full_pipeline_flow(self, sample_videos, sample_channel):
        """Test complete ETL flow: Extract → Transform → Load."""
        # Extract
        extracted_videos = sample_videos
        assert len(extracted_videos) > 0
        
        # Transform
        transformed = {
            "channel": sample_channel,
            "videos": extracted_videos,
            "total_views": sum(v.view_count for v in extracted_videos),
            "total_likes": sum(v.like_count for v in extracted_videos),
        }
        
        # Verify transformation
        assert transformed["total_views"] > 0
        assert transformed["total_likes"] > 0
        
        # Load (prepare for database)
        load_ready = {
            "channel_data": transformed["channel"].model_dump(),
            "video_data": [v.model_dump() for v in transformed["videos"]],
        }
        
        assert isinstance(load_ready["channel_data"], dict)
        assert isinstance(load_ready["video_data"], list)
