"""
Configuration management for YouTube API client.
Load settings from environment variables or .env file.
"""

import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass
class YouTubeConfig:
    api_key: str = field(default_factory=lambda: os.getenv("YOUTUBE_API_KEY", ""))
    api_service_name: str = "youtube"
    api_version: str = "v3"
    max_results: int = 50
    quota_limit_per_day: int = 10_000  # default free tier

    def validate(self) -> None:
        if not self.api_key:
            raise ValueError(
                "YOUTUBE_API_KEY is not set. "
                "Please set it in your environment or .env file."
            )
