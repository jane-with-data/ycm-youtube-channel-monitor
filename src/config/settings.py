import os
from dotenv import load_dotenv

load_dotenv()

# --- YouTube API ---
YOUTUBE_API_V3 = os.getenv("YOUTUBE_API_V3")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

# --- PostgreSQL database ---
POSTGRES_URL = os.getenv("POSTGRES_URL")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Quick check if essential configs are loaded
if not YOUTUBE_API_KEY:
    raise EnvironmentError("YOUTUBE_API_KEY not found in .env file.")