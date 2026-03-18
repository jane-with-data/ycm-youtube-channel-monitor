"""Application constants and configuration values.

This module defines all constants used throughout the application,
including data type configurations, styling, and Excel format settings.

Constants are defined with type hints and Final annotations to prevent
accidental modifications.
"""
from pathlib import Path
from typing import Final, Dict, List, Any, Set

# =============================================================================
# SYSTEM & PROJECT INFORMATION
DEFAULT_ENCODING: Final[str] = "utf-8"
MAX_WIDTH: Final[int] = 60

# =============================================================================
# Paths
PATH = {
    "PROJECT_ROOT": Path(__file__).parent.parent.parent,
    "DATA_DIR": Path(__file__).parent.parent.parent / "data",
    "DIR_DATA_BRONZE": Path(__file__).parent.parent.parent / "data" / "bronze",
    "DIR_DATA_SILVER": Path(__file__).parent.parent.parent / "data" / "silver",
    "DIR_DATA_GOLD": Path(__file__).parent.parent.parent / "data" / "gold",
    "LOGS_DIR": Path(__file__).parent.parent.parent / "data" / "logs",
    "TEMP_DIR": Path(__file__).parent.parent.parent / "data" / "temp"
}
# LOGGING =============================================================================
LOG: Final[Dict[str, Any]] = {
    "SET_LEVEL_FILE": "INFO",
    "SET_LEVEL_CONSOLE": "INFO",
    "NAME": "app.log",
    "LEVEL": "INFO",
    "WHEN": "midnight",
    "INTERVAL": 1,
    "BACKUP_COUNT": 30
}