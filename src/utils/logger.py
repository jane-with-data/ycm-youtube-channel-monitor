import logging
import sys
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler
from typing import Optional

# Load log config
try:
    from config.constants import LOG, DEFAULT_ENCODING
except ImportError:
    LOG = {"NAME": "app.log", "WHEN": "D", "INTERVAL": 1, "BACKUP_COUNT": 30, "SET_LEVEL_FILE": "INFO"}
    DEFAULT_ENCODING = "utf-8"

class Logger:
    _instance: Optional["Logger"] = None
    
    def __new__(cls) -> "Logger":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_logger()
        return cls._instance
    
    def _init_logger(self) -> None:
        log_dir = Path("data/logs")
        log_dir.mkdir(parents=True, exist_ok=True)
                        
        # Init root logger (root hoặc đặt tên riêng)
        # Sử dụng name='app' thay vì __name__ để thống nhất log toàn hệ thống
        self._logger = logging.getLogger("AppLogger")
        self._logger.setLevel(logging.DEBUG) 
        self._logger.propagate = False
        self._logger.handlers.clear()
        
        # --- 1. File Handler ---
        log_file = log_dir / LOG.get("NAME", "app.log")
        file_handler = TimedRotatingFileHandler(
            filename=str(log_file),
            when=LOG.get("WHEN", "D"),
            interval=LOG.get("INTERVAL", 1),
            backupCount=LOG.get("BACKUP_COUNT", 30),
            encoding=DEFAULT_ENCODING
        )
        
        # Get file config
        file_level = getattr(logging, str(LOG.get("SET_LEVEL_FILE", "DEBUG")).upper(), logging.DEBUG)
        file_handler.setLevel(file_level)
        
        # Format log content
        file_formatter = logging.Formatter(
            "[%(asctime)s][%(levelname)s][%(module)s.%(funcName)s:%(lineno)d] %(message)s"
        )
        file_handler.setFormatter(file_formatter)
        self._logger.addHandler(file_handler)
        
        # --- 2. Config Console Handler ---
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            "[%(asctime)s][%(levelname)s][%(module)s.%(funcName)s:%(lineno)d] %(message)s",
            datefmt="%H:%M:%S"
        )
        console_handler.setFormatter(console_formatter)
        self._logger.addHandler(console_handler)

    # Wrappers với stacklevel=2 để bắt đúng nơi gọi log
    def debug(self, msg: str): self._logger.debug(msg, stacklevel=2)
    def info(self, msg: str): self._logger.info(msg, stacklevel=2)
    def warning(self, msg: str): self._logger.warning(msg, stacklevel=2)
    def error(self, msg: str, exc_info: bool = False): 
        self._logger.error(msg, exc_info=exc_info, stacklevel=2)
    def critical(self, msg: str, exc_info: bool = False): 
        self._logger.critical(msg, exc_info=exc_info, stacklevel=2)

def get_logger() -> Logger:
    return Logger()