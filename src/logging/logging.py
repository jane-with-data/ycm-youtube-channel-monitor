import logging
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler
from typing import Optional
from config.constants import LOG, DEFAULT_ENCODING

class Logger:
    """
    Singleton logger with structured logging.
    Tự động bắt thông tin class/function thông qua Formatters.
    """
    
    _instance: Optional["Logger"] = None
    _logger: logging.Logger
    
    def __new__(cls) -> "Logger":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_logger()
        return cls._instance
    
    def _init_logger(self) -> None:
        """Khởi tạo logger với trình quản lý xoay vòng file và hiển thị console."""
        
        # 1. Tạo thư mục log
        log_dir = Path("data/logs")
        log_dir.mkdir(parents=True, exist_ok=True)
                        
        # 2. Tạo Logger chính
        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(logging.DEBUG) # Level thấp nhất để cho phép mọi loại log đi qua
        self._logger.propagate = False
        self._logger.handlers.clear()
        
        # 3. File Handler (Lưu chi tiết để trace lỗi)
        # Placeholder giải thích:
        # %(module)s: Tên file gọi log
        # %(funcName)s: Tên hàm gọi log
        # %(lineno)d: Dòng code gọi log
        log_file = log_dir / LOG.get("NAME", "app.log")
        file_handler = TimedRotatingFileHandler(
            filename=log_file,
            when=LOG.get("WHEN"),                    # Xoay vòng theo ngày
            interval=LOG.get("INTERVAL"),          # Mỗi 1 ngày
            backupCount=LOG.get("BACKUP_COUNT"),     # Giữ lại 30 ngày gần nhất
            encoding=DEFAULT_ENCODING
        )
        file_handler.setLevel(getattr(logging, f"{LOG.get("SET_LEVEL_FILE")}"))
        file_formatter = logging.Formatter(
            "[%(asctime)s][%(levelname)s][%(module)s.%(funcName)s:%(lineno)d] %(message)s"
        )
        file_handler.setFormatter(file_formatter)
        self._logger.addHandler(file_handler)
        
        # 4. Console Handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO) # Console thường chỉ hiện INFO trở lên
        console_formatter = logging.Formatter(
            "[%(asctime)s] %(levelname)s: %(message)s",
            datefmt="%H:%M:%S"
        )
        console_handler.setFormatter(console_formatter)
        self._logger.addHandler(console_handler)
    
    # --- Các phương thức bọc (Wrappers) ---
    def debug(self, message: str) -> None:
        self._logger.debug(message, stacklevel=2)
    
    def info(self, message: str) -> None:
        self._logger.info(message, stacklevel=2)
    
    def warning(self, message: str) -> None:
        self._logger.warning(message, stacklevel=2)
    
    def error(self, message: str, exc_info: bool = False) -> None:
        self._logger.error(message, exc_info=exc_info, stacklevel=2)
    
    def critical(self, message: str, exc_info: bool = False) -> None:
        self._logger.critical(message, exc_info=exc_info, stacklevel=2)

def get_logger() -> Logger:
    """Hàm tiện ích để lấy instance của Logger."""
    return Logger()

# --- Cách sử dụng ---
if __name__ == "__main__":
    logger = get_logger()
    logger.info("Chương trình bắt đầu chạy")
    
    def test_func():
        logger.debug("Đây là log trong một hàm test")
    
    test_func()
    
    try:
        1 / 0
    except Exception as e:
        logger.error(f"Có lỗi xảy ra: {e}", exc_info=True)