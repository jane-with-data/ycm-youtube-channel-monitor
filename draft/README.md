# YOUTBE CHANNEL MONITOR - YCM
## 1. Tổng quan dự án
**YCM - Youtube Channel Monitor** là project thu thập daily thông tin kênh Youtube, playlist thuộc kênh, video thuộc kênh, bình luận thuộc video để lưu trữ, theo dõi, phân tích.

## 2. Cấu trúc thư mục
```youtube_local_project/
├── data/
│   ├── raw/                # Lưu file JSON lấy từ API về (Staging)
│   └── youtube_data.db     # File SQLite đóng vai trò Warehouse
├── src/
│   ├── __init__.py
│   ├── extractor.py        # Code gọi YouTube API
│   ├── transformer.py      # Code làm sạch dữ liệu bằng Pandas
│   └── loader.py           # Code đẩy dữ liệu vào SQLite
├── .env                    # Lưu API_KEY (không đẩy lên Github)
├── main.py                 # File chạy chính (Orchestrator)
└── requirements.txt
```