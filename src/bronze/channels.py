import json
import pandas
import requests
import uuid
from datetime import datetime, timezone
from config.settings import YOUTUBE_API_V3, YOUTUBE_API_KEY
from config.constants import PATH
from pathlib import Path
from utils.logger import get_logger

# TODO:
class Channels:
    def __init__(self):
        self.YOUTUBE_API_V3 = YOUTUBE_API_V3
        self.YOUTUBE_API_KEY = YOUTUBE_API_KEY
        self.dir_data_bronze = PATH['DIR_DATA_BRONZE']
        self.logger = get_logger()
        
    def _fetch_channel_data_by_channel_id(self, channel_id: str) -> dict:
        """Get data by channel id using Youtube API V3"""
        
        BASE_URL = f"{self.YOUTUBE_API_V3}/channels/" 
        params = {
            "part": "snippet,contentDetails,statistics",
            "id": channel_id,
            "maxResults": 50,
            "key": self.YOUTUBE_API_KEY
        }
        
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        return response.json()

    def _gen_metadata(self) -> dict:    
        metadata = {
            "_bronze_id": str(uuid.uuid4()),
            "_landed_at": datetime.now(timezone.utc).isoformat(),
            "_source_system": "API",
            "_source_file": "youtube_api_v3",    
        }
        return metadata        
        
    def _save_to_json_file(self, data: dict) -> None:
        
        year = datetime.now(timezone.utc).strftime('%Y')
        month = datetime.now(timezone.utc).strftime('%m')
        day = datetime.now(timezone.utc).strftime('%d')
        hour = datetime.now(timezone.utc).strftime('%H')

        file_name = f"channels.jsonl"
        file_path_not_include_file_name = self.dir_data_bronze / "youtube" / "channels" / f"year={year}" / f"month={month}" / f"day={day}" / f"hour={hour}" 
        file_path_not_include_file_name.mkdir(parents=True, exist_ok=True)
        file_path = file_path_not_include_file_name / file_name

        # if rerun pipeline within the same hour - overwrite file
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(json.dumps(data, ensure_ascii=True) + "\n")
        self.logger.info(f"Save json successful at {file_path}")
        
    def run_pipeline(self, channel_id: str) -> None:
        metadata = self._gen_metadata()
        channel_data = self._fetch_channel_data_by_channel_id(channel_id)
        
        insert_data = {
            "metadata": metadata,
            "payload": channel_data               
        }

        self._save_to_json_file(insert_data)
        self.logger.info(f"Pipeline run successful with channel_id [{channel_id}]")
        

if __name__ == "__main__":
    channel = Channels()
    data = channel.run_pipeline("UCM9KgI3IytaTL9hr0vhXxuQ")