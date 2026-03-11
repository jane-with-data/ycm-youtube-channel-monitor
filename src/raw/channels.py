import json
import requests
from datetime import datetime
from config.settings import YOUTUBE_API_V3, YOUTUBE_API_KEY
from config.constants import PATH
from pathlib import Path

class Channels:
    def __init__(self):
        self.YOUTUBE_API_V3 = YOUTUBE_API_V3
        self.YOUTUBE_API_KEY = YOUTUBE_API_KEY
        self.dir_data_raw = PATH['DIR_DATA_RAW']
    def _fetch_channel_data_by_channel_id(self, channel_id: str) -> dict:
        """Lấy dữ liệu kênh YouTube dựa trên channel_id."""
        
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
            "_landed_at": datetime.now().isoformat(),
            "_source_system": "API",
            "_source_file": "youtube_api_v3",    
        }
        return metadata        

    def get_channel_data_by_channel_id_pipeline(self, channel_id: str) -> None :
        metadata = self._gen_metadata()
        channel_data = self._fetch_channel_data_by_channel_id(channel_id)
        
        insert_data = {
            "metadata": metadata,
            "payload": channel_data               
        }
        self._save_to_json_file(insert_data)
        
    def _save_to_json_file(self, data: dict) -> None:
        
        
        year = datetime.now().strftime('%Y')
        month = datetime.now().strftime('%m')
        day = datetime.now().strftime('%d')
        hour = datetime.now().strftime('%H')
        
        file_name = f"channels.jsonl"
        
        file_path_not_include_file_name = self.dir_data_raw / "youtube" / "channels" / f"year={year}" / f"month={month}" / f"day={day}" / f"hour={hour}" 
        file_path_not_include_file_name.mkdir(parents=True, exist_ok=True)
        
        file_path = file_path_not_include_file_name / file_name

        with open(file_path, 'a') as f:
            json.dump(data, f, ensure_ascii=True, indent=4)
        
if __name__ == "__main__":
    channel = Channels()
    data = channel.get_channel_data_by_channel_id_pipeline("UCM9KgI3IytaTL9hr0vhXxuQ")
    # print(data)
