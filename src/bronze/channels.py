import pandas as pd
import pyarrow
import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from config.constants import PATH
pd.set_option("display.max_rows", None, "display.max_columns", None)
pd.set_option("display.max_colwidth", None)

# Flow
# read raw data -> process metadata -> process payload -> generate additional columns -> merge -> rename columns -> just keep necessary columns -> write to parquet

class Channels:
    def __init__(self):
        
        # Gen File Path
        self.dir_data_raw = PATH['DIR_DATA_RAW']
        self.dir_data_bronze = PATH['DIR_DATA_BRONZE']
        
        year = datetime.now(timezone.utc).strftime('%Y')
        month = datetime.now(timezone.utc).strftime('%m')
        day = datetime.now(timezone.utc).strftime('%d')
        hour = datetime.now(timezone.utc).strftime('%H')
        
        file_name_raw = f"channels.jsonl"
        self.file_path_raw = self.dir_data_raw / "youtube" / "channels" / f"year={year}" / f"month={month}" / f"day={day}" / f"hour={hour}" / file_name_raw
        
        file_name_bronze = f"channels.parquet"
        self.file_path_bronze = self.dir_data_bronze / "youtube" / "channels" / f"year={year}" / f"month={month}" / f"day={day}" / f"hour={hour}" / file_name_bronze
    
    def _gen_checksum(self, data: dict) -> str:
        """
        Generate a checksum using SHA256
        """
        payload_str = json.dumps(data, sort_keys=True, ensure_ascii=True)
        return hashlib.sha256(payload_str.encode()).hexdigest()
    
    def _read_raw_layer(self, _file_path_raw = None) -> pd.DataFrame:
        if _file_path_raw is None:
            _file_path_raw = self.file_path_raw
            
        data = pd.read_json(_file_path_raw, lines=True)
        return data
    
    def _transform(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        len_df_raw = len(df_raw)
        
        # Process metadata: Flatten metadata column
        df_metadata = pd.json_normalize(df_raw['metadata'])
        
        # Process payload: Flatten payload column -> items[0]
        items_dict = df_raw['payload'].apply(lambda row: row.get("items", [{}])[0])
        df_items = pd.json_normalize(items_dict)
        
        # Generate system columns
        df_add_cols = pd.DataFrame({
            '_bronze_id': [str(uuid.uuid4()) for _ in range(len_df_raw)],
            '_ingested_at': [datetime.now(timezone.utc).isoformat() for _ in range(len_df_raw)],
            '_checksum': [self._gen_checksum(item) for item in items_dict],
            '_modified_at': [datetime.now(timezone.utc).isoformat() for _ in range(len_df_raw)],
            '_is_deleted': [False for _ in range(len_df_raw)],
        })
        
        # Merge metadata, items, and additional columns
        df_merged = pd.concat([df_metadata, df_items, df_add_cols], axis=1)
        
        # Rename columns
        df_merged = df_merged.rename(columns={
            'id': 'channel_id',
            'snippet.title': 'title',
            'snippet.description': 'description',
            'snippet.customUrl': 'custom_url',
            'snippet.publishedAt': 'published_at',
            'snippet.thumbnails.default.url': 'thumbnails_default_url',
            'snippet.thumbnails.default.width': 'thumbnails_default_width',
            'snippet.thumbnails.default.height': 'thumbnails_default_height',
            'snippet.thumbnails.medium.url': 'thumbnails_medium_url',
            'snippet.thumbnails.medium.width': 'thumbnails_medium_width',
            'snippet.thumbnails.medium.height': 'thumbnails_medium_height',
            'snippet.thumbnails.high.url': 'thumbnails_high_url',
            'snippet.thumbnails.high.width': 'thumbnails_high_width',
            'snippet.thumbnails.high.height': 'thumbnails_high_height',
            'contentDetails.relatedPlaylists.uploads': 'playlist_id_upload',
            'statistics.viewCount': 'view_count',
            'statistics.subscriberCount': 'subscriber_count',
            'statistics.videoCount': 'video_count',
        })
        
        # Just keep neccesary columns
        df_merged_keep_cols = [
            '_bronze_id',
            '_source_system',
            '_source_file',
            '_landed_at',
            '_ingested_at',
            '_checksum',
            '_modified_at',
            '_is_deleted',
            'channel_id',
            'playlist_id_upload',
            'title',
            'description',
            'custom_url',
            'published_at',
            'thumbnails_default_url',
            'thumbnails_default_width',
            'thumbnails_default_height',
            'thumbnails_medium_url',
            'thumbnails_medium_width',
            'thumbnails_medium_height',
            'thumbnails_high_url',
            'thumbnails_high_width',
            'thumbnails_high_height',
            'view_count',
            'subscriber_count',
            'video_count'
        ]
        df_merged = df_merged[df_merged_keep_cols]
        return df_merged
                
    def _save_to_parquet(self, df_save: pd.DataFrame, _file_path_bronze = None) -> None:
        if _file_path_bronze is None:
            _file_path_bronze = self.file_path_bronze
        
        _file_path_bronze.parent.mkdir(parents=True, exist_ok=True)
        df_save.to_parquet(_file_path_bronze, index=False)
    
    def run_pipeline(self) -> None:
        df_raw = self._read_raw_layer()
        df_transformed = self._transform(df_raw)
        self._save_to_parquet(df_transformed)
        
if __name__ == "__main__":
    channel = Channels()
    channel.run_pipeline()