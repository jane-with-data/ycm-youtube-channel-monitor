import pandas as pd
import json
from pathlib import Path
from datetime import datetime, timezone
import uuid
import hashlib
import pyarrow
from config.constants import PATH
from utils.logger import get_logger

pd.set_option("display.max_rows", None, "display.max_columns", None)
pd.set_option("display.max_colwidth", None)
# TODO
# 1. Thêm staging bronze -> stg -> silver
class Channels:
    def __init__(self):
        # Init logging
        self.logger = get_logger()
        
        # Gen File Path
        self.dir_data_bronze = PATH['DIR_DATA_BRONZE']
        self.dir_data_silver = PATH['DIR_DATA_SILVER']
        year = datetime.now(timezone.utc).strftime('%Y')
        month = datetime.now(timezone.utc).strftime('%m')
        day = datetime.now(timezone.utc).strftime('%d')
        hour = datetime.now(timezone.utc).strftime('%H')
        # snapshot = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:00:00')
        
        # Bronze layer
        file_name_bronze = f"channels.jsonl"
        self.file_path_bronze = self.dir_data_bronze / "youtube" / "channels" / f"year={year}" / f"month={month}" / f"day={day}" / f"hour={hour}" / file_name_bronze
        
        # Silver layer
            # Latest
        file_name_silver_latest = f"channels_latest.parquet"
        self.file_path_silver_latest = self.dir_data_silver / "youtube" / "channels" / f"latest" / file_name_silver_latest
        
            # Snapshot
        file_name_silver_snapshot_hourly = f"channels_snapshot_hourly.parquet"
        self.file_path_silver_snapshot = self.dir_data_silver / "youtube" / "channels" / f"snapshot" / f"year={year}" / f"month={month}" / f"day={day}" / f"hour={hour}" / file_name_silver_snapshot_hourly
        
        # rename config
        self.df_items_rename_lst = {
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
        }
        
        # cast type config
        self.datetime_cols_df_items = ["published_at"]
        self.string_cols_df_items = ["channel_id", "playlist_id_upload", "title", "custom_url", "description", "thumbnails_default_url", "thumbnails_medium_url", "thumbnails_high_url"]
        self.int64_cols_df_items = ['thumbnails_default_width', 'thumbnails_default_height', 'thumbnails_medium_width', 'thumbnails_medium_height', 'thumbnails_high_width', 'thumbnails_high_height', 'view_count', 'subscriber_count', 'video_count']
        
        self.datetime_cols_df_metadata = ["_landed_at"]
        self.string_cols_df_metadata = []
        self.int64_cols_df_metadata = []
        
        # keep_cols config
        # self.keep_cols_df_items = []
        # self.keep_cols_df_metadata = []
        # self.keep_cols = []
        
        
        # checksum
        self.checksum_field = [
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
        
        # keep cols
        self.keep_silver_latest_cols_lst = [
                '_latest_id',
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
                'video_count',
                '_ingested_at',
                '_updated_at',
                '_is_deleted',
                '_source_system',
                '_source_file',
                '_bronze_id',
                '_checksum'
            ]
    
    def _cast_type_df(self, df: pd.DataFrame, string_cols: list, int64_cols: list, datetime_cols: list) -> pd.DataFrame:
        
        dtype_map = (
            {col: "string" for col in string_cols}
            | {col: "Int64" for col in int64_cols}
        )
        
        # cast string + int
        df = df.astype(dtype_map)

        # cast datetime
        for col in datetime_cols:
            df[col] = (
                pd.to_datetime(df[col], utc=True)
                .astype("datetime64[us, UTC]")
            )
        return df
        
    def _gen_checksum(self, data: dict) -> str:
        """
        Generate a checksum using SHA256
        """
        data_str = json.dumps(data, sort_keys=True, ensure_ascii=True, default=str)
        hash_str = hashlib.sha256(data_str.encode()).hexdigest()
        return hash_str
    
    def _read_file(self, _file_path: Path = None, _file_format: str = None) -> pd.DataFrame:
        self.logger.debug(f"Read file at: [{_file_path}] with file_format: [{_file_format}]")
        if _file_path is None:
            self.logger.error(f"Not found file_path parameter")
            return None
        if _file_format is None:
            self.logger.error(f"Not found file_format parameter")
            return None
        
        if not _file_path.exists():
            self.logger.error(f"File not exist, file name: [{_file_path}]")
            return None
        
        if _file_format == 'jsonl':
            data = pd.read_json(_file_path, lines=True)
        elif _file_format == 'parquet':
            data = pd.read_parquet(_file_path)
        
        self.logger.debug(f"Read file succesful at: [{_file_path}]")
        return data
    
    def _save_to_parquet(self, df_save: pd.DataFrame = None, _file_path: Path = None) -> None:
        self.logger.debug(f"Save file at: [{_file_path}]")
        if df_save is None:
            self.logger.error(f"Not found `df_save` parameter")
            return None
        if _file_path is None:
            self.logger.error(f"Not found `_file_path` parameter")
            return None
        
        _file_path.parent.mkdir(parents=True, exist_ok=True)
        df_save.to_parquet(_file_path, index=False)
        self.logger.info(f"Save file succesful at: [{_file_path}]")
    
    def _build_rows_from_bronze(self) -> pd.DataFrame:
        
        # Process payload content
            # Flatten payload column -> items[0]
        items_dict = self.df_bronze['payload'].apply(lambda row: row.get("items", [{}])[0])
        df_items = pd.json_normalize(items_dict)
            # Rename columns
        df_items = df_items.rename(columns=self.df_items_rename_lst)
            # Cast type
        df_items = self._cast_type_df(df_items, self.string_cols_df_items, self.int64_cols_df_items, self.datetime_cols_df_items)
        
        # Process `metadata`
            # normalize data
        df_metadata = pd.json_normalize(self.df_bronze['metadata'])
            # cast type
        df_metadata = self._cast_type_df(df_metadata, self.string_cols_df_metadata, self.int64_cols_df_metadata, self.datetime_cols_df_metadata)    
        df_metadata = df_metadata.drop(columns=['_source_file'])
        
        # Process `system columns`
        df_system_cols = pd.DataFrame({
                    '_latest_id': [str(uuid.uuid4()) for _ in range(self.len_df_bronze)],
                    '_ingested_at': [datetime.now(timezone.utc) for _ in range(self.len_df_bronze)],
                    '_updated_at': pd.array([pd.NaT] * self.len_df_bronze, dtype="datetime64[us, UTC]"),
                    '_is_deleted': [False for _ in range(self.len_df_bronze)],
                    '_source_file': [str(self.file_path_bronze) for _ in range(self.len_df_bronze)]
                })
    
        # Process: df_output
            # Merge dfs
        df_output = pd.concat([df_metadata, df_items, df_system_cols], axis=1)
            # gen checksum
        df_output['_checksum'] = df_output[self.checksum_field].apply(lambda row: self._gen_checksum(row.to_dict()), axis=1)
            # just keep neccessary cols
        df_output = df_output[self.keep_silver_latest_cols_lst]

        return df_output
    
    def _process_silver_latest(self) -> None:
        
        # gen checksum from bronze data
        _df_rows_from_bronze = self._build_rows_from_bronze()
        
        # load existing silver_latest
        if self.file_path_silver_latest.exists():
            df_silver_latest_existing = self._read_file(self.file_path_silver_latest, "parquet")
      
            # Filter data for upsert
            df_classified = _df_rows_from_bronze.merge(
                df_silver_latest_existing[['channel_id', '_checksum', '_ingested_at']],
                on='channel_id',
                how='left',
                suffixes=['_bronze', '_silver_latest']
            )

            # `channel_id` not exist -> insert: df_insert
            df_insert = df_classified[df_classified['_checksum_silver_latest'].isna()]
            
            # `channel_id` exist & _checksum is not equal -> update: df_update
            df_update = df_classified[
                df_classified["_checksum_silver_latest"].notna() &
                (df_classified['_checksum_bronze'] != df_classified['_checksum_silver_latest'])
            ]
            df_update['_updated_at'] = [datetime.now(timezone.utc) for _ in range(len(df_update))]
            print(f"df_update: \n {df_update}")
            
            df_skip = df_classified[
                df_classified['_checksum_silver_latest'].notna() &
                (df_classified['_checksum_bronze'] == df_classified['_checksum_silver_latest'])
            ]

            # Upsert data
            print(f"INSERT: {len(df_insert)} | UPDATE: {len(df_update)} | SKIP: {len(df_skip)} | TOTAL_BRONZE: {len(_df_rows_from_bronze)} | TOTAL_EXISTING_SILVER_LATEST: {len(df_silver_latest_existing)}")
            df_silver_latest_write = df_silver_latest_existing.copy()
            print(f"df_silver_latest_write 0 SHAPE: {df_silver_latest_write.shape}")
                # Process: Insert data flow

            if len(df_insert) > 0:
                df_insert_cleaned = df_insert.drop(columns=['_checksum_silver_latest', '_ingested_at_silver_latest']).rename(columns={'_checksum_bronze': '_checksum', '_ingested_at_bronze': '_ingested_at'})
                df_silver_latest_write = pd.concat([df_silver_latest_existing, df_insert_cleaned], ignore_index=True)
                print(f"df_silver_latest_write df_insert SHAPE: {df_silver_latest_write.shape}")
                # Process: Upsert data flow
            if len(df_update) > 0:
                df_update_cleaned = df_update.drop(columns=['_checksum_silver_latest', '_ingested_at_bronze']).rename(columns={'_checksum_bronze': '_checksum', '_ingested_at_silver_latest': '_ingested_at'})
                df_update_processed = pd.concat([df_update_cleaned, df_silver_latest_write])
                df_silver_latest_write = df_update_processed.drop_duplicates(subset=['channel_id'], keep='first')
            
            self.logger.info(f"Start to UPSERT `silver_latest` at: [{self.file_path_silver_latest}]")
            self._save_to_parquet(df_silver_latest_write, self.file_path_silver_latest)
        else:
            # silver_latets not exist -> insert all (build_row//mode=insert row)
            self.logger.info(f"Start to save NEW `silver_latest` at: [{self.file_path_silver_latest}]")
            self._save_to_parquet(_df_rows_from_bronze, self.file_path_silver_latest)
            
    def run_pipeline(self):
        # Read Bronze Layer
        self.df_bronze = self._read_file(self.file_path_bronze, "jsonl")
        if self.df_bronze is None:
            exit()
        self.len_df_bronze = len(self.df_bronze)
        
        # Process Latest Table
        self._process_silver_latest()
        
if __name__ == "__main__":
    channels = Channels()        
    channels.run_pipeline()
        