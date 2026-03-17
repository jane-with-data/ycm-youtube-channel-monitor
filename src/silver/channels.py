import pandas as pd
import json
from pathlib import Path
from datetime import datetime, timezone
import uuid
import hashlib
import pyarrow
from config.constants import PATH

pd.set_option("display.max_rows", None, "display.max_columns", None)
pd.set_option("display.max_colwidth", None)

# Flow
# read bronze data -> process table latest -> write parquet -> process snapshot hourly -> write parquet

class Channels:
    def __init__(self):
        # Gen File Path
        self.dir_data_bronze = PATH['DIR_DATA_BRONZE']
        self.dir_data_silver = PATH['DIR_DATA_SILVER']
        
        year = datetime.now(timezone.utc).strftime('%Y')
        month = datetime.now(timezone.utc).strftime('%m')
        day = datetime.now(timezone.utc).strftime('%d')
        hour = datetime.now(timezone.utc).strftime('%H')
        # snapshot = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:00:00')
        
        file_name_bronze = f"channels.parquet"
        self.file_path_bronze = self.dir_data_bronze / "youtube" / "channels" / f"year={year}" / f"month={month}" / f"day={day}" / f"hour={hour}" / file_name_bronze
        
        file_name_silver_latest = f"channels_latest.parquet"
        self.file_path_silver_latest = self.dir_data_silver / "youtube" / "channels" / f"latest" / file_name_silver_latest
        
        file_name_silver_snapshot_hourly = f"channels_snapshot_hourly.parquet"
        self.file_path_silver_snapshot = self.dir_data_silver / "youtube" / "channels" / f"snapshot" / f"year={year}" / f"month={month}" / f"day={day}" / f"hour={hour}" / file_name_silver_snapshot_hourly
        
        # Read Bronze data
        self.df_bronze = self._read_parquet(self.file_path_bronze)
        self.len_df_bronze = len(self.df_bronze)
        
        # checksum
        self.checksum_field = [
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
                '_source',
                '_bronze_id',
                '_checksum'
            ]
    
    def _gen_checksum(self, data: dict) -> str:
        """
        Generate a checksum using SHA256
        """
        str = json.dumps(data, sort_keys=True, ensure_ascii=True)
        str_hash = hashlib.sha256(str.encode()).hexdigest()
        print("============================>\n")
        print(f"_gen_checksum DATA: {data}")
        print(f"_gen_checksum STR: {str}")
        print(f"_gen_checksum HASH: {str_hash}")
        return str_hash
    
    def _read_parquet(self, _file_path_layer = None) -> pd.DataFrame:
        if _file_path_layer is None:
            _file_path_layer = self.file_path_bronze
        data = pd.read_parquet(_file_path_layer)
        return data    
    
    def _save_to_parquet(self, df_save: pd.DataFrame, _file_name_silver_latest = None) -> None:
        if _file_name_silver_latest is None:
            _file_name_silver_latest = self.file_path_silver_latest
        
        _file_name_silver_latest.parent.mkdir(parents=True, exist_ok=True)
        df_save.to_parquet(_file_name_silver_latest, index=False)
    
    def _build_rows(self, df_business: pd.DataFrame) -> pd.DataFrame:
        
        len_df_business = len(df_business)
        
        df_business_processed = pd.DataFrame()
        df_business_processed["channel_id"] = df_business["channel_id"].astype("string")
        df_business_processed["playlist_id_upload"] = df_business["playlist_id_upload"].astype("string")
        df_business_processed["title"] = df_business["title"].astype("string")
        df_business_processed["custom_url"] = df_business["custom_url"].astype("string")
        df_business_processed["description"] = df_business["description"].astype("string")
        df_business_processed["published_at"] = df_business["published_at"]
        df_business_processed["thumbnails_default_url"] = df_business["thumbnails_default_url"].astype("string")
        df_business_processed["thumbnails_default_width"] = df_business["thumbnails_default_width"].astype("Int64")
        df_business_processed["thumbnails_default_height"] = df_business["thumbnails_default_height"].astype("Int64")
        df_business_processed["thumbnails_medium_url"] = df_business["thumbnails_medium_url"].astype("string")
        df_business_processed["thumbnails_medium_width"] = df_business["thumbnails_medium_width"].astype("Int64")
        df_business_processed["thumbnails_medium_height"] = df_business["thumbnails_medium_height"].astype("Int64")
        df_business_processed["thumbnails_high_url"] = df_business["thumbnails_high_url"].astype("string")
        df_business_processed["thumbnails_high_width"] = df_business["thumbnails_high_width"].astype("Int64")
        df_business_processed["thumbnails_high_height"] = df_business["thumbnails_high_height"].astype("Int64")
        df_business_processed["view_count"] = df_business["view_count"].astype("Int64")
        df_business_processed["subscriber_count"] = df_business["subscriber_count"].astype("Int64")
        df_business_processed["video_count"] = df_business["video_count"].astype("Int64")
        df_business_processed["_bronze_id"] = df_business["_bronze_id"].astype("string")
        
        df_system_cols = pd.DataFrame({
                '_latest_id': [str(uuid.uuid4()) for _ in range(len_df_business)],
                '_ingested_at': [datetime.now(timezone.utc).isoformat() for _ in range(len_df_business)],
                '_updated_at': [None for _ in range(len_df_business)],
                '_is_deleted': [False for _ in range(len_df_business)],
                '_source': [str(self.file_path_bronze) for _ in range(len_df_business)],
                '_checksum': df_business_processed[self.checksum_field].apply(lambda row: self._gen_checksum(row.to_dict()), axis=1)
            })
        
        df_output = pd.concat([df_business_processed, df_system_cols], axis=1)
        df_output = df_output[self.keep_silver_latest_cols_lst]
        return df_output
    
    def _process_silver_latest(self) -> pd.DataFrame:
        
        # build new checksum for bronze layer
        df_bronze = self.df_bronze.copy()
        # gen checksum
        df_bronze['_checksum'] = df_bronze.apply(
            lambda x: self._gen_checksum(x.to_dict()), axis=1)
        # print(df_bronze.columns)
        # is `silver_channels_latest` exist?
        # silver_latets exist -> upseart (build_rows -> upsert)
        
        # load existing silver_latest
        if self.file_path_silver_latest.exists():
            df_silver_latest_existing = self._read_parquet(self.file_path_silver_latest)
            
            # Filter data for upsert
            df_merged = df_bronze.merge(
                df_silver_latest_existing[['channel_id', '_checksum']],
                on='channel_id',
                how='left',
                suffixes=['_bronze', '_silver_latest']
            )

            df_insert = df_merged[df_merged['_checksum_silver_latest'].isna()]
            # print(f"df_insert: \n \n {df_insert}")
            df_update = df_merged[
                df_merged["_checksum_silver_latest"].notna() &
                (df_merged['_checksum_bronze'] != df_merged['_checksum_silver_latest'])
            ]
            
            df_skip = df_merged[
                df_merged['_checksum_silver_latest'].notna() &
                (df_merged['_checksum_bronze'] == df_merged['_checksum_silver_latest'])
            ]

            print(f"INSERT: {len(df_insert)} | UPDATE: {len(df_update)} | SKIP: {len(df_skip)} | TOTAL_BRONZE: {len(df_bronze)} | TOTAL_EXISTING_SILVER_LATEST: {len(df_silver_latest_existing)}")

            # Upsert data
                # Process: Insert data flow
            if len(df_insert) > 0:
                # df_insert_clean = df_insert.drop(columns=['_checksum_bronze', '_checksum_silver_latest'])
                df_new_rows = self._build_rows(df_insert)
                df_silver_latest_write = pd.concat([df_silver_latest_existing, df_new_rows], ignore_index=True)
                
                # Process: Upsert data flow
            if len(df_update) > 0:
                df_new_rows = self._build_rows(df_update)
                # print(f"df_new_row: {df_new_rows}")
                # print(f"df_silver befrore: {df_silver_latest_existing}")
                for _, row_update in df_new_rows.iterrows():
                    # find updated row
                    mask = df_silver_latest_existing['channel_id'] == row_update['channel_id']
                    # update business field
                    for col in self.checksum_field:
                        # print(f"col: {col}")
                        # print(f"col value: {df_silver_latest_existing[col]}")
                        # print(f"update value: {row_update[col]}")
                        # print(f"update dtypes: {row_update[col].dtypes}")
                        df_silver_latest_existing.loc[mask, col] = row_update[col]
                        
                    # print(f"df_silver after: {df_silver_latest_existing}")
                    # update system field
                    df_silver_latest_existing.loc[mask, '_updated_at'] = datetime.now(timezone.utc).isoformat()
                    df_silver_latest_existing.loc[mask, '_checksum'] = row_update['_checksum']
                    # df_silver_latest_existing.loc[mask, '_checksum_compare'] = self._gen_checksum(df_silver_latest_existing[self.checksum_field].to_dict())
                    df_silver_latest_existing['_checksum_compare'] = df_silver_latest_existing[self.checksum_field].apply(lambda x: self._gen_checksum(x.to_dict()), axis=1)
                    
                    print('sivler')
                    # print(df_silver_latest_existing.columns)
                    # print(df_silver_latest_existing.dtypes)
                    print(df_silver_latest_existing)
                # print(df_silver_latest_existing)
        else:
            # silver_latets not exist -> insert all (build_row//mode=insert row)
            df_silver_latest_write = self._build_rows(df_bronze)
            print(f"Done//build_row//mode=insert row")
            self._save_to_parquet(df_silver_latest_write)
            
    def run_pipeline(self):
        # Read Bronze Layer
        # df_bronze = self._read_parquet(self.file_path_bronze)
        
        # Process Latest Table
        df_latest = self._process_silver_latest()
        
        # # Write Latest Table to Silver Layer
        # self._write_silver_layer(df_latest, self.file_path_silver_latest)
        
        # # Process Snapshot Hourly Table
        # df_snapshot_hourly = self._process_snapshot_hourly_table(df_bronze)
        
        # # Write Snapshot Hourly Table to Silver Layer
        # self._write_silver_layer(df_snapshot_hourly, self.file_path_silver_snapshot)
if __name__ == "__main__":
    channels = Channels()        
    channels.run_pipeline()
        