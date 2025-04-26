import pandas as pd
from dagster import asset, Output, Definitions, AssetIn
from resources.mysql_io_manager import MySQLIOManager
from resources.minio_io_manager import MinIOIOManager
from resources.psql_io_manager import PostgreSQLIOManager

from assets.bronze import raw_videos_day, raw_channels_day
from assets.silver import clean_raw_videos_day, clean_raw_channels_day
# from assets.gold import dim_tag, dim_topic, dim_channel, fact_video
# from assets.warehouse import dwh_dim_tag, dwh_dim_topic, dwh_dim_channel

from assets.gold import dwh_channels_day, dwh_videos_day

MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "database": "youtube",
    "user": "admin",
    "password": "admin123",
}

MINIO_CONFIG = {
    "endpoint_url": "localhost:9000",
    "bucket": "warehouse",
    "aws_access_key_id": "minio",
    "aws_secret_access_key": "minio123",
}

PSQL_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "youtube",
    "user": "admin",
    "password": "admin123",
}


# define list of assets and resources for data pipeline
defs = Definitions(
    assets=[raw_videos_day, raw_channels_day, clean_raw_videos_day,clean_raw_channels_day,dwh_channels_day,dwh_videos_day],
    resources={
        "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
        "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
    },
)
