import pandas as pd
from dagster import asset, Output, Definitions, AssetIn
from resources.mysql_io_manager import MySQLIOManager
from resources.minio_io_manager import MinIOIOManager

from assets.bronze import raw_videos_day, raw_channels_day
from assets.silver import  clean_raw_videos_day, clean_raw_channels_day

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

# define list of assets and resources for data pipeline
defs = Definitions(
    assets=[raw_videos_day, raw_channels_day],
    resources={
        "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG)
    },
)