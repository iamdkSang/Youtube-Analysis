import pandas as pd
from dagster import asset, Output, AssetIn

@asset(
    ins={
        "raw_videos_day": AssetIn(key_prefix=["bronze", "ecom"]),
    },
    key_prefix=["silver", "ecom"],
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    group_name="SILVER",
    compute_kind="Pandas"
)
def clean_raw_videos_day(raw_videos_day: pd.DataFrame) -> Output[pd.DataFrame]:
    pd_data = raw_videos_day
    pd_data['Description'].fillna('No Description', inplace=True)
    pd_data['Topic'].fillna('No Topic', inplace=True)
    pd_data['Topic'] = pd_data['Topic'].replace('None', 'No Topic')
    pd_data['Likes'].fillna('0', inplace=True)
    pd_data['Likes'] = pd_data['Likes'].replace('None', '0')
    pd_data['NumComments'].fillna('0', inplace=True)
    pd_data['NumComments'] = pd_data['NumComments'].replace('None', '0')
    pd_data['Tag'].fillna('No Tag', inplace=True)
    pd_data['Tag'] = pd_data['Tag'].replace('None','No Tag')

    return Output(
        pd_data,
        metadata={
            "table": "videos",
            "records counts": len(pd_data),
        },
    )


@asset(
    ins={
        "raw_channels_day": AssetIn(key_prefix=["bronze", "ecom"]),
    },
    key_prefix=["silver", "ecom"],
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    group_name="SILVER",
    compute_kind="Pandas"
)

def clean_raw_channels_day(raw_channels_day: pd.DataFrame) -> Output[pd.DataFrame]:
    pd_data = raw_channels_day
    pd_data['Description'].fillna('No Description', inplace=True)
    pd_data['Description'] = pd_data['Description'].replace('default_value', 'No Description')
    pd_data['Description'] = pd_data['Description'].replace("\n","No Description")
    return Output(
        pd_data,
        metadata={
            "table": "channels",
            "records counts": len(pd_data),
        },
    )


