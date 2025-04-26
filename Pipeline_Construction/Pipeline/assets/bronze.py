import pandas as pd
from dagster import asset, Output

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom"],
    group_name="BRONZE",
    compute_kind="MySQL"
)
def raw_videos_day(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM videos_day "
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    return Output(
        pd_data,
        metadata={
        "table": "videos",
        "records count": len(pd_data),
        },
    )

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom"],
    group_name="BRONZE",
    compute_kind="MySQL"
)
def raw_channels_day(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM channels_day "
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    return Output(
        pd_data,
        metadata={
        "table": "channels",
        "records count": len(pd_data),
        },
    )