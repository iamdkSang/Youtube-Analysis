# import pandas as pd
# from dagster import asset, Output, AssetIn
#
# @asset(
#     ins={
#         "clean_raw_videos_day": AssetIn(key_prefix=["silver", "ecom"]),
#     },
#     key_prefix=["gold", "ecom"],
#     io_manager_key="minio_io_manager",
#     required_resource_keys={"minio_io_manager"},
#     group_name="GOLD",
#     compute_kind="POSTGRESQL"
# )
# def dim_tag(clean_raw_videos_day: pd.DataFrame) -> Output[pd.DataFrame]:
#     pd_data = clean_raw_videos_day.copy()
#     pd_data = pd_data[['Tag']].drop_duplicates()
#     pd_data['Tag ID'] = pd_data.reset_index().index
#     return Output(
#         pd_data,
#         metadata={
#             "table": "videos",
#             "records counts": len(pd_data),
#         },
#     )
#
#
#
# @asset(
#     ins={
#         "clean_raw_channels_day": AssetIn(key_prefix=["silver", "ecom"]),
#     },
#     key_prefix=["gold", "ecom"],
#     io_manager_key="minio_io_manager",
#     required_resource_keys={"minio_io_manager"},
#     group_name="GOLD",
#     compute_kind="POSTGRESQL"
# )
# def dim_topic(clean_raw_channels_day: pd.DataFrame) -> Output[pd.DataFrame]:
#     pd_data = clean_raw_channels_day.copy()
#     pd_data = pd_data[['Topic']].drop_duplicates()
#     pd_data['Topic ID'] = pd_data.reset_index().index
#     return Output(
#         pd_data,
#         metadata={
#             "table": "videos",
#             "records counts": len(pd_data),
#         },
#     )
#
#
#
# @asset(
#     ins={
#         "clean_raw_channels_day": AssetIn(key_prefix=["silver", "ecom"]),
#         "dim_topic": AssetIn(key_prefix=["gold", "ecom"]),
#     },
#     key_prefix=["gold", "ecom"],
#     io_manager_key="minio_io_manager",
#     required_resource_keys={"minio_io_manager"},
#     group_name="GOLD",
#     compute_kind="POSTGRESQL"
# )
# def dim_channel(clean_raw_channels_day: pd.DataFrame, dim_topic: pd.DataFrame) -> Output[pd.DataFrame]:
#     topic_id = dim_topic['Topic ID']
#     selected_columns = ['ChannelID','ChannelName','Description','View','NumSubscriber','NumVideo']
#     get_videos = clean_raw_channels_day[selected_columns]
#     pd_data = pd.concat([get_videos, topic_id], axis=1)
#     return Output(
#         pd_data,
#         metadata={
#             "table": "videos",
#             "records counts": len(pd_data),
#         },
#     )
#
#
# @asset(
#     ins={
#         "clean_raw_videos_day": AssetIn(key_prefix=["silver", "ecom"]),
#         "dim_topic": AssetIn(key_prefix=["gold", "ecom"]),
#         "dim_tag": AssetIn(key_prefix=["gold", "ecom"]),
#         "dim_channel": AssetIn(key_prefix=["gold", "ecom"])
#     },
#     key_prefix=["gold", "ecom"],
#     io_manager_key="minio_io_manager",
#     required_resource_keys={"minio_io_manager"},
#     group_name="Gold",
#     compute_kind="POSTGRESQL"
# )
# def fact_video(clean_raw_videos_day: pd.DataFrame, dim_topic: pd.DataFrame, dim_tag: pd.DataFrame, dim_channel: pd.DataFrame ) -> Output[pd.DataFrame]:
#     topic_id = dim_topic['Topic ID']
#     tag_id = dim_tag['Tag ID']
#     channel_id = dim_channel['ChannelID']
#     selected_columns = ['VideoID', 'Title', 'Duration', 'PublishedAt', 'Views', 'Likes', 'NumComments', 'Description']
#     get_videos = clean_raw_videos_day[selected_columns]
#     pd_data = pd.concat([get_videos,topic_id,tag_id,channel_id ], axis = 1)
#     return Output(
#         pd_data,
#         metadata={
#             "table": "videos",
#             "records counts": len(pd_data),
#         },
#     )

import pandas as pd
from dagster import asset, Output, AssetIn, multi_asset, AssetOut

@multi_asset(
    ins={
        "clean_raw_channels_day": AssetIn(key_prefix=["silver", "ecom"],)
    },
    outs={
        "clean_raw_channels_day": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["gold", "public"],
            group_name="Gold",
            metadata={
                "primary_keys": [
                    "ChannelID",
                ],
                "columns": [
                        "ChannelID",
                        "ChannelName",
                        "Description",
                        "View",
                        "NumSubscriber",
                        "Topic",
                        "NumVideo",
                ],
            }
        )
    },
    compute_kind="Postgres"
    )
def dwh_channels_day(clean_raw_channels_day) -> Output[pd.DataFrame]:
    return Output(
        clean_raw_channels_day,
        metadata={
            "schema": "public",
            "table": "clean_raw_channels_day",
            "records counts": len(clean_raw_channels_day),
        },
    )


@multi_asset(
    ins={
        "clean_raw_videos_day": AssetIn(key_prefix=["silver", "ecom"],)
    },
    outs={
        "clean_raw_videos_day": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["gold", "public"],
            group_name="Gold",
            metadata={
                "primary_keys": [
                    "ChannelID",
                ],
                "columns": [
                        "ChannelID",
                        "ChannelName",
                        "VideoID",
                        "Title",
                        "Topic",
                        "Duration",
                        "PublishedAt",
                        "Views",
                        "Likes",
                        "NumComments",
                        "Date",
                        "Description",
                         "Tag",

                ],
            }
        )
    },
    compute_kind="Postgres"
    )
def dwh_videos_day(clean_raw_videos_day) -> Output[pd.DataFrame]:
    return Output(
        clean_raw_videos_day,
        metadata={
            "schema": "public",
            "table": "clean_raw_channels_day",
            "records counts": len(clean_raw_videos_day),
        },
    )


