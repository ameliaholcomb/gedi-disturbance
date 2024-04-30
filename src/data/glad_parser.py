from datetime import date
import geopandas as gpd
import numpy as np
import pandas as pd
import pyspark.sql.types as T
from pyspark.sql.functions import col, datediff, lit
import rioxarray as rxr
import xarray as xr  # typing only
from typing import Tuple

from src import constants
from src.utils import raster_utils

START_DATE = date.fromisoformat("2014-12-31")
PRODUCT_DIR = constants.GLAD_PATH / "preprocessed"


def convert_shot_dates(shots_df):
    shots_df = shots_df.withColumn(
        "t1_days", datediff(col("t1_absolute_time"), lit(START_DATE))
    )
    shots_df = shots_df.withColumn(
        "t2_days", datediff(col("t2_absolute_time"), lit(START_DATE))
    )
    return shots_df


def get_sharding_geoms() -> gpd.GeoDataFrame:
    """Return an appropriate spatial sharding scheme for this dataset,
    for use with get_degradation_events_for_shot_pair, below.

    We choose a simple sharding scheme:
        process all shots that are contained in a single raster file
        geometry together.
    We use the full path of the raster file as a shard token to allow
    the subsequent function to call rxr.open_raster(shard_token).
    """
    degrade_files = list(PRODUCT_DIR.glob("*.tif"))
    gdf = gpd.GeoDataFrame(
        {
            "shard_token": [f.as_posix() for f in degrade_files],
            "geometry": [raster_utils.raster_to_bbox(f) for f in degrade_files],
        },
        geometry="geometry",
        crs=constants.WGS84,
    )
    if not gdf.shard_token.is_unique:
        raise ValueError("Shard tokens are not unique")
    return gdf


in_schema = T.StructType(
    [
        T.StructField("t1_shot_number", T.LongType()),
        T.StructField("t2_shot_number", T.LongType()),
        T.StructField("longitude", T.DoubleType()),
        T.StructField("latitude", T.DoubleType()),
        T.StructField("t1_days", T.IntegerType()),
        T.StructField("t2_days", T.IntegerType()),
        T.StructField("shard_token", T.StringType()),
    ]
)

out_schema = T.StructType(
    [
        T.StructField("t1_shot_number", T.LongType()),
        T.StructField("t2_shot_number", T.LongType()),
        T.StructField("measured_disturbance", T.IntegerType()),
        T.StructField("control_disturbance", T.IntegerType()),
        T.StructField("p1_disturb_date", T.IntegerType()),
        T.StructField("p2_disturb_date", T.IntegerType()),
        T.StructField("p3_disturb_date", T.IntegerType()),
        T.StructField("p4_disturb_date", T.IntegerType()),
        T.StructField("p5_disturb_date", T.IntegerType()),
        T.StructField("p6_disturb_date", T.IntegerType()),
        T.StructField("p7_disturb_date", T.IntegerType()),
        T.StructField("p8_disturb_date", T.IntegerType()),
        T.StructField("p9_disturb_date", T.IntegerType()),
    ]
)


def _overlay_disturbance_testable(
    rst: xr.DataArray, df: pd.DataFrame, buffer_days: int
) -> Tuple[np.array, np.array, np.array, np.array]:
    xs = raster_utils.get_idx(rst.x.data, df.longitude.values)
    ys = raster_utils.get_idx(rst.y.data, df.latitude.values)
    # For now, discard any coords on the edge of the raster
    valid = np.logical_and.reduce(
        (
            (xs - 1 >= 0),
            (xs + 1 < rst.shape[2]),
            (ys - 1 >= 0),
            (ys + 1 < rst.shape[1]),
        )
    )
    xs = xs[valid]
    ys = ys[valid]
    df = df.loc[valid]

    s1 = df.t1_days.values
    s2 = df.t2_days.values
    intervening_disturbance = np.zeros(s1.shape[0])
    other_disturbance = np.zeros(s1.shape[0])
    disturb_date = np.zeros((9, s1.shape[0]))
    p_idx = 0
    for i in range(-1, 2):
        for j in range(-1, 2):
            # Select only high-confidence alerts (c == 3)
            c = rst.data[0, ys + j, xs + i]
            # Convert to int to avoid overflow when subtracting BUFFER_DAYS
            # (could also add buffer to s1 and s2, but this is more readable)
            d = rst.data[1, ys + j, xs + i].astype(int)
            d = np.where(c == 3, d, 0)
            # Treatment set: disturbance was detected between s1 and s2
            disturbed_idx = np.logical_and((d - buffer_days) > s1, d < s2)
            disturb_date[p_idx, disturbed_idx] = d[disturbed_idx]
            intervening_disturbance += disturbed_idx.astype(int)
            # max_confidence = np.maximum(max_confidence, c)

            # Control set: degradation was detected (data != 0)
            # but it was before s1 or after s2
            disturbed_idx = np.logical_and(
                d != 0, np.logical_or(d < s1, (d - buffer_days) > s2)
            )
            disturb_date[p_idx, disturbed_idx] = d[disturbed_idx]
            other_disturbance += disturbed_idx.astype(int)
            p_idx = p_idx + 1
    return intervening_disturbance, other_disturbance, disturb_date, valid


def get_degradation_event_dates_for_shot_pair(
    buffer_days: int, df: pd.DataFrame
) -> pd.DataFrame:
    """Get an assessment of degradation in areas around lon,lat coords.

    Args: Dataframe of the schema enforced with in_schema, above

    Returns:
        Dataframe of the schema enforced in out_schema, above.
        The column degrade_count can be used as a single output metric.
    """

    file_name = df.shard_token.unique()
    if len(file_name) != 1:
        raise ValueError("Programmer error: shard_token is not unique.")

    with rxr.open_rasterio(file_name[0]) as rst:
        (
            intervening_disturbance,
            other_disturbance,
            disturb_date,
            valid,
        ) = _overlay_disturbance_testable(rst, df, buffer_days)
        with pd.option_context("mode.chained_assignment", None):
            df = df.loc[valid]
            df["measured_disturbance"] = intervening_disturbance
            df["control_disturbance"] = other_disturbance
            for i in range(9):
                df[f"p{i+1}_disturb_date"] = disturb_date[i]
    return df[
        [
            "t1_shot_number",
            "t2_shot_number",
            "measured_disturbance",
            "control_disturbance",
            *[f"p{i+1}_disturb_date" for i in range(9)],
        ]
    ]
