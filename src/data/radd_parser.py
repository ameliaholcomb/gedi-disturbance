from datetime import date, datetime, timedelta
import geopandas as gpd
import numpy as np
import pandas as pd
from pyspark.sql.functions import col, udf
import pyspark.sql.types as T
import rioxarray as rxr
import xarray  # typing only
from typing import Tuple

from src import constants
from src.utils import raster_utils

PRODUCT_DIR = constants.RADD_PATH / "radd_alert_latest_v2023-04-16"


def yydoy_to_datetime(yydoy: int) -> date:
    """Convert a RADD degradation event int to a datetime."""
    if int(yydoy) != yydoy:
        raise ValueError("RADD date must be an integer.")
    yydoy = int(yydoy)
    # RADD dates are given in "YYDOY" integer format:
    # 20001 => the 1st day of 2020, or 2020-01-01
    # The days are thus 0-indexed from Dec 31 of the previous year
    year = 2000 + (yydoy // 1000)
    doy = yydoy - (yydoy // 1000) * 1000
    return date.fromisoformat(f"{year - 1}-12-31") + timedelta(days=doy)


@udf(returnType=T.ArrayType(T.DateType()))
def yydoys_to_dates_udf(yydoys):
    """Convert an array of RADD degradation event ints to SQL DateTypes"""
    ret = []
    for yydoy in yydoys:
        yydoy = int(yydoy)
        # RADD dates are given in "YYDOY" integer format:
        # 20001 => the 1st day of 2020, or 2020-01-01
        # The days are thus 0-indexed from Dec 31 of the previous year
        year = 2000 + (yydoy // 1000)
        doy = yydoy - (yydoy // 1000) * 1000
        ret.append(date.fromisoformat(f"{year - 1}-12-31") + timedelta(days=doy))
    return ret


@udf(returnType=T.IntegerType())
def datetime_to_yydoy_udf(d: datetime):
    """Convert a datetime to yydoy"""
    yy = d.year - 2000
    doy = (d.date() - date.fromisoformat(f"{d.year - 1}-12-31")).days
    return yy * 1000 + doy


def convert_shot_dates(shots_df):
    shots_df = shots_df.withColumn(
        "t1_yydoy", datetime_to_yydoy_udf(col("t1_absolute_time"))
    )
    shots_df = shots_df.withColumn(
        "t2_yydoy", datetime_to_yydoy_udf(col("t2_absolute_time"))
    )
    return shots_df


def get_sharding_geoms() -> gpd.GeoDataFrame:
    """Return an appropriate spatial sharding scheme for this dataset,
    for use with get_degradation_event_dates_for_shot_pair, below.

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
        T.StructField("t1_yydoy", T.IntegerType()),
        T.StructField("t2_yydoy", T.IntegerType()),
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
    rst: xarray.DataArray, df: pd.DataFrame
) -> Tuple[np.array, np.array, np.array]:
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

    s1 = df.t1_yydoy.values
    s2 = df.t2_yydoy.values
    intervening_disturbance = np.zeros(s1.shape[0])
    other_disturbance = np.zeros(s1.shape[0])
    disturb_date = np.zeros((9, s1.shape[0]))
    p_idx = 0
    for i in range(-1, 2):
        for j in range(-1, 2):
            d = rst.data[1, ys + j, xs + i]
            # Treatment set: disturbance was detected between s1 and s2
            disturbed_idx = np.logical_and(d > s1, d < s2)
            disturb_date[p_idx, disturbed_idx] = d[disturbed_idx]
            intervening_disturbance += disturbed_idx.astype(int)

            # Control set: degradation was detected (data != 0)
            # but it was before s1 or after s2
            disturbed_idx = np.logical_or(np.logical_and(d != 0, d < s1), d > s2)
            disturb_date[p_idx, disturbed_idx] = d[disturbed_idx]
            other_disturbance += disturbed_idx.astype(int)
            p_idx = p_idx + 1
    return intervening_disturbance, other_disturbance, disturb_date


def get_degradation_event_dates_for_shot_pair(df: pd.DataFrame) -> pd.DataFrame:
    """Get an assessment of degradation in areas around lon,lat coords.

    Args: Dataframe of the schema enforced with in_schema, above

    Returns:
        Dataframe of the schema enforced in out_schema, above.
        The column degrade_count can be used as a single output metric.
    """
    file_name = df.shard_token.unique()
    if not len(file_name) == 1:
        raise ValueError("This function must only operate on one file.")

    with rxr.open_rasterio(file_name[0]) as rst:
        (
            intervening_disturbance,
            other_disturbance,
            disturb_date,
        ) = _overlay_disturbance_testable(rst, df)
        with pd.option_context("mode.chained_assignment", None):
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
