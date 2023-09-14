import geopandas as gpd
import numpy as np
import pandas as pd
from pyspark.sql.functions import col, udf
import pyspark.sql.types as T
import re
import rioxarray as rxr
from pathlib import Path  # for typing only
from typing import List

from src import constants
from src.processing import jrc_overlays
from src.utils import raster_utils

PRODUCT_DIR = constants.JRC_PATH / "AnnualChange" / "tifs"


def get_sharding_geoms() -> gpd.GeoDataFrame:
    """Return an appropriate spatial sharding scheme for this dataset,
    for use with get_degradation_event_dates_for_shot_pair, below.

    We choose a simple sharding scheme:
        process all shots that are contained in the geometry corresponding
        to a single JRC tile ID together.
    We use the ID_CORNER string common to all files covering this tile
        as a shard token. This can be used to reconstruct a file name
        for the desired year.
    """
    files = list(PRODUCT_DIR.glob("*2021_SAM*.tif"))
    tokens = [re.search(r"SAM_(.*)\.tif", f.name).groups()[0] for f in files]
    gdf = gpd.GeoDataFrame(
        {
            "shard_token": tokens,
            "geometry": [raster_utils.raster_to_bbox(f) for f in files],
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
        T.StructField("t1_year", T.IntegerType()),
        T.StructField("t2_year", T.IntegerType()),
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
    ]
)


@udf(returnType=T.IntegerType())
def datetime_to_year_udf(d):
    """Convert datetime to year
    JRC data comes at a yearly resolution,
    so we don't care about the exact date
    """
    return d.date().year


def convert_shot_dates(shots_df):
    shots_df = shots_df.withColumn(
        "t1_year",
        datetime_to_year_udf(col("t1_absolute_time")),
    )
    shots_df = shots_df.withColumn(
        "t2_year",
        datetime_to_year_udf(col("t2_absolute_time")),
    )
    return shots_df


def _get_files(tokens) -> List[Path]:
    if not len(tokens) == 1:
        raise ValueError("This function must only operate on one file")

    years = [2019, 2020, 2021, 2022]

    return [
        PRODUCT_DIR / f"JRC_TMF_AnnualChange_v1_{year}_SAM_{tokens[0]}.tif"
        for year in years
    ]


def get_degradation_event_dates_for_shot_pair(df: pd.DataFrame) -> pd.DataFrame:
    """Get an assessment of degradation in areas around lon,lat coords.

    Args: Dataframe of the schema enforced with in_schema, above

    Returns:
        Dataframe of the schema enforced in out_schema, above.
        The column degrade_count can be used as a single output metric.
    """
    files = _get_files(df.shard_token.unique())

    # Load last four years of raster data (2019-2022)
    with rxr.open_rasterio(files[0]) as raster_2019:
        with rxr.open_rasterio(files[1]) as raster_2020:
            with rxr.open_rasterio(files[2]) as raster_2021:
                with rxr.open_rasterio(files[3]) as raster_2022:
                    (
                        measured,
                        control,
                        disturb_date,
                        valid,
                    ) = jrc_overlays._overlay_disturbance(
                        raster_2019, raster_2020, raster_2021, raster_2022, df
                    )

            with pd.option_context("mode.chained_assignment", None):
                df = df.loc[valid]
                df["measured_disturbance"] = measured
                df["control_disturbance"] = control
                for i in range(4):
                    df[f"p{i+1}_disturb_date"] = disturb_date[i]

        return df[
            [
                "t1_shot_number",
                "t2_shot_number",
                "measured_disturbance",
                "control_disturbance",
                *[f"p{i+1}_disturb_date" for i in range(4)],
            ]
        ]


def get_degradation_event_dates_for_shot_pair_no_deforestation(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Get an assessment of degradation in areas around lon,lat coords.

    Args: Dataframe of the schema enforced with in_schema, above

    Returns:
        Dataframe of the schema enforced in out_schema, above.
        The column degrade_count can be used as a single output metric.
    """
    files = _get_files(df.shard_token.unique())

    # Load last four years of raster data (2019-2022)
    with rxr.open_rasterio(files[0]) as raster_2019:
        with rxr.open_rasterio(files[1]) as raster_2020:
            with rxr.open_rasterio(files[2]) as raster_2021:
                with rxr.open_rasterio(files[3]) as raster_2022:
                    (
                        measured,
                        control,
                        disturb_date,
                        valid,
                    ) = jrc_overlays._overlay_disturbance_no_deforestation(
                        raster_2019, raster_2020, raster_2021, raster_2022, df
                    )

            with pd.option_context("mode.chained_assignment", None):
                df = df.loc[valid]
                df["measured_disturbance"] = measured
                df["control_disturbance"] = control
                for i in range(4):
                    df[f"p{i+1}_disturb_date"] = disturb_date[i]

        return df[
            [
                "t1_shot_number",
                "t2_shot_number",
                "measured_disturbance",
                "control_disturbance",
                *[f"p{i+1}_disturb_date" for i in range(4)],
            ]
        ]


out_schema_undisturbed = T.StructType(
    [
        T.StructField("t1_shot_number", T.LongType(), False),
        T.StructField("t2_shot_number", T.LongType(), False),
    ]
)


def filter_undisturbed_shot_pairs(df: pd.DataFrame) -> pd.DataFrame:
    """Get an assessment of degradation in areas around lon,lat coords.

    Args: Dataframe of the schema enforced with in_schema, above

    Returns:
        Dataframe of the schema enforced in out_schema, above.
        The column degrade_count can be used as a single output metric.
    """
    files = _get_files(df.shard_token.unique())

    # Load last four years of raster data (2019-2022)
    with rxr.open_rasterio(files[0]) as raster_2019:
        with rxr.open_rasterio(files[1]) as raster_2020:
            with rxr.open_rasterio(files[2]) as raster_2021:
                with rxr.open_rasterio(files[3]) as raster_2022:
                    uc, non_edge = jrc_overlays._overlay_undisturbed(
                        raster_2019, raster_2020, raster_2021, raster_2022, df
                    )

            with pd.option_context("mode.chained_assignment", None):
                df = df.loc[non_edge]
                df["undisturbed_count"] = uc
                df = df.loc[df.undisturbed_count == (9 * 4)]

        return df[
            [
                "t1_shot_number",
                "t2_shot_number",
            ]
        ]
