import geopandas as gpd
import numpy as np
import pandas as pd
import pyspark.sql.types as T
import re
import rioxarray as rxr
import xarray as xr  # for typing only
from typing import Tuple

from src import constants
from src.data import jrc_parser
from src.utils import raster_utils

PRODUCT_DIR = constants.JRC_PATH / "Intensity"
DISRUPT_OBS_DIR = "disrupt"
VALID_OBS_DIR = "valid"
# Example filenames:
# disrupt_obs_S9_W75-0000016384-0000016384.tif
# valid_obs_S9_W80.tif
FILE_NAME_RE = r"([a-z]+_obs)_([SN]\d+_[WE]\d+(-\d+-\d+)?)"


def get_sharding_geoms() -> gpd.GeoDataFrame:
    """Return an appropriate spatial sharding scheme for this dataset,
    for use with get_intensity_for_degrade_event, below.

    We choose a simple sharding scheme:
        process all shots that are contained in the geometry corresponding
        to a single disrupt/valid_obs raster together.
        We use the export raster code (e.g. S9_W75-0000088320-0000123648) as a shard
        token, which matches between the disrupt_obs and valid_obs
        raster sets.
    """
    files = list((PRODUCT_DIR / DISRUPT_OBS_DIR).glob("*.tif"))
    tokens = [re.search(FILE_NAME_RE, f.stem).groups()[1] for f in files]
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
        T.StructField("p1_disturb_date", T.IntegerType()),
        T.StructField("p2_disturb_date", T.IntegerType()),
        T.StructField("p3_disturb_date", T.IntegerType()),
        T.StructField("p4_disturb_date", T.IntegerType()),
        T.StructField("t1_year", T.IntegerType()),
        T.StructField("t2_year", T.IntegerType()),
        T.StructField("shard_token", T.StringType()),
    ]
)

out_schema = T.StructType(
    [
        T.StructField("t1_shot_number", T.LongType()),
        T.StructField("t2_shot_number", T.LongType()),
        T.StructField("disturb_intensity", T.FloatType()),
    ]
)


def _overlay_intensity(
    rst_disrupt: xr.DataArray, rst_valid: xr.DataArray, df: pd.DataFrame
) -> Tuple[np.array, np.array]:
    xs = raster_utils.get_idxs_two_nearest(
        rst_disrupt.x.data, df.longitude.values
    )
    ys = raster_utils.get_idxs_two_nearest(
        rst_disrupt.y.data, df.latitude.values
    )
    # For now, discard any coords on the edge of the raster
    valid = np.logical_and.reduce(
        (
            (np.min(xs, axis=1) >= 0),
            (np.max(xs, axis=1) < rst_disrupt.shape[2]),
            (np.min(ys, axis=1) >= 0),
            (np.max(ys, axis=1) < rst_disrupt.shape[1]),
        )
    )
    xs = xs[valid]
    ys = ys[valid]
    df = df.loc[valid]
    intensity = np.zeros(len(df))

    p_idx = 0
    for i in [0, 1]:
        for j in [0, 1]:
            first_disrupt_idx = df[f"p{p_idx+1}_disturb_date"].values - 2019
            # Some pixels may have disturb_date == 0, because not all
            # pixels in the window were marked as disturbed.
            cs = first_disrupt_idx >= 0
            disrupt_obs = rst_disrupt.data[
                first_disrupt_idx[cs], ys[cs, i], xs[cs, j]
            ]
            valid_obs = rst_valid.data[
                first_disrupt_idx[cs], ys[cs, i], xs[cs, j]
            ]
            # Some pixels may not have any valid observations in this year,
            # because it was another pixel in the window that caused the shot
            # to be marked disturbed.
            # Avoid dividing by zero and add 0 to the intensity.
            valid_obs[valid_obs == 0] = 1  # If valid == 0, disrupt == 0 anyway.
            intensity[cs] += disrupt_obs / valid_obs
            p_idx += 1
    return intensity, valid


def get_intensity_for_degrade_event(df: pd.DataFrame) -> pd.DataFrame:
    """Measure intensity of a detected disturbance around lon,lat coords.

    Args: Dataframe of the schema enforced with in_schema, above

    Returns:
        Dataframe of the schema enforced in out_schema, above.
    """
    token = df.shard_token.unique()
    if not len(token) == 1:
        raise ValueError("This function must only operate on one file")
    print(f"Processing shard token {token[0]} with {len(df)} rows")

    disrupt_file = PRODUCT_DIR / DISRUPT_OBS_DIR / f"disrupt_obs_{token[0]}.tif"
    valid_file = PRODUCT_DIR / VALID_OBS_DIR / f"valid_obs_{token[0]}.tif"

    with rxr.open_rasterio(disrupt_file) as rst_disrupt:
        with rxr.open_rasterio(valid_file) as rst_valid:
            intensity, non_edge = _overlay_intensity(
                rst_disrupt, rst_valid, df
            )

        with pd.option_context("mode.chained_assignment", None):
            df = df.loc[non_edge]
            df["disturb_intensity"] = intensity / 4

    return df[
        [
            "t1_shot_number",
            "t2_shot_number",
            "disturb_intensity",
        ]
    ]
