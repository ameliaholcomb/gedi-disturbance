import geopandas as gpd
import numpy as np
import warnings
from typing import List, Tuple


def _to_nesw(lon: float, lat: float) -> tuple[tuple[float, float], tuple[str, str]]:
    lon_ew = "W" if lon < 0 else "E"
    lat_ns = "S" if lat < 0 else "N"
    return (abs(lon), abs(lat)), (lon_ew, lat_ns)


def chunk_region(
    geometry: gpd.GeoDataFrame, chunk_x: int = 2, chunk_y: int = 2
) -> List[Tuple[str, gpd.GeoDataFrame]]:
    """Split up a region into smaller chunks for processing.

    Args:
        geometry: region to split up
        chunk_x: distance in x direction per chunk (computed in the geometry coordinate system)
        chunk_y: distance in y direction per chunk (computed in the geometry coordinate system)
    Returns:
        list of (name, geometry) tuples
        names are coded by the top left corner of the tile
    """
    minx, miny, maxx, maxy = geometry.bounds.values[0]
    minx, miny, maxx, maxy = (
        np.floor(minx),
        np.floor(miny),
        np.ceil(maxx) + chunk_x,
        np.ceil(maxy) + chunk_y,
    )

    print("Splitting up region into chunks ...")
    chunks = []
    # iterating over boxes starting at the bottom right
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        for y in range(int(miny), int(maxy), chunk_y):
            for x in range(int(minx), int(maxx), chunk_x):
                (left, top), (lon_dir, lat_dir) = _to_nesw(
                    x, y + chunk_y
                )  # top left corner
                tiletext = f"{lat_dir}{int(top)}_{lon_dir}{int(left)}"
                tile_region = geometry.clip_by_rect(x, y, x + chunk_x, y + chunk_y)
                if tile_region.is_empty.iat[0]:
                    continue
                chunks.append((tiletext, tile_region))
    return chunks
