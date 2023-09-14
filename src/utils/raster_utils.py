import numpy as np
import numba
import rasterio as rio
from shapely.geometry import box


def raster_to_bbox(file_path):
    """Get the bounding box convered by this raster."""
    if not file_path.name.endswith(".tif"):
        return
    with rio.Env():
        with rio.open(file_path) as src:
            return box(*(src.bounds))


# TODO: Provide a function to check this assumption
def get_idx_fast(array, values):
    """Find the pixel index in a raster covering the given coords.

    Note: ONLY safe for rasters covering small areas, where step size
    does not differ much across the raster.
    Args:
        array (n,): array of of raster coordinate values
                    e.g. from raster.x.data
        values (k,): coordinate values to find in the raster
    Returns:
        np.array(k,): indices containing coordinate values
    """
    step = array[1] - array[0]
    return np.floor((values - array[0]) / step).astype(np.int64)


@numba.njit
def get_idx(array, values):
    """Find the pixel index in a raster covering the given coords.

    Args:
        array (n,): array of of raster coordinate values
                    e.g. from raster.x.data
        values (k,): coordinate values to find in the raster
    Returns:
        np.array(k,): indices containing coordinate values
    """
    nearest_idxs = np.zeros_like(values, dtype=np.int64)
    for i, value in enumerate(values):
        nearest_idxs[i] = (np.abs(array - value)).argmin()
    return nearest_idxs


@numba.njit
def get_idxs_two_nearest(array, values):
    """Find the 2x2 pixel box in a raster that best covers a small
     circle around the given coords.

    If the coords fall outside the raster, the nearest pixel is
    the border pixel, but the second-nearest pixel will be listed
    as an out-of-bounds index.

    Args:
        array (n,): array of of raster coordinate values
                    e.g. from raster.x.data
        values (k,): coordinate values to find in the raster
    Returns:
        np.array(k,): indices containing coordinate values
    """
    half_pixel = (array[1] - array[0]) / 2
    array_center = array + half_pixel
    argmins = np.zeros((*values.shape, 2), dtype=np.int64)
    for i, value in enumerate(values):
        argmins[i, 0] = (np.abs(array_center - value)).argmin()
        if value < array_center[argmins[i, 0]]:
            argmins[i, 1] = argmins[i, 0] - 1
        else:
            argmins[i, 1] = argmins[i, 0] + 1

    return argmins
