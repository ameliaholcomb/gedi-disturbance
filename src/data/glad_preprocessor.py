import rasterio as rio
import rioxarray as rxr
import pathlib
import xarray as xr

from src import constants
from src.utils import raster_utils

# New path for the preprocessed files
NEW_PATH = pathlib.Path(constants.GLAD_PATH / "preprocessed")


def split_confidence_dates(ds: xr.Dataset) -> xr.Dataset:
    """Split the confidence-date GLAD values into separate bands."""
    # CDDDD => C, DDDD
    band = ds.values.squeeze()
    new_ds = xr.Dataset(
        data_vars=dict(
            confidence=(["y", "x"], band // 10000),
            date=(["y", "x"], band % 10000),
        ),
        coords={"y": ds.y.values, "x": ds.x.values},
    )
    return new_ds


def get_bounds_files() -> dict:
    """Get a dict of bounds to files."""
    bounds_files = {}
    for file in constants.GLAD_PATH.glob("*.tif"):
        b = raster_utils.raster_to_bbox(file).bounds
        bounds_files[b] = file
    for file in constants.BASEMAP_DIR.glob("*.tif"):
        b = raster_utils.raster_to_bbox(file).bounds
        if b in bounds_files:
            bounds_files[b] = (bounds_files[b], file)
        else:
            print(f"Missing alert file for {file}")
    for _, v in bounds_files.items():
        if not isinstance(v, tuple):
            print(f"Missing basemap file for {v}")
    # Remove any regions without both files
    bounds_files = {
        k: v for k, v in bounds_files.items() if isinstance(v, tuple)
    }
    return bounds_files


def main():
    bounds_files = get_bounds_files()
    print(f"Found {len(bounds_files)} files to fix.")

    for bounds, files in bounds_files.items():
        alert, base = files
        token = raster_utils.get_token(bounds)
        alert = rxr.open_rasterio(alert)
        base = rxr.open_rasterio(base)

        # Set to zero where the basemap indicates no primary forest
        alert = alert * base
        # Split the confidence-date values into separate bands
        new = split_confidence_dates(alert)
        # Write the new bands to a new file
        outfile = NEW_PATH / f"{token}.tif"
        new.rio.write_crs(alert.rio.crs, inplace=True)
        new.rio.to_raster(outfile, transform=alert.rio.transform())


if __name__ == "__main__":
    main()
