import pandas as pd
import numpy as np
import xarray as xr  # for typing only
from typing import Tuple

from src.utils import raster_utils

JRC_ANNUAL_CHANGE_UNDISTURBED = 1
JRC_ANNUAL_CHANGE_DEGRADATION = 2
JRC_ANNUAL_CHANGE_DEFORESTATION = 3
JRC_ANNUAL_CHANGE_RECOVERY = 4
JRC_ANNUAL_CHANGE_OTHER_LAND_USE = 6


def _overlay_disturbance(
    rst_2019: xr.DataArray,
    rst_2020: xr.DataArray,
    rst_2021: xr.DataArray,
    rst_2022: xr.DataArray,
    df: pd.DataFrame,
) -> Tuple[np.array, np.array, np.array]:
    xs = raster_utils.get_idxs_two_nearest(rst_2019.x.data, df.longitude.values)
    ys = raster_utils.get_idxs_two_nearest(rst_2019.y.data, df.latitude.values)
    # For now, discard any coords on the edge of the raster
    valid = np.logical_and.reduce(
        (
            (np.min(xs, axis=1) >= 0),
            (np.max(xs, axis=1) < rst_2019.shape[2]),
            (np.min(ys, axis=1) >= 0),
            (np.max(ys, axis=1) < rst_2019.shape[1]),
        )
    )
    xs = xs[valid]
    ys = ys[valid]
    df = df.loc[valid]

    s1 = df.t1_year.values
    s2 = df.t2_year.values
    measured = np.zeros(s1.shape[0])
    # control: there was a disturbance,
    # but the paired GEDI shots didn't happen to capture it.
    control = np.zeros(s1.shape[0])
    disturb_date = np.zeros((4, s1.shape[0]))
    p_idx = 0
    for i in [0, 1]:
        for j in [0, 1]:
            d_2019 = rst_2019.data[0, ys[:, j], xs[:, i]]
            d_2020 = rst_2020.data[0, ys[:, j], xs[:, i]]
            d_2021 = rst_2021.data[0, ys[:, j], xs[:, i]]
            d_2022 = rst_2022.data[0, ys[:, j], xs[:, i]]
            # prepend an undisturbed column
            # to distinguish between no disturbance (argmax = 0)
            # and disturbance occurs in the first column (argmax = 1)
            d_undisturbed = np.repeat(
                JRC_ANNUAL_CHANGE_UNDISTURBED, d_2019.shape
            )
            d = np.column_stack([d_undisturbed, d_2019, d_2020, d_2021, d_2022])

            # if there are multiple instances of the maximum,
            # argmax returns the first one.
            first_disturb_year = 2018 + np.argmax(
                d == JRC_ANNUAL_CHANGE_DEGRADATION,
                axis=1,
            )

            # Control: ((s1 < s2 < disturbance) | (disturbance < s1 < s2)) & (¬undisturbed)
            cs = np.logical_and(
                np.logical_or(
                    s2 < first_disturb_year,
                    s1 > first_disturb_year,
                ),
                first_disturb_year > 2019,
            )
            control[cs] += 1
            disturb_date[p_idx, cs] = first_disturb_year[cs]

            # Treatment: (s1 < disturbance) & (disturbance < s2)
            cs = np.logical_and(
                s1 < first_disturb_year, first_disturb_year < s2
            )
            measured[cs] += 1
            disturb_date[p_idx, cs] = first_disturb_year[cs]
            p_idx += 1
    return measured, control, disturb_date, valid


def _overlay_disturbance_no_deforestation(
    rst_2019: xr.DataArray,
    rst_2020: xr.DataArray,
    rst_2021: xr.DataArray,
    rst_2022: xr.DataArray,
    df: pd.DataFrame,
) -> Tuple[np.array, np.array, np.array]:
    xs = raster_utils.get_idxs_two_nearest(rst_2019.x.data, df.longitude.values)
    ys = raster_utils.get_idxs_two_nearest(rst_2019.y.data, df.latitude.values)
    # For now, discard any coords on the edge of the raster
    valid = np.logical_and.reduce(
        (
            (np.min(xs, axis=1) >= 0),
            (np.max(xs, axis=1) < rst_2019.shape[2]),
            (np.min(ys, axis=1) >= 0),
            (np.max(ys, axis=1) < rst_2019.shape[1]),
        )
    )
    xs = xs[valid]
    ys = ys[valid]
    df = df.loc[valid]

    s1 = df.t1_year.values
    s2 = df.t2_year.values
    measured = np.zeros(s1.shape[0])
    # control: there was a disturbance,
    # but the paired GEDI shots didn't happen to capture it.
    control = np.zeros(s1.shape[0])
    disturb_date = np.zeros((4, s1.shape[0]))
    p_idx = 0
    for i in [0, 1]:
        for j in [0, 1]:
            d_2019 = rst_2019.data[0, ys[:, j], xs[:, i]]
            d_2020 = rst_2020.data[0, ys[:, j], xs[:, i]]
            d_2021 = rst_2021.data[0, ys[:, j], xs[:, i]]
            d_2022 = rst_2022.data[0, ys[:, j], xs[:, i]]
            # prepend an undisturbed column
            # to distinguish between no disturbance (argmax = 0)
            # and disturbance occurs in the first column (argmax = 1)
            d_undisturbed = np.repeat(
                JRC_ANNUAL_CHANGE_UNDISTURBED, d_2019.shape
            )
            d = np.column_stack([d_undisturbed, d_2019, d_2020, d_2021, d_2022])

            # if there are multiple instances of the maximum,
            # argmax returns the first one.
            first_disturb_year = 2018 + np.argmax(
                d == JRC_ANNUAL_CHANGE_DEGRADATION,
                axis=1,
            )

            first_deforest_year = 2018 + np.argmax(
                d == JRC_ANNUAL_CHANGE_DEFORESTATION,
                axis=1,
            )

            # Control: ((s1 < s2 < disturbance) | (disturbance < s1 < s2)) &
            #  (¬undisturbed) & (s1 < s2 < deforest | deforest < s1 < s2)
            cs = np.logical_and(
                np.logical_or(
                    s2 < first_disturb_year,
                    s1 > first_disturb_year,
                ),
                np.logical_and(
                    np.logical_or(
                        s2 < first_deforest_year,
                        s1 > first_deforest_year,
                    ),
                    first_disturb_year > 2019,
                ),
            )
            control[cs] += 1
            disturb_date[p_idx, cs] = first_disturb_year[cs]

            # Treatment: (s1 < disturbance) & (disturbance < s2)
            # & (s1 < s2 < deforest | deforest < s1 < s2)
            cs = np.logical_and(
                np.logical_and(
                    s1 < first_disturb_year,
                    first_disturb_year < s2,
                ),
                np.logical_or(
                    s2 < first_deforest_year,
                    s1 > first_deforest_year,
                ),
            )
            measured[cs] += 1
            disturb_date[p_idx, cs] = first_disturb_year[cs]
            p_idx += 1
    return measured, control, disturb_date, valid


def _overlay_undisturbed(
    rst_2019: xr.DataArray,
    rst_2020: xr.DataArray,
    rst_2021: xr.DataArray,
    rst_2022: xr.DataArray,
    df: pd.DataFrame,
) -> Tuple[np.array, np.array, np.array]:
    xs = raster_utils.get_idx(rst_2019.x.data, df.longitude.values)
    ys = raster_utils.get_idx(rst_2019.y.data, df.latitude.values)
    # For now, discard any coords on the edge of the raster
    valid = np.logical_and.reduce(
        (
            (xs - 1 >= 0),
            (xs + 1 < rst_2019.shape[2]),
            (ys - 1 >= 0),
            (ys + 1 < rst_2019.shape[1]),
        )
    )
    xs = xs[valid]
    ys = ys[valid]
    df = df.loc[valid]

    uc = np.zeros((9, len(df)))
    p_idx = 0
    for i in range(-1, 2):
        for j in range(-1, 2):
            for rst in [rst_2019, rst_2020, rst_2021, rst_2022]:
                undisturbed = (
                    rst.data[0, ys + j, xs + i] == JRC_ANNUAL_CHANGE_UNDISTURBED
                )
                uc[p_idx] += undisturbed
            p_idx += 1
    return uc.sum(axis=0), valid
