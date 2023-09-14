import pandas as pd
import numpy as np
import unittest
import xarray as xr

from src.data import jrc_overlays


class TestJrcOverlays(unittest.TestCase):
    def _make_jrc_data(self, disturb_data):
        d_2019 = xr.DataArray(
            data=disturb_data[0, :, :].reshape(1, *disturb_data.shape[1:]),
            dims=["dtype", "y", "x"],
            coords={
                "y": np.arange(disturb_data.shape[1]),
                "x": np.arange(disturb_data.shape[2]),
            },
        )
        d_2020 = xr.DataArray(
            data=disturb_data[1, :, :].reshape(1, *disturb_data.shape[1:]),
            dims=["dtype", "y", "x"],
            coords={
                "y": np.arange(disturb_data.shape[1]),
                "x": np.arange(disturb_data.shape[2]),
            },
        )
        d_2021 = xr.DataArray(
            data=disturb_data[2, :, :].reshape(1, *disturb_data.shape[1:]),
            dims=["dtype", "y", "x"],
            coords={
                "y": np.arange(disturb_data.shape[1]),
                "x": np.arange(disturb_data.shape[2]),
            },
        )
        d_2022 = xr.DataArray(
            data=disturb_data[3, :, :].reshape(1, *disturb_data.shape[1:]),
            dims=["dtype", "y", "x"],
            coords={
                "y": np.arange(disturb_data.shape[1]),
                "x": np.arange(disturb_data.shape[2]),
            },
        )
        return d_2019, d_2020, d_2021, d_2022

    def test_treat_1px(self):
        a = np.zeros((4, 6, 7))  # 4 years, 6 lon, 7 lat
        # Disturbance starting in 2020 at (2,2)
        a[1:3, 2, 2] = jrc_overlays.JRC_ANNUAL_CHANGE_DEGRADATION

        rsts = self._make_jrc_data(a)
        df = pd.DataFrame(
            {
                "t1_year": [2019, 2019, 2019],
                "t2_year": [2021, 2021, 2021],
                "latitude": [2.5, 5.1, 5.7],
                "longitude": [2.5, 1.1, 4.5],
            }
        )
        treat, ctrl, date = jrc_overlays._overlay_disturbance(*rsts, df)
        self.assertTrue(np.all(treat == np.array([1.0, 0.0])))
        self.assertTrue(np.all(ctrl == np.array([0.0, 0.0])))
        date_correct = np.zeros((4, 2))
        date_correct[0, 0] = 2020
        self.assertTrue(np.all(date == date_correct))

    def test_treat_2px(self):
        a = np.zeros((4, 6, 7))  # 4 years, 6 lon, 7 lat
        # Disturbance starting in 2020 at (2,2)
        a[1:3, 2, 2] = jrc_overlays.JRC_ANNUAL_CHANGE_DEGRADATION
        # Disturbance in 2021 at (3,3)
        a[2, 3, 3] = jrc_overlays.JRC_ANNUAL_CHANGE_DEGRADATION

        rsts = self._make_jrc_data(a)
        df = pd.DataFrame(
            {
                "t1_year": [2019, 2019, 2019],
                "t2_year": [2021, 2021, 2022],
                "latitude": [2.5, 3.9, 3.1],
                "longitude": [2.5, 4.2, 3.1],
            }
        )
        treat, ctrl, date = jrc_overlays._overlay_disturbance(*rsts, df)
        self.assertTrue(np.all(treat == np.array([1.0, 0.0, 2.0])))
        self.assertTrue(np.all(ctrl == np.array([0.0, 0.0, 0.0])))
        date_correct = np.zeros((4, 3))
        date_correct[0, 0] = 2020
        date_correct[0, 2] = 2021
        date_correct[3, 2] = 2020
        self.assertTrue(np.all(date == date_correct))

    def test_control_1px(self):
        a = np.zeros((4, 6, 7))  # 4 years, 6 lon, 7 lat
        # Disturbance starting in 2021 at (2,2)
        a[2:3, 2, 2] = jrc_overlays.JRC_ANNUAL_CHANGE_DEGRADATION

        rsts = self._make_jrc_data(a)
        df = pd.DataFrame(
            {
                "t1_year": [2019, 2019, 2019],
                "t2_year": [2020, 2021, 2021],
                "latitude": [2.5, 2.5, 5.7],
                "longitude": [2.5, 2.1, 4.5],
            }
        )
        treat, ctrl, date = jrc_overlays._overlay_disturbance(*rsts, df)
        self.assertTrue(np.all(treat == np.array([0.0, 0.0])))
        self.assertTrue(np.all(ctrl == np.array([1.0, 0.0])))
        date_correct = np.zeros((4, 2))
        date_correct[0, 0] = 2021
        self.assertTrue(np.all(date == date_correct))

    def test_control_2px(self):
        a = np.zeros((4, 6, 7))  # 4 years, 6 lon, 7 lat
        # Disturbance starting in 2020 at (2,2)
        a[1:3, 2, 2] = jrc_overlays.JRC_ANNUAL_CHANGE_DEGRADATION
        # Disturbance in 2019 at (3,3) # should not be counted
        a[0, 3, 3] = jrc_overlays.JRC_ANNUAL_CHANGE_DEGRADATION
        # Disturbance in 2021 at (2,3)
        a[2, 2, 3] = jrc_overlays.JRC_ANNUAL_CHANGE_DEGRADATION

        rsts = self._make_jrc_data(a)
        df = pd.DataFrame(
            {
                "t1_year": [2021, 2019, 2022],
                "t2_year": [2022, 2021, 2022],
                "latitude": [2.5, 3.9, 3.1],
                "longitude": [2.5, 4.2, 3.1],
            }
        )
        treat, ctrl, date = jrc_overlays._overlay_disturbance(*rsts, df)
        self.assertTrue(np.all(treat == np.array([0.0, 0.0, 0.0])))
        self.assertTrue(np.all(ctrl == np.array([1.0, 0.0, 2.0])))
        date_correct = np.zeros((4, 3))
        date_correct[0, 0] = 2020
        date_correct[1, 2] = 2021
        date_correct[3, 2] = 2020
        self.assertTrue(np.all(date == date_correct))


suite = unittest.TestLoader().loadTestsFromTestCase(TestJrcOverlays)
