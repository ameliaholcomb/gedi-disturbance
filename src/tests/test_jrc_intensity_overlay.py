import pandas as pd
import numpy as np
import unittest
import xarray as xr

from src.data import jrc_intensity_parser


class TestCase(unittest.TestCase):
    def _make_intensity_data(self, disturb_data, valid_data):
        d_disturb = xr.DataArray(
            data=disturb_data,
            dims=["dtype", "y", "x"],
            coords={
                "y": np.arange(disturb_data.shape[1]),
                "x": np.arange(disturb_data.shape[2]),
            },
        )
        d_valid = xr.DataArray(
            data=valid_data,
            dims=["dtype", "y", "x"],
            coords={
                "y": np.arange(disturb_data.shape[1]),
                "x": np.arange(disturb_data.shape[2]),
            },
        )
        return d_disturb, d_valid

    def test_intensity_1px(self):
        d = np.zeros((4, 6, 7))  # 4 years, 6 lon, 7 lat
        v = np.zeros((4, 6, 7))  # 4 years, 6 lon, 7 lat
        # Disturbance in 2020 at (2,2), intensifying in 2021
        d[1, 2, 2] = 12
        v[1, 2, 2] = 32
        d[2, 2, 2] = 16
        v[2, 2, 2] = 32

        rsts = self._make_intensity_data(d, v)
        df = pd.DataFrame(
            {
                "t1_year": [2019, 2019, 2019],
                "t2_year": [2022, 2022, 2022],
                # third point is on raster edge
                "latitude": [2.4, 5.1, 5.7],
                "longitude": [2.4, 1.1, 4.5],
            }
        )

        intensity, non_edge = jrc_intensity_parser._overlay_intensity(*rsts, df)
        intensity_correct = np.array([0.375, 0])
        print(intensity)
        print(non_edge)
        # self.assertTrue(np.all(intensity == intensity_correct))
        # non_edge_correct = np.array([True, True, False])
        # self.assertTrue(np.all(non_edge == non_edge_correct))

    def test_intensity_3px(self):
        d = np.zeros((4, 6, 7))  # 4 years, 6 lon, 7 lat
        v = np.zeros((4, 6, 7))  # 4 years, 6 lon, 7 lat
        # Disturbance starting in 2020 at (2,2)
        d[1:, 2, 2] = 12
        v[1:, 2, 2] = 32
        # Disturbance in 2021 at (3,3)
        d[2, 3, 3] = 10
        v[2, 3, 3] = 25
        # Disturbance in 2022 at (1, 2)
        d[3, 1, 2] = 4
        v[3, 1, 2] = 8

        rsts = self._make_intensity_data(d, v)
        df = pd.DataFrame(
            {
                "t1_year": np.zeros(3),  # unused
                "t2_year": np.zeros(3),  # unused
                "p1_disturb_date": [2020, 2021, 0],
                "p2_disturb_date": [0, 0, 2020],
                "p3_disturb_date": [2022, 0, 0],
                "p4_disturb_date": [0, 2020, 2022],
                # p_i in y,x format:
                # 1: [2,2], [2,1], [1,2], [1,1]
                # 2: [3,3], [3,2], [2,3], [2,2]
                # 3: [2,1], [2,2], [1,1], [1,2]
                "latitude": [2.4, 3.1, 2.1],
                "longitude": [2.4, 3.1, 1.5],
            }
        )

        intensity, non_edge = jrc_intensity_parser._overlay_intensity(*rsts, df)
        intensity_correct = np.array([0.875, 0.775, 0.875])
        self.assertTrue(np.all(intensity == intensity_correct))
        non_edge_correct = np.array([True, True, True])
        self.assertTrue(np.all(non_edge == non_edge_correct))


suite = unittest.TestLoader().loadTestsFromTestCase(TestCase)
