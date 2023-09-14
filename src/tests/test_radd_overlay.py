import pandas as pd
import numpy as np
import unittest
import xarray as xr

from src.data import radd_parser


class TestRaddParser(unittest.TestCase):
    def _make_radd_data(self, disturb_data):
        conf = np.zeros_like(disturb_data)
        return xr.DataArray(
            data=np.stack([conf, disturb_data], axis=0),
            dims=["dtype", "y", "x"],
            coords={
                "y": np.arange(disturb_data.shape[0]),
                "x": np.arange(disturb_data.shape[1]),
            },
        )

    def test_treat_1px(self):
        # Dates: a < b < c < ... etc.
        a = 19001
        b = 19002
        c = 20001
        d = 20002
        e = 22000
        rst = self._make_radd_data(
            np.array(
                # x ----------------> (0-6, len 7)
                [
                    [0, 0, 0, 0, 0, 0, 0],
                    [0, 0, 0, 0, 0, 0, 0],
                    [0, 0, c, 0, 0, 0, 0],
                    [0, 0, 0, 0, 0, 0, 0],
                    [0, 0, 0, 0, 0, 0, 0],
                    [0, 0, 0, 0, 0, 0, 0],
                ]
            )
        )
        # The first pair is centered at c (2,2)
        # and is treatment because a < c < d
        # The second pair is on an invalid (edge) pixel and is omitted
        # The third pair is centered on an an area of no disturbance
        df = pd.DataFrame(
            {
                "t1_yydoy": [a, a, a],
                "t2_yydoy": [d, b, e],
                "latitude": [2.5, 5.1, 4.3],
                "longitude": [2.5, 1.1, 4.5],
            }
        )
        treat, ctrl, date = radd_parser._overlay_disturbance_testable(rst, df)
        self.assertTrue(np.all(treat == np.array([1.0, 0.0])))
        self.assertTrue(np.all(ctrl == np.array([0.0, 0.0])))
        date_correct = np.zeros((9, 2))
        date_correct[4, 0] = c
        self.assertTrue(np.all(date == date_correct))

    def test_treat_2px(self):
        # Dates: a < b < c < ... etc.
        a = 19001
        b = 19002
        c = 19364
        d = 20001
        e = 20002
        f = 22000
        rst = self._make_radd_data(
            np.array(
                # x ----------------> (0-6, len 7)
                [
                    [0, 0, 0, 0, 0, 0, 0],
                    [0, 0, 0, 0, 0, 0, 0],
                    [0, 0, d, c, 0, 0, 0],
                    [0, 0, 0, 0, 0, 0, 0],
                    [0, 0, 0, 0, 0, 0, 0],
                    [0, 0, 0, 0, 0, 0, 0],
                ]
            )
        )
        # The first pair is centered at d (2,2)
        # and is treatment because a < d < e
        # The window also has c (3,2), and a < c < e
        # The other pairs are centered on invalid (edge) pixels and are omitted
        df = pd.DataFrame(
            {
                "t1_yydoy": [a, a, a],
                "t2_yydoy": [e, b, f],
                "latitude": [2.5, 5.1, 5.7],
                "longitude": [2.5, 1.1, 4.5],
            }
        )
        treat, ctrl, date = radd_parser._overlay_disturbance_testable(rst, df)
        self.assertTrue(np.all(treat == np.array([2.0])))
        self.assertTrue(np.all(ctrl == np.array([0.0])))
        date_correct = np.zeros((9, 1))
        date_correct[4] = d
        date_correct[7] = c
        self.assertTrue(np.all(date == date_correct))

    def test_control_1px(self):
        # Dates: a < b < c < ... etc.
        a = 19001
        b = 19002
        c = 20001
        d = 20002
        e = 22000
        rst = self._make_radd_data(
            np.array(
                # x ----------------> (0-6, len 7)
                [
                    [0, 0, 0, 0, 0, 0, 0],
                    [0, 0, 0, 0, 0, 0, 0],
                    [0, 0, c, 0, 0, 0, 0],
                    [0, 0, 0, 0, 0, 0, 0],
                    [0, 0, 0, 0, 0, 0, 0],
                    [0, 0, 0, 0, 0, 0, 0],
                ]
            )
        )
        # The first pair is centered at c (2,2)
        # and is control because a < b < c
        # The other pairs are centered on invalid (edge) pixels and are omitted
        df = pd.DataFrame(
            {
                "t1_yydoy": [a, a, a],
                "t2_yydoy": [b, b, e],
                "latitude": [2.5, 5.1, 5.7],
                "longitude": [2.5, 1.1, 4.5],
            }
        )
        treat, ctrl, date = radd_parser._overlay_disturbance_testable(rst, df)
        self.assertTrue(np.all(treat == np.array([0.0])))
        self.assertTrue(np.all(ctrl == np.array([1.0])))
        date_correct = np.zeros((9, 1))
        date_correct[4] = c
        self.assertTrue(np.all(date == date_correct))

    def test_control_2px(self):
        # Dates: a < b < c < ... etc.
        a = 19001
        b = 19002
        c = 19364
        d = 20001
        e = 20002
        f = 22000
        rst = self._make_radd_data(
            np.array(
                # x ----------------> (0-6, len 7)
                [
                    [0, 0, 0, 0, 0, 0, 0],
                    [0, 0, 0, 0, 0, 0, 0],
                    [0, 0, d, a, 0, 0, 0],
                    [0, 0, 0, 0, 0, 0, 0],
                    [0, 0, 0, 0, 0, 0, 0],
                    [0, 0, 0, 0, 0, 0, 0],
                ]
            )
        )
        # The first pair is centered at d (2,2)
        # and is control because b < c < d
        # The window also has c (3,2), and a < b < c
        # The other pairs are centered on invalid (edge) pixels and are omitted
        df = pd.DataFrame(
            {
                "t1_yydoy": [b, b, b],
                "t2_yydoy": [c, c, f],
                "latitude": [2.5, 5.1, 5.7],
                "longitude": [2.5, 1.1, 4.5],
            }
        )
        treat, ctrl, date = radd_parser._overlay_disturbance_testable(rst, df)
        self.assertTrue(np.all(treat == np.array([0.0])))
        self.assertTrue(np.all(ctrl == np.array([2.0])))
        date_correct = np.zeros((9, 1))
        date_correct[4] = d
        date_correct[7] = a
        self.assertTrue(np.all(date == date_correct))

    def test_control_treat(self):
        # Dates: a < b < c < ... etc.
        a = 19001
        b = 19002
        c = 19364
        d = 20001
        e = 20002
        f = 21001
        g = 22000
        h = 22001
        rst = self._make_radd_data(
            np.array(
                # x  0  1  2  3  4  5  6    # y
                [
                    [0, 0, 0, 0, 0, 0, 0],  # 0
                    [0, 0, 0, 0, 0, 0, 0],  # 1
                    [0, 0, d, f, h, 0, 0],  # 2
                    [0, 0, 0, 0, c, 0, 0],  # 3
                    [0, 0, 0, 0, 0, 0, 0],  # 4
                    [0, 0, 0, 0, 0, 0, 0],  # 5
                ]
            )
        )
        df = pd.DataFrame(
            {
                "t1_yydoy": [a, a, a],
                "t2_yydoy": [e, b, g],
                "latitude": [2.5, 3.1, 2.7],
                "longitude": [2.5, 3.0, 4.5],
            }
        )
        # Pair 1: (y=2,x=2)
        # treatment 1 because a < d < e
        # control 1 because a < e < f
        # Pair 2: (y=3,x=3)
        # treatment 0 (no disturbance between a and b)
        # control 4 because a < b < c,d,f,h
        # Pair 3: (y=2,x=4)
        # treatment 2 because a < c,f < g
        # control 1 because a < g < h
        treat, ctrl, date = radd_parser._overlay_disturbance_testable(rst, df)
        self.assertTrue(np.all(treat == np.array([1.0, 0.0, 2.0])))
        self.assertTrue(np.all(ctrl == np.array([1.0, 4.0, 1.0])))
        date_correct = np.zeros((9, 3))
        date_correct[4, 0] = d
        date_correct[7, 0] = f
        date_correct[0, 1] = d
        date_correct[3, 1] = f
        date_correct[6, 1] = h
        date_correct[7, 1] = c
        date_correct[0, 2] = f
        date_correct[3, 2] = h
        date_correct[4, 2] = c
        self.assertTrue(np.all(date == date_correct))


suite = unittest.TestLoader().loadTestsFromTestCase(TestRaddParser)
