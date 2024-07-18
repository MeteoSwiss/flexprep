import unittest
from datetime import datetime

import numpy as np
import xarray as xr

from flexprep.domain.validation_utils import validate_dataset


class TestValidateDataset(unittest.TestCase):

    def setUp(self):
        self.params = ["temperature", "pressure"]
        self.ref_time = datetime(2023, 7, 18)
        self.step = 6
        self.prev_step = 3

        # Create a valid dataset
        self.ds = {
            "temperature": xr.DataArray(
                np.random.rand(3),
                dims=["time"],
                coords={
                    "time": [0, 3, 6],
                    "ref_time": np.datetime64(self.ref_time, "ns"),
                },
            ),
            "pressure": xr.DataArray(
                np.random.rand(3),
                dims=["time"],
                coords={
                    "time": [0, 3, 6],
                    "ref_time": np.datetime64(self.ref_time, "ns"),
                },
            ),
        }

    def test_valid_dataset(self):
        # This should not raise any exceptions
        validate_dataset(self.ds, self.params, self.ref_time, self.step, self.prev_step)

    def test_missing_param(self):
        # Remove one parameter from the dataset
        ds_incomplete = self.ds.copy()
        del ds_incomplete["pressure"]

        with self.assertRaises(ValueError) as context:
            validate_dataset(
                ds_incomplete, self.params, self.ref_time, self.step, self.prev_step
            )
        self.assertEqual(
            str(context.exception),
            "Not all requested parameters are present in the dataset",
        )

    def test_incorrect_time_steps(self):
        # Modify time steps to be incorrect
        ds_incorrect_time = self.ds.copy()
        ds_incorrect_time["temperature"] = xr.DataArray(
            np.random.rand(2),
            dims=["time"],
            coords={"time": [0, 7], "ref_time": np.datetime64(self.ref_time, "ns")},
        )

        with self.assertRaises(ValueError) as context:
            validate_dataset(
                ds_incorrect_time, self.params, self.ref_time, self.step, self.prev_step
            )
        self.assertEqual(str(context.exception), "Downloaded steps are incorrect")

    def test_incorrect_ref_time(self):
        # Modify ref_time to be incorrect
        ds_incorrect_ref_time = self.ds.copy()
        ds_incorrect_ref_time["temperature"] = xr.DataArray(
            np.random.rand(3),
            dims=["time"],
            coords={
                "time": [0, 3, 6],
                "ref_time": np.datetime64(datetime(2023, 1, 1), "ns"),
            },
        )

        with self.assertRaises(ValueError) as context:
            validate_dataset(
                ds_incorrect_ref_time,
                self.params,
                self.ref_time,
                self.step,
                self.prev_step,
            )
        self.assertEqual(
            str(context.exception), "The forecast reference time is incorrect"
        )
