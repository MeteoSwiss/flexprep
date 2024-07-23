from datetime import datetime

import numpy as np
import pytest
import xarray as xr

from flexprep.domain.validation_utils import validate_dataset


@pytest.fixture
def setup_data():
    params = ["temperature", "pressure"]
    ref_time = datetime(2023, 7, 18)
    step = 6
    prev_step = 3

    # Create a valid dataset
    ds = {
        "temperature": xr.DataArray(
            np.random.rand(3),
            dims=["time"],
            coords={
                "time": [0, 3, 6],
                "ref_time": np.datetime64(ref_time, "ns"),
            },
        ),
        "pressure": xr.DataArray(
            np.random.rand(3),
            dims=["time"],
            coords={
                "time": [0, 3, 6],
                "ref_time": np.datetime64(ref_time, "ns"),
            },
        ),
    }
    return ds, params, ref_time, step, prev_step


def test_valid_dataset(setup_data):
    ds, params, ref_time, step, prev_step = setup_data
    # This should not raise any exceptions
    validate_dataset(ds, params, ref_time, step, prev_step)


def test_missing_param(setup_data):
    ds, params, ref_time, step, prev_step = setup_data
    # Remove one parameter from the dataset
    del ds["pressure"]

    with pytest.raises(
        ValueError, match="Not all requested parameters are present in the dataset"
    ):
        validate_dataset(ds, params, ref_time, step, prev_step)


def test_incorrect_time_steps(setup_data):
    ds, params, ref_time, step, prev_step = setup_data
    # Modify time steps to be incorrect
    ds["temperature"] = xr.DataArray(
        np.random.rand(2),
        dims=["time"],
        coords={"time": [0, 7], "ref_time": np.datetime64(ref_time, "ns")},
    )

    with pytest.raises(ValueError, match="Downloaded steps are incorrect"):
        validate_dataset(ds, params, ref_time, step, prev_step)


def test_incorrect_ref_time(setup_data):
    ds, params, ref_time, step, prev_step = setup_data
    # Modify ref_time to be incorrect
    ds["temperature"] = xr.DataArray(
        np.random.rand(3),
        dims=["time"],
        coords={
            "time": [0, 3, 6],
            "ref_time": np.datetime64(datetime(2023, 1, 1), "ns"),
        },
    )

    with pytest.raises(ValueError, match="The forecast reference time is incorrect"):
        validate_dataset(ds, params, ref_time, step, prev_step)
