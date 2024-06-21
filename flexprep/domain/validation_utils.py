import typing
from datetime import datetime

import numpy as np


def validate_dataset(
    ds: dict[str, typing.Any],
    params: list[str],
    ref_time: datetime,
    step: int,
    prev_step: int,
) -> None:
    """Validate the dataset to ensure it contains the required param and timesteps."""
    if not all(param in ds.keys() for param in params):
        raise ValueError("Not all requested parameters are present in the dataset")
    if not all(
        (np.array_equal(array.coords["time"].values, [0]))
        or (prev_step == 0 and np.array_equal(array.coords["time"].values, [0, step]))
        or (
            prev_step != 0
            and np.array_equal(array.coords["time"].values, [0, prev_step, step])
        )
        for array in ds.values()
    ):
        raise ValueError("Downloaded steps are incorrect")
    if not all(
        (array.coords["ref_time"].values == np.datetime64(ref_time, "ns"))
        for array in ds.values()
    ):
        raise ValueError("The forecast reference time is incorrect")
