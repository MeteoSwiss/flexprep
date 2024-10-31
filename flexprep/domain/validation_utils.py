import typing
from datetime import datetime

import numpy as np
import pandas as pd


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
        (
            np.array_equal(
                array.coords["lead_time"].values,
                [pd.to_timedelta(0, "h").value],
            )
        )
        or (
            prev_step == 0
            and np.array_equal(
                array.coords["lead_time"].values,
                pd.to_timedelta([0, step], "h").values,
            )
        )
        or (
            prev_step != 0
            and np.array_equal(
                array.coords["lead_time"].values,
                pd.to_timedelta([0, prev_step, step], "h").values,
            )
        )
        for array in ds.values()
    ):
        raise ValueError("Downloaded steps are incorrect")

    if not all(
        (array.coords["ref_time"].values == np.datetime64(ref_time, "ns"))
        for array in ds.values()
    ):
        raise ValueError("The forecast reference time is incorrect")
