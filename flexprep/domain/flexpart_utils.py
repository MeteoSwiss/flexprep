import logging
import typing
from typing import Any

FileObject = dict[str, Any]


def prepare_output(
    ds_out: dict[str, typing.Any],
    ds_in: dict[str, typing.Any],
    input_fields: set,
    constant_fields: set,
) -> None:
    """Prepare the output dataset."""
    missing_fields = (ds_in.keys() & input_fields) - {"etadot"} - ds_out.keys()

    missing_const = (ds_in.keys() & constant_fields) - ds_out.keys()

    for field in ds_out:
        ds_out[field] = ds_out[field].isel(time=slice(-1, None)).squeeze()
    for field in missing_fields:
        logging.warning(f"Field '{field}' not found in output")
        ds_out[field] = ds_in[field].isel(time=slice(-1, None)).squeeze()
    for field in missing_const:
        ds_out[field] = ds_in[field].squeeze()

    ds_out["etadot"] = ds_out.pop("omega")
    ds_out["cp"] = ds_out["cp"] * 1000
    ds_out["lsp"] = ds_out["lsp"] * 100
