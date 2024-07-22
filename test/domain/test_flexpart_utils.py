import pytest
from unittest.mock import MagicMock

from flexprep.domain.flexpart_utils import prepare_output
from flexprep.domain.processing import CONSTANTS, INPUT_FIELDS


@pytest.fixture
def setup_data():
    ds_out = {field: MagicMock() for field in INPUT_FIELDS}
    ds_out["omega"] = MagicMock()
    ds_in = {field: MagicMock() for field in list(CONSTANTS | INPUT_FIELDS)}
    return ds_out, ds_in

def test_prepare_output_missing_fields(setup_data):
    ds_out, ds_in = setup_data
    del ds_out["u"]

    prepare_output(ds_out, ds_in, INPUT_FIELDS, CONSTANTS)

    # Check that ds_out now contains input_fields + constants
    assert set(CONSTANTS | INPUT_FIELDS) == set(ds_out.keys())
