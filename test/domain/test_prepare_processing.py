from datetime import datetime as dt
from unittest.mock import MagicMock, patch

import pytest

from flexprep.domain.data_model import IFSForecast
from flexprep.domain.prepare_processing import launch_pre_processing

forecast_ref_time = dt.strptime("202406180000", "%Y%m%d%H%M")


@pytest.mark.parametrize(
    "query_return_value, test_forecast_obj, expected_call_args, expected_call_count",
    [
        # Case 1: Enough zero steps
        (
            [
                IFSForecast(forecast_ref_time, 0, "key1", False),
                IFSForecast(forecast_ref_time, 0, "key2", False),
                IFSForecast(forecast_ref_time, 3, "key3", False),
            ],
            IFSForecast(forecast_ref_time, 3, "key3", False),
            [
                {
                    "forecast_ref_time": forecast_ref_time,
                    "step": 0,
                    "key": "key1",
                    "processed": False,
                },
                {
                    "forecast_ref_time": forecast_ref_time,
                    "step": 0,
                    "key": "key2",
                    "processed": False,
                },
                {
                    "forecast_ref_time": forecast_ref_time,
                    "step": 3,
                    "key": "key3",
                    "processed": False,
                },
            ],
            1,
        ),
        # Case 2: Not enough zero steps
        (
            [
                IFSForecast(forecast_ref_time, 0, "key1", False),
                IFSForecast(forecast_ref_time, 3, "key2", False),
            ],
            IFSForecast(forecast_ref_time, 3, "key2", False),
            [
                {
                    "forecast_ref_time": forecast_ref_time,
                    "step": 0,
                    "key": "key1",
                    "processed": False,
                },
                {
                    "forecast_ref_time": forecast_ref_time,
                    "step": 3,
                    "key": "key2",
                    "processed": False,
                },
            ],
            0,
        ),
    ],
)
@patch("flexprep.domain.prepare_processing.DB")
@patch("flexprep.domain.prepare_processing.Processing")
def test_launch_pre_processing(
    MockProcessing,
    MockDB,
    query_return_value,
    test_forecast_obj,
    expected_call_args,
    expected_call_count,
):
    # Mock the database interactions
    mock_db_instance = MockDB.return_value
    mock_db_instance.query_table.return_value = query_return_value

    # Mock the Processing class
    mock_processing_instance = MockProcessing.return_value
    mock_processing_instance.process = MagicMock()

    # Call the function
    launch_pre_processing(test_forecast_obj)

    # Verify that the process method was called with the expected arguments
    if expected_call_count > 0:
        actual_call_args = mock_processing_instance.process.call_args[0][0]
        assert actual_call_args == expected_call_args

    # Verify the number of times process was called
    assert mock_processing_instance.process.call_count == expected_call_count
