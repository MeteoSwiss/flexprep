from unittest.mock import MagicMock, patch

import pytest

from flexprep.domain.data_model import IFSForecast
from flexprep.domain.prepare_processing import launch_pre_processing


@pytest.mark.parametrize(
    "query_return_value, test_forecast_obj, expected_call_args, expected_call_count",
    [
        # Case 1: Enough zero steps
        (
            [
                IFSForecast("202406180000", 0, "loc1", False),
                IFSForecast("202406180000", 0, "loc2", False),
                IFSForecast("202406180000", 3, "loc2", False),
            ],
            IFSForecast("202406180000", 3, "loc3", False),
            [
                {
                    "forecast_ref_time": "202406180000",
                    "step": 0,
                    "location": "loc1",
                    "processed": False,
                },
                {
                    "forecast_ref_time": "202406180000",
                    "step": 0,
                    "location": "loc2",
                    "processed": False,
                },
                {
                    "forecast_ref_time": "202406180000",
                    "step": 3,
                    "location": "loc3",
                    "processed": False,
                },
            ],
            1,
        ),
        # Case 2: Not enough zero steps
        (
            [
                IFSForecast("202406180000", 0, "loc1", False),
                IFSForecast("202406180000", 3, "loc2", False),
            ],
            IFSForecast("202406180000", 3, "loc3", False),
            [
                {
                    "forecast_ref_time": "202406180000",
                    "step": 0,
                    "location": "loc1",
                    "processed": False,
                },
                {
                    "forecast_ref_time": "202406180000",
                    "step": 3,
                    "location": "loc3",
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
