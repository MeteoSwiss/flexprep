from datetime import datetime
from unittest.mock import MagicMock, patch

from flexprep.domain.prepare_processing import (
    aggregate_s3_objects,
    launch_pre_processing,
)


def test_aggregate_s3_objects():

    objects = {
        "Contents": [
            {"Key": "P1S06180000061800011"},
            {"Key": "P1S06180300061803001"},
            {"Key": "P1D06180000061800001"},
        ]
    }

    result = aggregate_s3_objects(objects)

    expected_result = [
        {
            "key": "P1S06180000061800011",
            "forecast_ref_time": datetime(2024, 6, 18, 0, 0),
            "step": 0,
            "processed": "N",
        },
        {
            "key": "P1D06180000061800001",
            "forecast_ref_time": datetime(2024, 6, 18, 0, 0),
            "step": 0,
            "processed": "N",
        },
        {
            "key": "P1S06180300061803001",
            "forecast_ref_time": datetime(2024, 6, 18, 3, 0),
            "step": 0,
            "processed": "N",
        },
    ]

    assert result == expected_result


@patch("flexprep.domain.prepare_processing.Processing")
def test_launch_pre_processing(MockProcessing):
    mock_processing_instance = MockProcessing.return_value
    mock_processing_instance.process = MagicMock()

    objects = {
        "Contents": [
            {"Key": "P1S06180000061800011"},
            {"Key": "P1S06180000061803001"},
            {"Key": "P1D06180000061800001"},
        ]
    }

    launch_pre_processing(objects)

    # Verify that the process method was called the expected number of times
    assert mock_processing_instance.process.call_count == 1
    # Verify that the process method was called with the expected arguments
    # Expected call arguments
    expected_call_args = [
        {
            "key": "P1S06180000061800011",
            "forecast_ref_time": datetime(2024, 6, 18, 0, 0),
            "step": 0,
            "processed": "N",
        },
        {
            "key": "P1D06180000061800001",
            "forecast_ref_time": datetime(2024, 6, 18, 0, 0),
            "step": 0,
            "processed": "N",
        },
        {
            "key": "P1S06180000061803001",
            "forecast_ref_time": datetime(2024, 6, 18, 0, 0),
            "step": 3,
            "processed": "N",
        },
    ]

    # Extract actual call arguments
    actual_call_args = mock_processing_instance.process.call_args[0][0]

    # Verify that the process method was called with the expected arguments
    assert actual_call_args == expected_call_args
