import pytest
from datetime import datetime
from flexprep.domain.prepare_processing import PrepProcessing
from flexprep.domain.processing import Processing
import unittest
from unittest.mock import patch, MagicMock


def test_aggregate_s3_objects():
    
    objects = {
        "Contents": [
            {"Key": "P1S06180000061800011"},
            {"Key": "P1S06180300061803001"},
            {"Key": "P1D06180000061800001"},
        ]
    }
    
    prep_processing = PrepProcessing()
    result = prep_processing.aggregate_s3_objects(objects)
    
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
        }
    ]
    
    assert result == expected_result

class TestLaunchPreProcessing(unittest.TestCase):
    @patch('flexprep.domain.prepare_processing.Processing')
    def test_launch_pre_processing(self, MockProcessing):
        mock_processing_instance = MockProcessing.return_value
        mock_processing_instance.process = MagicMock()

        obj = PrepProcessing()

        objects = {
            "Contents": [
                {"Key": "P1S06180000061800011"},
                {"Key": "P1S06180000061803001"},
                {"Key": "P1D06180000061800001"},
            ]
        }

        # Call the method to be tested
        obj.launch_pre_processing(objects)

        # Verify that the process method was called the expected number of times
        self.assertEqual(mock_processing_instance.process.call_count, 1)
        # Verify that the process method was called with the expected arguments
        mock_processing_instance.process.assert_called_with([
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
        }
    ])
