import unittest
from unittest.mock import MagicMock, patch, mock_open
import tempfile

# Assuming the above code is defined in a module named processing_module
from flexprep.domain.processing import Processing

class TestProcessing(unittest.TestCase):
    def setUp(self):
        self.processing = Processing()
        self.file_objs = [
            {"step": "0", "processed": "N", "forecast_ref_time": "20230717T00Z", "key": "file1"},
            {"step": "0", "processed": "N", "forecast_ref_time": "20230717T00Z", "key": "file2"},
            {"step": "3", "processed": "N", "forecast_ref_time": "20230717T00Z", "key": "file3"},
        ]
    @patch('flexprep.domain.processing.grib_decoder.save')
    @patch('flexprep.domain.processing.metadata.extract_pv')
    @patch('flexprep.domain.processing.S3client.download_file')
    @patch('flexprep.domain.processing.config.set_values')
    @patch('flexprep.domain.processing.data_source.DataSource')
    @patch('flexprep.domain.processing.grib_decoder.load')
    @patch('flexprep.domain.processing.validate_dataset')
    @patch('flexprep.domain.processing.flx.fflexpart')
    @patch('flexprep.domain.processing.prepare_output')
    @patch('flexprep.domain.processing.os.unlink')
    def test_process(self, mock_unlink, mock_prepare_output, mock_fflexpart, mock_validate_dataset, mock_grib_decoder_load, mock_DataSource, mock_set_values, mock_download_file, mock_extract_pv, mock_grib_save):
        mock_download_file.side_effect = ["tempfile1", "tempfile2", "tempfile3", "tempfile4"]
        mock_grib_decoder_load.return_value = {"u": MagicMock(message="mock_message")}
        mock_fflexpart.return_value = {"field1": MagicMock(attrs={"v_coord": "hybrid"})}

        with patch('builtins.open', mock_open()) as mocked_file:
            self.processing.process(self.file_objs)

            # Assert that download_file was called for each file
            self.assertEqual(mock_download_file.call_count, 3)
            

            # Assert that grib_decoder.load was called once
            mock_grib_decoder_load.assert_called_once()

            # Assert that validate_dataset was called once
            mock_validate_dataset.assert_called_once()

            # Assert that extract_pv was called once
            mock_extract_pv.assert_called_once()

            # Assert that fflexpart was called once
            mock_fflexpart.assert_called_once()

            # Assert that prepare_output was called once
            mock_prepare_output.assert_called_once()

            # Assert that os.unlink was called for each tempfile
            self.assertEqual(mock_unlink.call_count, 3)

            # Assert that files were written to
            mock_grib_save.assert_called_once()
