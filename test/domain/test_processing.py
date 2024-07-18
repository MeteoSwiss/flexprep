import logging
import unittest
from io import StringIO

from flexprep.domain.processing import Processing


class TestSortAndDownloadFiles(unittest.TestCase):

    def setUp(self):
        # Initialize any necessary objects or setup here
        self.obj = Processing()  # Initialize YourClass instance

        # Set up a StringIO object to capture logging output
        self.log_capture = StringIO()
        self.log_handler = logging.StreamHandler(self.log_capture)
        self.log_handler.setLevel(logging.ERROR)
        logging.getLogger().addHandler(self.log_handler)

    def test_sorted_files_length_less_than_3(self):
        # Create file objects with length less than 3

        file_objs = [
            self.obj.FileObject(step="1", filename="file1"),
            self.obj.FileObject(step="2", filename="file2"),
        ]

        self.obj._sort_and_download_files(file_objs)

        # Check the captured log output
        self.log_handler.flush()
        log_contents = self.log_capture.getvalue().strip()

        # Verify the logged message
        self.assertIn(
            "Sorting and validation failed: Not enough files for pre-processing",
            log_contents,
        )
