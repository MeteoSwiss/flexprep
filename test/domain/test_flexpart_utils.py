import unittest
from unittest.mock import MagicMock

from flexprep.domain.flexpart_utils import prepare_output
from flexprep.domain.processing import CONSTANTS, INPUT_FIELDS


class TestPrepareOutput(unittest.TestCase):

    def setUp(self):

        self.ds_out = {field: MagicMock() for field in INPUT_FIELDS}
        self.ds_out["omega"] = MagicMock()
        self.ds_in = {field: MagicMock() for field in list(CONSTANTS | INPUT_FIELDS)}

    def test_prepare_output_missing_fields(self):
        del self.ds_out["u"]
        prepare_output(self.ds_out, self.ds_in, INPUT_FIELDS, CONSTANTS)
        # Check that ds_out now contains input_fields + constants
        self.assertSetEqual(set(CONSTANTS | INPUT_FIELDS), set(self.ds_out.keys()))
