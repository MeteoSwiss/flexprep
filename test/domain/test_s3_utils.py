import os
import unittest
from unittest.mock import patch

import boto3
from moto import mock_aws

from flexprep.domain.s3_utils import S3client


class TestS3client(unittest.TestCase):

    @mock_aws
    def setUp(self):
        self.s3_client = S3client()
        self.s3 = boto3.client("s3", region_name="us-east-1")
        self.bucket_name = "test-bucket"
        self.s3.create_bucket(Bucket=self.bucket_name)

    @mock_aws
    @patch.dict(
        os.environ,
        {
            "S3INPUT_ACCESS_KEY": "fake_access_key",
            "S3INPUT_SECRET_KEY": "fake_secret_key",
        },
    )
    def test_create_s3_client(self):
        client = self.s3_client._create_s3_client(
            endpoint_url="https://s3.amazonaws.com",
            access_key="fake_access_key",
            secret_key="fake_secret_key",
        )
        self.assertEqual(client.meta.service_model.service_name, "s3")

    # TODO: Create tests for other s3 utils
