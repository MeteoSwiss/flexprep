"""
Services providing core functionality.
"""

import logging

from pilotecmwf_pp_starter import CONFIG
from pilotecmwf_pp_starter.domain.prepare_processing import PrepProcessing
from pilotecmwf_pp_starter.domain.s3_utils import S3client

logger = logging.getLogger(__name__)


def _check_s3input_content(s3_client, bucket_name: str) -> bool:
    try:
        s3_objects = s3_client.list_objects_v2(Bucket=bucket_name)
        if "Contents" not in s3_objects:
            logger.error(f"No objects found in bucket {bucket_name}")
            return False
        return True
    except Exception as e:
        logger.error(f"Error checking S3 bucket content: {e}")
        return False


def process():
    logger.debug("Initialize Processing")

    s3_input = S3client().s3_client_input
    bucket_name = CONFIG.main.s3_buckets.input.name

    if not _check_s3input_content(s3_input, bucket_name):
        logger.error("S3 input content check failed. Aborting processing.")
        return

    s3_objects = s3_input.list_objects_v2(Bucket=bucket_name)

    PrepProcessing().launch_pre_processing(s3_objects)
