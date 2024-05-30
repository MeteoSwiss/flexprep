"""
Process unprocessed IFS data using meteodatalab.
Author: Nina Burgdorfer
Date: 03.04.2024
"""
import logging  
import tempfile
import json
from pathlib import Path
import time
import boto3
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig 
import subprocess
import os
import eccodes
import numpy as np
import xarray as xr
import yaml
from importlib.resources import files
import sys
import earthkit.data


# First-party
import meteodatalab.operators.flexpart as flx
from meteodatalab import grib_decoder

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Set the desired multipart threshold value (5GB)
GB = 1024 ** 3
config = TransferConfig(multipart_threshold=5*GB)

# Initializing variables for the S3 client
S3_BUCKET_NAME = "flexpart-input"
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY')
S3_ENDPOINT_URL = "https://object-store.os-api.cci1.ecmwf.int"

def download_ecmwf(file, location, info=''):
    
    logging.info(f"Downloading {info} {file} from S3 {S3_BUCKET_NAME} bucket")

    # Creating an S3 client to interact with S3 bucket 
    s3_client = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT_URL,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    use_ssl=True,
    )
    
    try: 
        start = time.time()
        response = s3_client.download_file(S3_BUCKET_NAME, Path(file).name, location, Config=config)
        elapsed = time.time() - start
        logging.info(f"Finished downloading {info} {file} in: {elapsed} seconds.")

    except Exception as e:
        raise(e)
    


def list_objects_from_s3():
    
    logging.debug(locals())

    logging.info("List objects inside our bucket")
    
    # Creating an S3 client to interact with S3 bucket 
    s3_client = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT_URL,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    use_ssl=True,
    )
    logging.info("Connect with bucket")
    
    try:
        response = s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
        logging.info("Connection successful. Bucket exists.")
    except Exception as e:
        logging.error("Connection failed: %s", e)
        return

    objects = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME)

    logging.info("List content of the bucket")
    # Check if 'Contents' key exists in objects
    if 'Contents' in objects:
        # Extract and print the 'Contents' key
        contents = objects['Contents']
        logging.info("Contents:")
        for item in contents:
            key = item.get('Key', 'Unknown Key')
            logging.info(f"Key: {key}")
    else:
        logging.info("Bucket is empty or 'Contents' key does not exist.")


def upload_file(file_name, object_name=None):
    """Upload a file to the S3 bucket

    :param file_name: File to upload
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT_URL,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    use_ssl=True,
    )
    try:
        response = s3_client.upload_file(file_name, S3_BUCKET_NAME, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def main():
    # Check the content of the S3 before uploading files 
    list_objects_from_s3()

    # Upload files to s3
    should_upload_to_s3 = False

    # Select 4 timesteps to upload (min. 3)
    # NB: Atm. this is hardcoded but later event should be triggered when ecmwf files arrived in s3
    # Location in the container to upload from to s3
    folder_path = "/data/24040100"
    datafiles = ["efsf00000000", "efsf00010000", "efsf00020000", "efsf00030000"]

    if should_upload_to_s3:
        # Upload files
        for file in datafiles:
            # Upload the file
            upload_success = upload_file(os.path.join(folder_path, file))
            
            # Check if the file was uploaded successfully
            if upload_success:
                logging.info(f"File {file} uploaded successfully!")
            else:
                logging.info(f"Failed to upload file {file}.")
    else:
        logging.info("Skipping file upload as they are aleady uploaded.")
    
    # Check the content of the S3 after uploading files 
    list_objects_from_s3()

    # meteodata-lab job
    # Constants and input fields keys
    constants = ("z", "lsm", "sdor")
    inputf = (
        "etadot", "t", "q", "u", "v", "sp", "10u", "10v", "2t", "2d", "tcc", "sd",
        "cp", "lsp", "ssr", "sshf", "ewss", "nsss"
    )

    valid_temp_files = []

    # Download from S3
    for index, file in enumerate(datafiles):
        timestep = index + 1  # Timestep starts from 1
        # Create a temporary file with a name that includes the index

        with tempfile.NamedTemporaryFile(suffix=f'_file_{index}', delete=False) as tmp_file:
            # Download the file from S3
            download_ecmwf(file, tmp_file.name, info=f'timestep {timestep}')
            valid_temp_files.append(tmp_file.name)

    """
    ds = grib_decoder.load_fieldnames(
        constants + inputf, valid_temp_files, ref_param="T", extract_pv="U"
    )
    """

if __name__ == "__main__":
    main()
