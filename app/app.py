"""Pre-Process IFS HRES data as input to Flexpart."""
import logging
import tempfile
import os
from pathlib import Path
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
import numpy as np
from itertools import groupby
import meteodatalab.operators.flexpart as flx
from meteodatalab import grib_decoder, data_source, metadata, config

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Configuration
GB = 1024 ** 3
TRANSFER_CONFIG = TransferConfig(multipart_threshold=5 * GB)
S3INPUT_ENDPOINT_URL = "https://object-store.os-api.cci1.ecmwf.int"
S3OUTPUT_ENDPOINT_URL = "" # NB: doesn't exist yet  
S3INPUT_BUCKET_NAME = "flexpart-input"
S3OUTPUT_BUCKET_NAME = "flexpart-output" # NB: doesn't exist yet  
TINCR = int(os.getenv('TINCR', 3))
TSTART = int(os.getenv('TSTART', 0))

def get_s3_client(endpoint_url, access_key, secret_key):
    return boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        use_ssl=True,
    )

s3_input = get_s3_client(S3INPUT_ENDPOINT_URL, os.getenv('S3INPUT_ACCESS_KEY'), os.getenv('S3INPUT_SECRET_KEY'))
# NB: doesn't exist yet  
# s3_output = get_s3_client(S3OUTPUT_ENDPOINT_URL, os.getenv('S3OUTPUT_ACCESS_KEY'), os.getenv('S3OUTPUT_SECRET_KEY'))

# Define constants and input fields for flexpart
constants = ("z", "lsm", "sdor")
input_fields = (
    "u", "v", "etadot", "t", "q", "sp", "10u", "10v", "2t", "2d", "tcc", "sd",
    "cp", "lsp", "ssr", "sshf", "ewss", "nsss"
)

def download_file(s3_client, bucket, key, local_path):
    try:
        s3_client.download_file(bucket, key, local_path, Config=TRANSFER_CONFIG)
        logging.info(f"Downloaded file from S3: {key}")
    except ClientError as e:
        logging.error(f"Error downloading file {key}: {e}")

def upload_file(s3_client, bucket, local_path):
    key = os.path.basename(local_path)
    try:
        s3_client.upload_file(local_path, bucket, key)
        logging.info(f"Uploaded file to S3: {key}")
    except ClientError as e:
        logging.error(f"Error uploading file {local_path}: {e}")

def validate_dataset(ds, params, ref_time, step, prev_step):
    if not all(param in ds.keys() for param in params):
        raise ValueError("Not all requested parameters are present in the dataset")
    if not all(
        (np.array_equal(array.coords["time"].values, [0])) or
        (prev_step == 0 and np.array_equal(array.coords["time"].values, [0, step])) or
        (prev_step != 0 and np.array_equal(array.coords["time"].values, [0, prev_step, step]))
        for array in ds.values()
    ):
        raise ValueError("Downloaded steps are incorrect")
    if not all((array.coords["ref_time"].values == np.datetime64(ref_time, 'ns')) for array in ds.values()):
        raise ValueError("The forecast reference time is incorrect")

def process_fields(ds_out, ds_in, input_fields, constant_fields):
    missing_fields = [field for field in ds_in.keys() if field not in ds_out.keys() and field in input_fields and field != 'etadot']
    missing_const = [const for const in ds_in.keys() if const not in ds_out.keys() and const in constant_fields]

    for field in ds_out:
        ds_out[field] = ds_out[field].isel(time=slice(-1, None)).squeeze()
    for field in missing_fields:
        logging.warning(f"Field '{field}' not found in output")
        ds_out[field] = ds_in[field].isel(time=slice(-1, None)).squeeze()
    for field in missing_const:
        ds_out[field] = ds_in[field].squeeze()

    ds_out['etadot'] = ds_out.pop('omega')
    ds_out['cp'] = ds_out['cp'] * 1000
    ds_out['lsp'] = ds_out['lsp'] * 100

def pre_process(file_objs):
    def download_temp_file(file_info):
        temp_file = tempfile.NamedTemporaryFile(suffix=file_info['key'], delete=False)
        download_file(s3_input, S3INPUT_BUCKET_NAME, file_info['key'], temp_file.name)
        file_info['temp_file'] = temp_file.name
        return temp_file

    file_objs.sort(key=lambda x: int(x['step']), reverse=True)
    if len(file_objs) < 3:
        raise ValueError("Not enough files for pre-processing")

    to_process = file_objs[0]
    step_to_process = int(to_process['step'])
    forecast_ref_time = to_process['forecast_ref_time']

    if to_process['processed'] == "Y":
        logging.warning(f"Already processed timestep {step_to_process}. Skipping.")
        return

    temp_files = [download_temp_file(to_process)]
    prev_file = file_objs[1]
    prev_step = int(prev_file['step'])

    if prev_step != 0:
        temp_files.append(download_temp_file(prev_file))
        init_files = file_objs[2:4]
    else:
        init_files = file_objs[1:3]

    init_files.sort(key=lambda x: x['key'][-2:])
    temp_files.extend([download_temp_file(f) for f in init_files])

    datafiles = [f.name for f in temp_files]

    request = {"param": list(constants + input_fields)}
    with config.set_values(data_scope="ifs"):
        source = data_source.DataSource(datafiles=datafiles)
        ds_in = grib_decoder.load(source, request)
        validate_dataset(ds_in, request["param"], forecast_ref_time, step_to_process, prev_step)
        ds_in |= metadata.extract_pv(ds_in["u"].message)

    ds_out = flx.fflexpart(ds_in)
    process_fields(ds_out, ds_in, input_fields, constants)

    for temp_file in temp_files:
        temp_file.close()

    output_file = tempfile.NamedTemporaryFile(suffix=f"output_dispf{forecast_ref_time}_{step_to_process}", delete=False)
    with open(output_file.name, "wb") as fout:
        for name, field in ds_out.items():
            if field.attrs.get('v_coord') == 'hybrid':

                logging.info(f"Writing GRIB fields to {output_file.name}")
                grib_decoder.save(field, fout)

    # NB: s3-output doesn't exist yet
    # upload_file(s3_output, S3OUTPUT_BUCKET_NAME, output_file.name)

def select_objects(objects):
    obj_in_s3 = []

    for item in objects.get('Contents', []):
        key = item.get('Key')
        forecast_ref_time_str= f"{datetime.now().year}{key[3:11]}"
        forecast_ref_time = datetime.strptime(forecast_ref_time_str, "%Y%m%d%H%M")
        valid_time_str = f"{datetime.now().year}{key[11:18]}"
        valid_time_obj = datetime.strptime(valid_time_str, "%Y%m%d%H%M")

        step = 0 if valid_time_obj.minute == 1 else int((valid_time_obj - forecast_ref_time).total_seconds() / 3600)
        obj_in_s3.append({
            "key": key,
            "forecast_ref_time": forecast_ref_time,
            "step": step,
            "processed": "N",
        })

    obj_in_s3.sort(key=lambda x: x['forecast_ref_time'])
    for fcst_ref_time, group in groupby(obj_in_s3, key=lambda x: x['forecast_ref_time']):
        files_per_run = list(group)
        step_zero = [file for file in files_per_run if file['step'] == 0]
        steps = [file['step'] for file in files_per_run]

        if len(step_zero) < 2:
            logging.info(f"Currently only {len(step_zero)} step=0 files are available. Waiting for these before processing.")
            continue

        for file in files_per_run:
            step = file['step']
            if (step - TSTART) % TINCR == 0:
                prev_step = step - TINCR
                if prev_step in steps and file['processed'] == "N":
                    logging.info(f"Launching Pre-Processing for timestep {step}")
                    if prev_step == 0:
                        pre_process(step_zero +[file])
                    else:
                        prev_file = next((item for item in files_per_run if item['step'] == prev_step), None)
                        if prev_file:
                            pre_process(step_zero + [prev_file, file])
                    file['processed'] = "Y"

                else:
                    logging.info(f"Not launching Pre-Processing for timestep {step}, {prev_step} in {steps}: {prev_step in steps}, processed: {file['processed'] == 'Y'}")

if __name__ == "__main__":
    objects = s3_input.list_objects_v2(Bucket=S3INPUT_BUCKET_NAME)
    # NB: atm. loop through all objects on s3 but next: only on new data 
    select_objects(objects)
    logging.info("Processing finished")
