import logging
import os
import tempfile
import typing

import meteodatalab.operators.flexpart as flx
from meteodatalab import config, data_source, grib_decoder, metadata

from pilotecmwf_pp_starter.domain.flexpart_utils import prepare_output
from pilotecmwf_pp_starter.domain.s3_utils import S3client
from pilotecmwf_pp_starter.domain.validation_utils import validate_dataset

# Define constants and input fields for pre-flexpart
constants = {"z", "lsm", "sdor"}
input_fields = {
    "u",
    "v",
    "etadot",
    "t",
    "q",
    "sp",
    "10u",
    "10v",
    "2t",
    "2d",
    "tcc",
    "sd",
    "cp",
    "lsp",
    "ssr",
    "sshf",
    "ewss",
    "nsss",
}


class Processing:
    FileObject = dict[str, typing.Any]

    def __init__(self) -> None:
        self.s3_client = S3client()

    def process(self, file_objs: list[FileObject]) -> None:
        """Process file objects by downloading, validating, and processing the data."""

        file_objs.sort(key=lambda x: int(x["step"]), reverse=True)
        if len(file_objs) < 3:
            raise ValueError("Not enough files for pre-processing")

        to_process = file_objs[0]
        step_to_process = int(to_process["step"])
        forecast_ref_time = to_process["forecast_ref_time"]

        if to_process["processed"] == "Y":
            logging.warning(f"Already processed timestep {step_to_process}. Skipping.")
            return

        temp_files = [self.s3_client.download_file(to_process)]
        prev_file = file_objs[1]
        prev_step = int(prev_file["step"])

        if prev_step != 0:
            temp_files.append(self.s3_client.download_file(prev_file))
            init_files = file_objs[2:4]
        else:
            init_files = file_objs[1:3]

        init_files.sort(key=lambda x: x["key"][-2:])
        temp_files.extend([self.s3_client.download_file(f) for f in init_files])

        request = {"param": list(constants | input_fields)}
        with config.set_values(data_scope="ifs"):
            source = data_source.DataSource(datafiles=temp_files)
            ds_in = grib_decoder.load(source, request)
            validate_dataset(
                ds_in, request["param"], forecast_ref_time, step_to_process, prev_step
            )
            ds_in |= metadata.extract_pv(ds_in["u"].message)

        ds_out = flx.fflexpart(ds_in)
        prepare_output(ds_out, ds_in, input_fields, constants)

        for temp_file in temp_files:
            os.unlink(temp_file)

        output_file = tempfile.NamedTemporaryFile(
            suffix=f"output_dispf{forecast_ref_time}_{step_to_process}", delete=False
        )
        with open(output_file.name, "wb") as fout:
            for name, field in ds_out.items():
                if field.attrs.get("v_coord") == "hybrid":

                    logging.info(f"Writing GRIB fields to {output_file.name}")
                    grib_decoder.save(field, fout)

        # NB: s3-output doesn't exist yet
        # upload_file(s3_output, S3OUTPUT_BUCKET_NAME, output_file.name)
