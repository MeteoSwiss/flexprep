import logging
import os
import tempfile
import typing

import meteodatalab.operators.flexpart as flx
from meteodatalab import config, data_source, grib_decoder, metadata

from flexprep.domain.flexpart_utils import prepare_output
from flexprep.domain.s3_utils import S3client
from flexprep.domain.validation_utils import validate_dataset

# Define constants and input fields for pre-flexpart
CONSTANTS = {"z", "lsm", "sdor"}
INPUT_FIELDS = {
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
        result = self._sort_and_download_files(file_objs)
        if result is None:
            logging.error("Failed to sort and download files.")
            return

        temp_files, to_process, prev_file = result

        ds_in = self._load_and_validate_data(temp_files, to_process, prev_file)
        if ds_in is None:
            logging.error("Failed to load and validate data.")
            return
        ds_out = self._apply_flexpart(ds_in)
        self._save_output(
            ds_out, to_process["forecast_ref_time"], int(to_process["step"])
        )

    def _sort_and_download_files(
        self, file_objs: list[FileObject]
    ) -> tuple[list[str], FileObject, FileObject] | None:
        """Sort file objects, validate, and select files for processing."""
        try:
            sorted_files = sorted(file_objs, key=lambda x: int(x["step"]), reverse=True)
            if len(sorted_files) < 3:
                raise ValueError("Not enough files for pre-processing")
            to_process = sorted_files[0]
            prev_file = sorted_files[1]
            if int(prev_file["step"]) != 0:
                init_files = sorted_files[2:4]
                tempfiles = self._download_files(
                    [to_process, prev_file, init_files[0], init_files[1]]
                )
            else:
                init_files = sorted_files[1:3]
                tempfiles = self._download_files(
                    [to_process, init_files[0], init_files[1]]
                )
            return tempfiles, to_process, prev_file
        except Exception as e:
            logging.error(f"Sorting and validation failed: {e}")
            return None

    def _download_files(self, files_to_download: list[FileObject]) -> list[str]:
        """Download files from S3 based on the file objects."""
        try:
            temp_files = [
                self.s3_client.download_file(file_obj) for file_obj in files_to_download
            ]
            return temp_files
        except Exception as e:
            logging.error(f"File download failed: {e}")
            raise RuntimeError("An error occurred while downloading files.") from e

    def _load_and_validate_data(
        self, temp_files: list[str], to_process: FileObject, prev_file: FileObject
    ) -> typing.Any:
        """Load and validate data from downloaded files."""
        request = {"param": list(CONSTANTS | INPUT_FIELDS)}
        with config.set_values(data_scope="ifs"):
            source = data_source.DataSource(datafiles=temp_files)
            ds_in = grib_decoder.load(source, request)
            validate_dataset(
                ds_in,
                request["param"],
                to_process["forecast_ref_time"],
                int(to_process["step"]),
                int(prev_file["step"]),
            )
            ds_in |= metadata.extract_pv(ds_in["u"].message)
        for temp_file in temp_files:
            os.unlink(temp_file)
        return ds_in

    def _apply_flexpart(self, ds_in: typing.Any) -> typing.Any:
        """Process data and return processed data structure."""
        ds_out = flx.fflexpart(ds_in)
        prepare_output(ds_out, ds_in, INPUT_FIELDS, CONSTANTS)
        return ds_out

    def _save_output(
        self, ds_out: typing.Any, forecast_ref_time: str, step_to_process: int
    ):
        """Save processed data to a temporary file."""
        output_file = tempfile.NamedTemporaryFile(
            suffix=f"output_dispf{forecast_ref_time}_{step_to_process}", delete=False
        )
        with open(output_file.name, "wb") as fout:
            for name, field in ds_out.items():
                if field.attrs.get("v_coord") == "hybrid":
                    logging.info(f"Writing GRIB fields to {output_file.name}")
                    grib_decoder.save(field, fout)
