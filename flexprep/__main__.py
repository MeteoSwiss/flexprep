"""Pre-Process IFS HRES data as input to Flexpart."""

import argparse
import logging
import sys
from datetime import datetime as dt
from pathlib import Path

from flexprep.domain.data_model import IFSForecast
from flexprep.domain.db_utils import DB
from flexprep.domain.prepare_processing import launch_pre_processing

logger = logging.getLogger(__name__)


def parse_arguments():
    """Parse and return command-line arguments."""
    parser = argparse.ArgumentParser(description="Parse metadata of new file received")
    parser.add_argument("--step", type=str, required=True, help="Step argument")
    parser.add_argument(
        "--date", type=str, required=True, help="Date argument (yyyymmdd)"
    )
    parser.add_argument("--time", type=str, required=True, help="Time argument (HH)")
    parser.add_argument("--location", type=str, required=True, help="Location argument")

    return parser.parse_args()


def create_ifs_forecast_obj(args):
    """Create an IFSForecast object based on the parsed arguments."""
    try:

        # Combine date and time to create forecast_ref_time
        forecast_ref_time_str = f"{args.date}{int(args.time):02d}00"
        forecast_ref_time = dt.strptime(forecast_ref_time_str, "%Y%m%d%H%M")
        return IFSForecast(
            row_id=None,
            forecast_ref_time=forecast_ref_time,
            step=int(args.step),
            key=Path(args.location).name,
            processed=False,
        )
    except Exception as e:
        logger.error(f"Error creating IFSForecast object: {e}")
        sys.exit(1)


if __name__ == "__main__":
    """Main function to parse arguments and process the IFS forecast."""

    args = parse_arguments()
    logger.info(
        f"Notification received for file - "
        f"Step: {args.step}, Date: {args.date}, "
        f"Time: {args.time}, Location: {args.location}"
    )

    ifs_forecast_obj = create_ifs_forecast_obj(args)
    db = DB()

    # Insert the forecast object into the database
    db.insert_item(ifs_forecast_obj)
    logger.info(
        f"Put item ({ifs_forecast_obj.forecast_ref_time}, "
        f"{ifs_forecast_obj.step}, {ifs_forecast_obj.key}) succeeded."
    )

    # Initiate the processing of the current step
    launch_pre_processing(ifs_forecast_obj)

    # Since the forecast steps may arrive out of sequence,
    # process any earlier steps that were previously skipped
    pending_fcst_objs = DB().get_pending_steps_from_db(
        ifs_forecast_obj.forecast_ref_time, ifs_forecast_obj.step
    )

    for obj in pending_fcst_objs:
        launch_pre_processing(obj)
