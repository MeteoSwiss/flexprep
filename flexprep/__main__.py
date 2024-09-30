"""Pre-Process IFS HRES data as input to Flexpart."""

import argparse
import logging
import sys
from datetime import datetime as dt
from pathlib import Path

from flexprep.domain.data_model import IFSForecast
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
            flexpart=False,
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

    launch_pre_processing(ifs_forecast_obj)
