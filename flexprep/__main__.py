"""Pre-Process IFS HRES data as input to Flexpart."""

import argparse
import logging
import sys
from datetime import datetime as dt
from pathlib import Path

from flexprep.domain.data_model import IFSForecast
from flexprep.domain.prepare_processing import launch_pre_processing

_LOGGER = logging.getLogger(__name__)


def parse_arguments():
    """Parse and return command-line arguments."""
    parser = argparse.ArgumentParser(description="Parse metadata of new file received")
    parser.add_argument("--step", type=int, required=True, help="Step argument")
    parser.add_argument("--date", type=str, required=True, help="Date argument (mmdd)")
    parser.add_argument("--time", type=str, required=True, help="Time argument (HHMM)")
    parser.add_argument("--location", type=str, required=True, help="Location argument")

    return parser.parse_args()


def create_ifs_forecast_obj(args):
    """Create an IFSForecast object based on the parsed arguments."""
    try:
        # year is missing from "time" in dess. products so retrieve it
        # Get the current year and month
        now = dt.now()
        current_year = now.year
        current_month = now.month

        # Extract month from args.date
        forecast_month = int(args.date[:2])

        # Determine the correct year for the forecast_ref_time
        if current_month == 1 and forecast_month == 12:
            # If current month is Jan and forecast month is Dec, use the prev year
            forecast_year = current_year - 1
        else:
            # Otherwise, use the current year
            forecast_year = current_year

        # Combine date and time to create forecast_ref_time
        forecast_ref_time_str = f"{forecast_year}{args.date}{args.time}"
        forecast_ref_time = dt.strptime(forecast_ref_time_str, "%Y%m%d%H%M")
        return IFSForecast(
            row_id=None,
            forecast_ref_time=forecast_ref_time,
            step=args.step,
            key=Path(args.location).name,
            processed=False,
        )
    except Exception as e:
        _LOGGER.error(f"Error creating IFSForecast object: {e}")
        sys.exit(1)


if __name__ == "__main__":
    """Main function to parse arguments and process the IFS forecast."""

    args = parse_arguments()
    _LOGGER.info(
        f"Notification received for file - "
        f"Step: {args.step}, Date: {args.date}, "
        f"Time: {args.time}, Location: {args.location}"
    )

    ifs_forecast_obj = create_ifs_forecast_obj(args)

    launch_pre_processing(ifs_forecast_obj)
