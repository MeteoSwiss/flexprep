"""Pre-Process IFS HRES data as input to Flexpart."""

import argparse
import logging
import sys

from flexprep.domain.prepare_processing import launch_pre_processing
from flexprep.domain.data_model import IFSForecast


def parse_arguments():
    """Parse and return command-line arguments."""
    parser = argparse.ArgumentParser(description="Parse metadata of new file received")
    parser.add_argument("--step", type=int, required=True, help="Step argument")
    parser.add_argument(
        "--date", type=str, required=True, help="Date argument (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--time", type=str, required=True, help="Time argument (HH:MM:SS)"
    )
    parser.add_argument("--location", type=str, required=True, help="Location argument")

    return parser.parse_args()


def create_ifs_forecast_obj(args):
    """Create an IFSForecast object based on the parsed arguments."""
    try:
        forecast_ref_time = f"{args.date}T{args.time}Z"
        return IFSForecast(
            forecast_ref_time=forecast_ref_time,
            step=args.step,
            location=args.location,
            processed=False,
        )
    except Exception as e:
        logging.error(f"Error creating IFSForecast object: {e}")
        sys.exit(1)


def main():
    """Main function to parse arguments and process the IFS forecast."""
    logging.basicConfig(level=logging.INFO)

    args = parse_arguments()
    logging.info(
        f"Notification received for file - "
        f"Step: {args.step}, Date: {args.date}, "
        f"Time: {args.time}, Location: {args.location}"
    )

    ifs_forecast_obj = create_ifs_forecast_obj(args)

    launch_pre_processing(ifs_forecast_obj)


if __name__ == "__main__":
    main()
