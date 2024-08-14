"""Pre-Process IFS HRES data as input to Flexpart."""
import argparse
import logging

from domain.models import IFSForecast
from flexprep.services.processing_service import process

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Parse metadata of new file received')
    parser.add_argument('--step', type=str, required=True, help='Step argument')
    parser.add_argument('--date', type=str, required=True, help='Date argument')
    parser.add_argument('--time', type=str, required=True, help='Time argument')
    parser.add_argument('--location', type=str, required=True, help='Location argument')

    args = parser.parse_args()
    logging.info(f"Notification received for file step: {args.step}, date: {args.date}, time: {args.time}, location: {args.location}")

    ifs_forecast_obj = IFSForecast(
        forecast_ref_time=f"{args.date}T{args.time}Z",
        step=int(args.step),
        location=args.location,
        processed=False
    )

    process(ifs_forecast_obj)
