import logging
from datetime import datetime
from itertools import groupby
import typing

from flexprep import CONFIG
from flexprep.domain.processing import Processing
from flexprep.domain.db_utils import DB

def launch_pre_processing(ifs_forecast_obj):
    db = DB()

    # Insert the forecast object into the database
    db.insert_item(ifs_forecast_obj)
    logging.info(
        f"Put item ({ifs_forecast_obj.forecast_ref_time}, {ifs_forecast_obj.step}, {ifs_forecast_obj.location}) succeeded."
    )

    # Query the table for items with the same forecast reference time
    items_in_table = db.query_table(ifs_forecast_obj.forecast_ref_time)

    # Get step zero items and all steps
    step_zero = [item.to_dict() for item in items_in_table if item.step == 0]
    steps = [item.step for item in items_in_table]

    # Check if there are enough step=0 files
    if len(step_zero) < 2:
        message = (
            f"Currently only {len(step_zero)} step=0 files are available. "
            "Waiting for these before processing."
        )
        logging.info(message)
        return  # Exit the function if there aren't enough step=0 files

    step = ifs_forecast_obj.step
    # Check if the step is aligned with the time increment settings
    if (
        step - CONFIG.main.time_settings.tstart
    ) % CONFIG.main.time_settings.tincr != 0:
        return  # Exit if the step is not aligned with time increment

    prev_step = step - CONFIG.main.time_settings.tincr
    # Check if the previous step exists or if the current object has already been processed
    if prev_step not in steps or ifs_forecast_obj.processed == True:
        logging.info(
            f"Not launching Pre-Processing for timestep {step}: "
            f"prev_step in steps: {prev_step in steps}, "
            f"processed: {ifs_forecast_obj.processed == True}"
        )
        return  # Exit if the previous step is missing or already processed

    logging.info(f"Launching Pre-Processing for timestep {step}")

    # Process step zero and the current step object
    if prev_step == 0:
        Processing().process(step_zero + ifs_forecast_obj.to_dict())
    else:
        # Find the previous step object
        prev_obj = next(
            (item.to_dict() for item in items_in_table if item.step == prev_step),
            None,
        )
        if prev_obj:
            Processing().process(step_zero + [prev_obj, ifs_forecast_obj.to_dict()])
        else:
            msg = f"Cannot find file for previous step {prev_step}"
            logging.error(msg)
            raise ValueError(msg)

