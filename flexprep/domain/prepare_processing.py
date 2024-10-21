import logging

from flexprep import CONFIG
from flexprep.domain.db_utils import DB
from flexprep.domain.processing import Processing

logger = logging.getLogger(__name__)


def launch_pre_processing(ifs_forecast_obj):
    db = DB()

    # Query the table for items with the same forecast reference time
    items_in_table = db.query_table(ifs_forecast_obj.forecast_ref_time)

    # Get step zero items and all steps
    step_zero_items = [item.to_dict() for item in items_in_table if item.step == 0]
    steps = [item.step for item in items_in_table]

    # Ensure there are at least two step-zero items
    if len(step_zero_items) < 2:
        message = (
            f"Currently only {len(step_zero_items)} step=0 files are available. "
            "Waiting for these before processing."
        )
        logger.info(message)
        return

    # Check if the step is aligned with the configured time increment
    step_to_process = ifs_forecast_obj.step
    if (
        step_to_process - CONFIG.main.time_settings.tstart
    ) % CONFIG.main.time_settings.tincr != 0:
        logger.info("Step is not aligned with the configured time increment.")
        return

    prev_step = step_to_process - CONFIG.main.time_settings.tincr
    # Check if the previous step exists
    # or if the current object has already been processed
    if prev_step not in steps or ifs_forecast_obj.processed:
        logger.info(
            f"Not launching Pre-Processing for timestep {step_to_process}: "
            f"prev_step in steps: {prev_step in steps}, "
            f"processed: {ifs_forecast_obj.processed == True}"
        )
        return

    logger.info(f"Launching Pre-Processing for timestep {step_to_process}")

    # Process items for the current and previous step, including step-zero items
    process_items = step_zero_items + [ifs_forecast_obj.to_dict()]
    if prev_step != 0:
        prev_obj = next(
            (item.to_dict() for item in items_in_table if item.step == prev_step), None
        )
        if prev_obj:
            process_items.append(prev_obj)
        else:
            msg = f"Cannot find file for previous step {prev_step}"
            logger.exception(msg)
            raise ValueError(msg)

    Processing().process(process_items)
