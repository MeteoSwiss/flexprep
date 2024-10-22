import logging
import sqlite3
import typing
from datetime import datetime as dt

from flexprep import CONFIG
from flexprep.domain.data_model import IFSForecast

logger = logging.getLogger(__name__)


class DB:
    conn: sqlite3.Connection

    def __init__(self) -> None:
        try:
            """Establish a database connection."""
            self.db_path = CONFIG.main.db_path
            self.conn = sqlite3.connect(self.db_path)
            logger.debug("Connected to database.")
            self._initialize_db()
        except sqlite3.Error as e:
            logger.exception(f"An error occurred: {e}")
            raise
        else:
            logger.debug("Database setup complete.")

    def _initialize_db(self) -> None:
        """Create tables if they do not exist."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS uploaded (
            row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            forecast_ref_time TEXT NOT NULL,
            step INTEGER NOT NULL,
            key TEXT NOT NULL,
            processed BOOLEAN NOT NULL,
            UNIQUE(forecast_ref_time, step, key)
        )
        """
        try:
            with self.conn:
                self.conn.execute(create_table_query)
                logger.debug("Table uploaded is ready.")
        except sqlite3.Error as e:
            logger.exception(f"An error occurred while initializing the database: {e}")
            raise

    def insert_item(self, item: IFSForecast) -> None:
        """Insert a single item into the 'uploaded' table and update its row_id."""
        try:
            with self.conn:
                # Insert the item and get the newly inserted row_id
                result = self.conn.execute(
                    """
                    INSERT INTO uploaded (forecast_ref_time, step, key, processed)
                    VALUES (?, ?, ?, ?)
                    RETURNING row_id
                    """,
                    (item.forecast_ref_time, item.step, item.key, item.processed),
                )

                # Fetch the row_id from the result and update the item's row_id
                item.row_id = result.fetchone()[0]

            logger.debug("Data inserted successfully")
        except sqlite3.Error as e:
            logger.exception(f"An error occurred while inserting data: {e}")
            raise

    def get_processable_steps(
        self, forecast_ref_time: dt
    ) -> list[list[dict[str, typing.Any]]]:
        """Query the database for unprocessed steps that can be processed,
        ensuring that at least two step=0 items exist and that each step has
        its previous step already processed before proceeding.

        Args:
            forecast_ref_time (str): The forecast reference time to query for.

        Returns:
            list[list[IFSForecast]]: A list of lists containing IFSForecast objects
            for each step, including two step-0 items and the previous step if step!=0
        """
        # First, check if there are at least two step=0 items
        step_zero_count_query = (
            "SELECT COUNT(*) "
            "FROM uploaded "
            "WHERE forecast_ref_time = ? AND step = 0"
        )

        try:
            with self.conn:
                # Query for step-0 count
                cursor = self.conn.execute(step_zero_count_query, (forecast_ref_time,))
                step_zero_count = cursor.fetchone()[0]

                # If fewer than two step-0 items exist, return an empty list
                if step_zero_count < 2:
                    logger.info(
                        f"Currently only {step_zero_count} step=0 files are available. "
                        "Waiting for these before processing."
                    )
                    return []

                # Proceed with querying the steps
                tstart = CONFIG.main.time_settings.tstart
                tincr = CONFIG.main.time_settings.tincr

                query = (
                    "SELECT row_id, forecast_ref_time, step, key, processed "
                    "FROM uploaded "
                    "WHERE forecast_ref_time = ? "
                    "AND processed = FALSE "
                    "AND step != 0 "
                    "AND (step - ?) % ? = 0 "
                    "ORDER BY step ASC"
                )

                cursor = self.conn.execute(query, (forecast_ref_time, tstart, tincr))
                rows = cursor.fetchall()

                logger.info(f"Query returned {len(rows)} pending timestep(s).")

                # Prepare the combined list of processable steps
                combined_steps = []

                for row in rows:
                    current_step = row[2]  # The 'step' field
                    previous_step = current_step - tincr

                    # Get two step-0 items
                    step_zero_query = (
                        "SELECT * FROM uploaded "
                        "WHERE forecast_ref_time = ? AND step = 0 "
                        "LIMIT 2"
                    )
                    cursor = self.conn.execute(step_zero_query, (forecast_ref_time,))
                    step_zero_items = cursor.fetchall()

                    # Create step-0 IFSForecast objects
                    step_zero_forecasts = [
                        IFSForecast(
                            row_id=zero_row[0],
                            forecast_ref_time=dt.strptime(
                                zero_row[1], "%Y-%m-%d %H:%M:%S"
                            ),
                            step=int(zero_row[2]),
                            key=zero_row[3],
                            processed=zero_row[4],
                        ).to_dict()
                        for zero_row in step_zero_items
                    ]

                    # Check if the previous step exists in the database
                    prev_step_query = (
                        "SELECT row_id, forecast_ref_time, step, key, processed "
                        "FROM uploaded "
                        "WHERE forecast_ref_time = ? AND step = ? "
                        "LIMIT 1"
                    )
                    cursor = self.conn.execute(
                        prev_step_query, (forecast_ref_time, previous_step)
                    )
                    prev_step_row = cursor.fetchone()

                    processable_list = step_zero_forecasts  # Start with step-0 items

                    # If the previous step is found and is not step-0, include it
                    if prev_step_row and previous_step != 0:
                        processable_list.append(
                            IFSForecast(
                                row_id=prev_step_row[0],
                                forecast_ref_time=dt.strptime(
                                    prev_step_row[1], "%Y-%m-%d %H:%M:%S"
                                ),
                                step=int(prev_step_row[2]),
                                key=prev_step_row[3],
                                processed=prev_step_row[4],
                            ).to_dict()
                        )
                    elif not prev_step_row:
                        logger.info(
                            f"Step {current_step} skipped, "
                            f"previous step {previous_step} not found."
                        )
                        continue

                    # Add the current step as an IFSForecast object
                    processable_list.append(
                        IFSForecast(
                            row_id=row[0],
                            forecast_ref_time=dt.strptime(row[1], "%Y-%m-%d %H:%M:%S"),
                            step=int(row[2]),
                            key=row[3],
                            processed=row[4],
                        ).to_dict()
                    )

                    # Append the processable list to combined_steps
                    combined_steps.append(processable_list)

                return combined_steps

        except sqlite3.Error as e:
            logger.exception(f"An error occurred while querying the database: {e}")
            raise

    def update_item_as_processed(self, row_id: int) -> None:
        """Update the 'processed' field of a specific item to True."""
        try:
            with self.conn:
                result = self.conn.execute(
                    """
                    UPDATE uploaded
                    SET processed = 1
                    WHERE row_id = ?
                    """,
                    (row_id,),
                )
                if result.rowcount > 0:
                    logger.info("Item marked as processed.")
                else:
                    logger.warning("No item found to update.")
        except sqlite3.Error as e:
            logger.exception(f"An error occurred while updating the item: {e}")
            raise
