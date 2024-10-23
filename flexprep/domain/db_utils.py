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
                    (
                        item.forecast_ref_time,
                        item.step,
                        item.key,
                        item.processed,
                    ),
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
        """
        Query the database for unprocessed steps that can be processed, ensuring
        that at least two step=0 items exist and that each step has its previous
        step present before proceeding.

        Args:
            forecast_ref_time (datetime): The forecast reference time to query for.

        Returns:
            list[list[dict]]: A list of lists containing IFSForecast objects for
            each step, including two step-0 items and the previous step if step != 0.
        """
        try:
            # Ensure database connection is managed properly with context
            with self.conn:
                # Query for step-0 items (up to 2)
                step_zero_items = self._fetch_step_zero_items(forecast_ref_time)

                # Return if fewer than two step-0 items exist
                if len(step_zero_items) < 2:
                    logger.info(
                        f"Currently only {len(step_zero_items)} step=0 files are available. "
                        "Waiting for these before processing."
                    )
                    return []

                # Fetch the current and previous steps in a single query
                rows = self._fetch_current_and_previous_steps(forecast_ref_time)

                if not rows:
                    logger.info(
                        f"No pending timesteps available for forecast_ref_time={forecast_ref_time}."
                    )
                    return []

                logger.info(f"Query returned {len(rows)} pending timestep(s).")

                # Prepare and return the combined list of processable steps
                combined_steps = self._prepare_processable_steps(step_zero_items, rows)

                return combined_steps

        except sqlite3.Error as e:
            logger.exception(
                f"An error occurred while querying the database for forecast_ref_time={forecast_ref_time}: {e}"
            )
            raise

    def _fetch_step_zero_items(self, forecast_ref_time: dt) -> list:
        """
        Fetch step-0 items from the database.

        Args:
            forecast_ref_time (datetime): The forecast reference time to query for.

        Returns:
            list: A list of step-0 rows.
        """
        step_zero_query = (
            "SELECT row_id, forecast_ref_time, step, key, processed "
            "FROM uploaded "
            "WHERE forecast_ref_time = ? AND step = 0 "
            "LIMIT 2"
        )
        cursor = self.conn.execute(step_zero_query, (forecast_ref_time,))
        return cursor.fetchall()

    def _fetch_current_and_previous_steps(self, forecast_ref_time: dt) -> list:
        """
        Fetch current steps and their previous steps from the database.

        Args:
            forecast_ref_time (datetime): The forecast reference time to query for.

        Returns:
            list: A list of rows containing both the current step and its previous step.
        """
        tstart = CONFIG.main.time_settings.tstart
        tincr = CONFIG.main.time_settings.tincr

        step_query = """
        WITH step_data AS (
            SELECT
                cur.row_id AS cur_row_id,
                cur.forecast_ref_time AS cur_forecast_ref_time,
                cur.step AS cur_step,
                cur.key AS cur_key,
                cur.processed AS cur_processed,
                prev.row_id AS prev_row_id,
                prev.forecast_ref_time AS prev_forecast_ref_time,
                prev.step AS prev_step,
                prev.key AS prev_key,
                prev.processed AS prev_processed,
                ROW_NUMBER() OVER (PARTITION BY cur.step ORDER BY cur.step ASC) AS rn
            FROM
                uploaded cur
            LEFT JOIN
                uploaded prev
            ON
                cur.forecast_ref_time = prev.forecast_ref_time
                AND prev.step = cur.step - ?
            WHERE
                cur.forecast_ref_time = ?
                AND cur.processed = FALSE
                AND cur.step != 0
                AND (cur.step - ?) % ? = 0
        )
        SELECT *
        FROM step_data
        WHERE (prev_step = 0 AND rn = 1) OR (prev_step != 0) OR (prev_step is NULL)
        ORDER BY cur_step ASC;
        """

        cursor = self.conn.execute(
            step_query, (tincr, forecast_ref_time, tstart, tincr)
        )
        return cursor.fetchall()

    def _prepare_processable_steps(self, step_zero_items: list, rows: list) -> list:
        """
        Prepare the processable steps by combining step-0 items with the previous
        and current steps from the query results.

        Args:
            step_zero_items (list): The step-0 items to include in the processable steps.
            rows (list): The rows containing the current and previous steps.

        Returns:
            list: A combined list of processable steps.
        """
        combined_steps = []

        # Create step-0 IFSForecast objects
        step_zero_forecasts = [
            IFSForecast(
                row_id=zero_row[0],
                forecast_ref_time=dt.strptime(zero_row[1], "%Y-%m-%d %H:%M:%S"),
                step=int(zero_row[2]),
                key=zero_row[3],
                processed=zero_row[4],
            ).to_dict()
            for zero_row in step_zero_items
        ]

        for row in rows:
            current_step = row[2]  # cur_step
            prev_step = current_step - CONFIG.main.time_settings.tincr
            prev_row_id = row[5]

            # Start with step-0 items
            processable_list = step_zero_forecasts.copy()

            # If previous step exists and is not step-0, add it
            if prev_row_id is not None and prev_step != 0:  # prev_row_id is present
                processable_list.append(
                    IFSForecast(
                        row_id=row[5],  # prev_row_id
                        forecast_ref_time=dt.strptime(row[6], "%Y-%m-%d %H:%M:%S"),
                        step=int(row[7]),  # prev_step
                        key=row[8],  # prev_key
                        processed=row[9],  # prev_processed
                    ).to_dict()
                )
            elif prev_row_id is None:
                logger.info(
                    f"Step {current_step} skipped, "
                    f"previous step {prev_step} not found."
                )
                continue

            # Add the current step to the processable list
            processable_list.append(
                IFSForecast(
                    row_id=row[0],  # cur_row_id
                    forecast_ref_time=dt.strptime(row[1], "%Y-%m-%d %H:%M:%S"),
                    step=int(row[2]),  # cur_step
                    key=row[3],  # cur_key
                    processed=row[4],  # cur_processed
                ).to_dict()
            )

            combined_steps.append(processable_list)

        return combined_steps

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
