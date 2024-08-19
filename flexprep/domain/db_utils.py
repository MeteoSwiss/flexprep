import logging
import sqlite3
from datetime import datetime as dt

from flexprep import CONFIG
from flexprep.domain.data_model import IFSForecast

_LOGGER = logging.getLogger(__name__)


class DB:
    conn: sqlite3.Connection

    def __init__(self) -> None:
        try:
            """Establish a database connection."""
            self.db_path = CONFIG.main.db_path
            self.conn = sqlite3.connect(self.db_path)
            _LOGGER.info("Connected to database.")
            self._initialize_db()
        except sqlite3.Error as e:
            _LOGGER.exception(f"An error occurred: {e}")
            raise
        finally:
            _LOGGER.info("Database setup complete.")

    def _initialize_db(self) -> None:
        """Create tables if they do not exist."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS uploaded (
            forecast_ref_time TEXT NOT NULL,
            step INTEGER NOT NULL,
            key TEXT NOT NULL,
            processed BOOLEAN NOT NULL
        )
        """
        try:
            with self.conn:
                self.conn.execute(create_table_query)
                _LOGGER.info("Table uploaded is ready.")
        except sqlite3.Error as e:
            _LOGGER.exception(f"An error occurred while initializing the database: {e}")
            raise

    def insert_item(self, item: IFSForecast) -> None:
        """Insert a single item into the 'uploaded' table."""
        try:
            with self.conn:
                self.conn.execute(
                    """
                    INSERT INTO uploaded (forecast_ref_time, step, key, processed)
                    VALUES (?, ?, ?, ?)
                """,
                    (item.forecast_ref_time, item.step, item.key, item.processed),
                )
            _LOGGER.info("Data inserted successfully.")
        except sqlite3.Error as e:
            _LOGGER.exception(f"An error occurred while inserting data: {e}")
            raise

    def query_table(self, forecast_ref_time: str) -> list[IFSForecast]:
        """Query the table for items with a specific forecast_ref_time.

        Args:
            forecast_ref_time (str): The forecast reference time to query for.

        Returns:
            list[ifs_forecast]: A list of ifs_forecast objects that match the query.
        """

        query = (
            "SELECT forecast_ref_time, step, key, processed "
            "FROM uploaded "
            "WHERE forecast_ref_time = ?"
        )

        try:
            with self.conn:
                cursor = self.conn.execute(query, (forecast_ref_time,))
                rows = cursor.fetchall()
                _LOGGER.info(f"Query returned {len(rows)} items.")

                results = [
                    IFSForecast(
                        forecast_ref_time=row[0],
                        step=row[1],
                        key=row[2],
                        processed=row[3],
                    )
                    for row in rows
                ]
                return results

        except sqlite3.Error as e:
            _LOGGER.exception(f"An error occurred while querying the database: {e}")
            raise

    def update_item_as_processed(
        self, forecast_ref_time: dt, step: int, key: str
    ) -> None:
        """Update the 'processed' field of a specific item to True."""
        try:
            with self.conn:
                result = self.conn.execute(
                    """
                    UPDATE uploaded
                    SET processed = 1
                    WHERE forecast_ref_time = ? AND step = ? AND key = ?
                """,
                    (forecast_ref_time, step, key),
                )
                if result.rowcount > 0:
                    _LOGGER.info("Item marked as processed.")
                else:
                    _LOGGER.warning("No item found to update.")
        except sqlite3.Error as e:
            _LOGGER.exception(f"An error occurred while updating the item: {e}")
            raise
