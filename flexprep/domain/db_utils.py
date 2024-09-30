import logging
import sqlite3

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
            logger.info("Connected to database.")
            self._initialize_db()
        except sqlite3.Error as e:
            logger.exception(f"An error occurred: {e}")
            raise
        else:
            logger.info("Database setup complete.")

    def _initialize_db(self) -> None:
        """Create tables if they do not exist."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS uploaded (
            row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            forecast_ref_time TEXT NOT NULL,
            step INTEGER NOT NULL,
            key TEXT NOT NULL,
            processed BOOLEAN NOT NULL,
            flexpart BOOLEAN NOT NULL,
            UNIQUE(forecast_ref_time, step, key)
        )
        """
        try:
            with self.conn:
                self.conn.execute(create_table_query)
                logger.info("Table uploaded is ready.")
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
                    INSERT INTO uploaded (forecast_ref_time, step, key,
                                        processed, flexpart)
                    VALUES (?, ?, ?, ?, ?)
                    RETURNING row_id
                    """,
                    (
                        item.forecast_ref_time,
                        item.step,
                        item.key,
                        item.processed,
                        item.flexpart,
                    ),
                )

                # Fetch the row_id from the result and update the item's row_id
                item.row_id = result.fetchone()[0]

            logger.info("Data inserted successfully")
        except sqlite3.Error as e:
            logger.exception(f"An error occurred while inserting data: {e}")
            raise

    def query_table(self, forecast_ref_time: str) -> list[IFSForecast]:
        """Query the table for items with a specific forecast_ref_time.

        Args:
            forecast_ref_time (str): The forecast reference time to query for.

        Returns:
            list[ifs_forecast]: A list of ifs_forecast objects that match the query.
        """

        query = (
            "SELECT row_id, forecast_ref_time, step, key, processed, flexpart "
            "FROM uploaded "
            "WHERE forecast_ref_time = ?"
        )

        try:
            with self.conn:
                cursor = self.conn.execute(query, (forecast_ref_time,))
                rows = cursor.fetchall()
                logger.info(f"Query returned {len(rows)} items.")
                results = [
                    IFSForecast(
                        row_id=row[0],
                        forecast_ref_time=row[1],
                        step=row[2],
                        key=row[3],
                        processed=row[4],
                        flexpart=row[5],
                    )
                    for row in rows
                ]
                return results

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
