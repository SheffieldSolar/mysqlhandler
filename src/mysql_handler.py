"""
Created on 2022-02-01

@author: Julian Briggs

Docs on installing and using 3 Python mysql connectors: mysql.connector, mysqlDB, pymysql
https://www.a2hosting.co.uk/kb/developer-corner/mysql/connecting-to-mysql-using-python

https://py-pkgs.org/07-releasing-versioning.html

Cursors:
https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlconnection-cursor.html

https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursor-close.html
Use close() when you are done using a cursor. This method closes the cursor, resets all results,
and ensures that the cursor object has no reference to its original connection object.

https://stackoverflow.com/questions/5669878/when-to-close-cursors-using-mysqldb

Google Cloud SQL defaults to MySQL-8.0.18 (@ 2022-05-19) but can upgrade: gcloud sql instances patch sheffieldsolar --database-version=MYSQL_8_0_28
I reviewed the code and attempted to leave suggestions for improving its quality. While most comments were applied successfully, one of the patterns I used did not match correctly. Here's what I tried to address:

    Added a suggestion for adding type hints to the config parameter in override_mysql_options. DONE
    Suggested using Mapping instead of Dict in redact_mysql_options for better flexibility. DONE
    Recommended a minor performance improvement in the use of a tuple constructor in cols_on_dup. DONE
    Attempted to comment on the insert_select_on_duplicate_key_update_statement method regarding the keys parameter, but the pattern did not match correctly.
    Noted the need to ensure max_val is not None in the reset_auto_increment method to prevent potential errors. DONE

Use logger.info(%(var)s,{'var':var}) style formatting in logger.debug calls
"""

from argparse import Namespace
from contextlib import AbstractContextManager, closing
import logging
import traceback
from typing import Any, Dict, Tuple, Sequence, Optional, List

from mysql import connector


logger = logging.getLogger(__name__)
# Define a type alias for rows returned from the database
Rows = Sequence[Tuple[Any, ...]]


class MysqlHandler(AbstractContextManager):
    """
    A context manager for handling MySQL database operations.

    Provides methods to execute queries, inserts, and handle database transactions.
    Automatically closes the connection when the context is exited.
    """

    @staticmethod
    def override_mysql_options(config: Namespace):
        """
        Override the MySQL options based on the provided configuration.

        Updates specific fields like database, host, user, and password if available.
        """
        if config.mysql_database:
            config.mysql_options.update({"database": config.mysql_database})
        if config.mysql_host:
            config.mysql_options.update({"host": config.mysql_host})
        if config.mysql_password:
            config.mysql_options.update({"password": config.mysql_password})
        if config.mysql_user:
            config.mysql_options.update({"user": config.mysql_user})

    @staticmethod
    def redact_mysql_options(mysql_options: Dict[str, str]) -> Dict[str, str]:
        """
        Redact sensitive information (e.g., password) in MySQL options for logging purposes.

        Args:
            mysql_options (Dict[str, str]): Dictionary containing MySQL connection options.

        Returns:
            Dict[str, str]: A copy of the options with sensitive information redacted.
        """
        mysql_options_redacted = mysql_options.copy()
        mysql_options_redacted.update({"password": "REDACTED"})
        return mysql_options_redacted

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        """
        Ensures the database connection is closed when exiting the context.

        Logs logger.critical errors if a MySQL error occurs.
        """
        logger.debug("Exiting context and closing the connection.")
        self.cnx.close()
        if isinstance(exc_value, connector.errors.Error):
            logger.critical(
                "Database error %(exc_type)s %(exc_value)s mysql_options_redacted %(mysql_options_redacted)s %(stack)s",
                {
                    "exc_type": exc_type,
                    "exc_value": exc_value,
                    "mysql_options_redacted": self.mysql_options_redacted,
                    "stack": traceback.format_exc(),
                },
            )
            return False  # Propagate exception

    def __init__(self, mysql_options, cnx=None):
        """
        Initialize the MySQL handler.

        Args:
            mysql_options (dict): Dictionary of MySQL connection options.
            cnx (Optional[connector.MySQLConnection]): Optional existing connection object.
        """
        logger.debug("Initializing MysqlHandler with options.")
        self.mysql_options = mysql_options
        self.mysql_options_redacted = MysqlHandler.redact_mysql_options(mysql_options)
        if cnx:
            logger.debug("Using provided connection object.")
            self.cnx = cnx
        else:
            logger.debug(
                "MysqlHandler.__init__.mysql_options: %(mysql_options_redacted)s",
                {"mysql_options_redacted": self.mysql_options_redacted},
            )
            try:
                logger.debug("Establishing new connection to MySQL.")
                self.cnx = connector.connect(**mysql_options)
            except connector.errors.Error as err:
                logger.critical(
                    "Database error %(err)s mysql_options_redacted %(mysql_options_redacted)s %(stack)s",
                    {
                        "err": err,
                        "mysql_options_redacted": self.mysql_options_redacted,
                        "stack": traceback.format_exc(),
                    },
                )
                raise

    def close(self):
        """
        Close the database connection.

        No exception is raised if the connection is already closed.
        """
        logger.debug("Closing the database connection.")
        self.cnx.close()

    def execute(self, statement: str, params=None, multi=False) -> None:
        """
        Execute a single MySQL statement.

        Args:
            statement (str): The SQL statement to execute.
            params (Optional[dict]): Optional parameters for the SQL statement.
            multi (bool): Whether to execute multiple statements.
        """
        logger.debug(
            "Executing statement: %(statement)s with params: %(params)s",
            {"statement": statement, "params": params},
        )
        params = params or {}
        with closing(self.cnx.cursor()) as cursor:
            try:
                if multi:
                    logger.debug("Executing multiple statements.")
                    list(cursor.execute(statement, multi=True))
                else:
                    cursor.execute(statement, params, multi=False)
            except connector.errors.Error as err:
                logger.debug(
                    "Error executing statement: %(err)s",
                    {"err": err},
                )
                raise

    def executemany(self, statement: str, rows: Rows) -> None:
        """
        Execute a SQL statement with multiple rows of data.

        Args:
            statement (str): SQL statement (e.g., insert).
            rows (Rows): Rows of data to insert.
        """
        logger.debug(
            "Executing statement with multiple rows: %(statement)s",
            {"statement": statement},
        )
        with closing(self.cnx.cursor()) as cursor:
            try:
                cursor.executemany(statement, rows)
            except connector.errors.Error as err:
                logger.debug("Error executing multiple rows: %(err)s", {"err": err})
                err.add_note(f"statement {statement}")
                raise

    def fetchone(
        self, statement, params: Optional[Dict[str, Any]] = None
    ) -> Tuple[Any]:
        """
        Fetch a single row from the database.

        Args:
            statement (str): SQL query.
            params (Optional[dict]): Optional query parameters.

        Returns:
            Tuple[Any]: A tuple representing the row.
        """
        if params is None:
            params = {}
        logger.debug(
            "Fetching one row with statement: %(statement)s and params: %(params)s",
            {"statement": statement, "params": params},
        )
        with closing(self.cnx.cursor()) as cursor:
            try:
                cursor.execute(statement, params=params)
                result = cursor.fetchone()
                logger.debug(
                    "Fetched row: %(result)s",
                    {"result": result},
                )
                return result
            except connector.errors.Error as err:
                logger.debug(
                    "Error fetching one row: %(err)s",
                    {"err": err},
                )
                raise

    def fetchall(
        self,
        statement: str,
        params: Optional[Dict[str, Any]] = None,
        multi: bool = False,
        dictionary: bool = False,
    ) -> Rows | List[Dict[str, Any]]:
        """
        Fetch multiple rows from the database.

        Args:
            statement (str): SQL query.
            params (Optional[dict]): Optional query parameters.
            multi (bool): Whether the query includes multiple statements.
            dictionary (bool): Whether to return results as dictionaries.

        Returns:
            Rows | List[Dict[str, Any]]: A sequence of rows or dictionaries.
        """
        logger.debug(
            "Fetching all rows with statement: %(statement)s and params: %(params)s",
            {"statement": statement, "params": params},
        )
        if params is None:
            params = {}
        with closing(self.cnx.cursor(dictionary=dictionary)) as cursor:
            try:
                cursor.execute(statement, params=params, multi=multi)
                results = cursor.fetchall()
                logger.debug("Fetched rows: %(results)s", {"results": results})
                return results
            except connector.errors.Error as err:
                logger.debug("Error fetching all rows: %(error)s", {"error": err})
                err.add_note(f"statement {statement}")
                raise

    def insert_on_duplicate_key_update(
        self,
        table: str,
        cols: Tuple[str, ...],
        keys: Tuple[str, ...],
        rows: Rows,
        on_dup: str = "",
    ) -> None:
        """
        Perform an insert with "on duplicate key update" semantics.

        Args:
            table (str): Table to insert into.
            cols (Tuple[str, ...]): Columns to insert.
            keys (Tuple[str, ...]): Key columns for deduplication.
            rows (Rows): Rows of data to insert.
            on_dup (str): Custom "on duplicate key" SQL clause.
        """
        logger.debug(
            "Inserting with on duplicate key update into %(table)s.", {"table": table}
        )
        statement = self.insert_on_duplicate_key_update_statement(
            table, cols, keys, on_dup=on_dup
        )
        logger.debug("Statement: %(statement)s", {"statement": statement})
        self.executemany(statement, rows)

    def insert_on_duplicate_key_update_statement(
        self, table: str, cols: Tuple[str, ...], keys: Tuple[str, ...], on_dup: str = ""
    ) -> str:
        """
        Generate SQL for an "insert on duplicate key update" operation.

        Args:
            table (str): Table to insert into.
            cols (Tuple[str, ...]): Columns
