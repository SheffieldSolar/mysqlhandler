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

"""
from contextlib import AbstractContextManager, closing
from logging import debug, critical
import traceback
from typing import Any, Dict, Tuple, Sequence, Optional

import mysql.connector.errors


Rows = Sequence[Tuple[Any, ...]]


class MysqlHandler(AbstractContextManager):

    """Insert and query database.
    constructor takes database connection cnx for testing.
    Create cnx if cnx=None and mysql_options are passed in
    """

    @staticmethod
    def override_mysql_options(config):
        # Override mysql_options with individual options
        if config.mysql_database:
            config.mysql_options.update({"database": config.mysql_database})
        if config.mysql_host:
            config.mysql_options.update({"host": config.mysql_host})
        if config.mysql_password:
            config.mysql_options.update({"password": config.mysql_password})
        if config.mysql_user:
            config.mysql_options.update({"user": config.mysql_user})

    @staticmethod
    def redact_mysql_options(mysql_options: Dict) -> Dict:
        mysql_options_redacted = mysql_options.copy()
        mysql_options_redacted.update({"password": "REDACTED"})
        return mysql_options_redacted

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        # pylint: disable=unused-argument
        self.cnx.close()
        if isinstance(exc_value, mysql.connector.errors.Error):
            critical(
                "Database error %(exc_type)s %(exc_value)s mysql_options_redacted %(mysql_options_redacted)s %(stack)s",
                {
                    "exc_type": exc_type,
                    "exc_value": exc_value,
                    "mysql_options_redacted": self.mysql_options_redacted,
                    "stack": exc_tb,
                },
            )
            return False  # Propagate except

    def __init__(self, mysql_options, cnx=None):
        self.mysql_options = mysql_options
        self.mysql_options_redacted = MysqlHandler.redact_mysql_options(mysql_options)
        if cnx:
            self.cnx = cnx
        else:
            debug(
                "MysqlHandler.__init__.mysql_options: %(mysql_options_redacted)s",
                {"mysql_options_redacted": self.mysql_options_redacted},
            )
            try:
                self.cnx = mysql.connector.connect(**mysql_options)
            except mysql.connector.errors.Error as err:
                critical(
                    "Database error %(err)s mysql_options_redacted %(mysql_options_redacted)s %(stack)s",
                    {
                        "err": err,
                        "mysql_options_redacted": self.mysql_options_redacted,
                        "stack": traceback.format_exc(),
                    },
                )
                raise

    def close(self):
        """Close database connection.
        Raises no exception.
        https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlconnection-disconnect.html
        """
        self.cnx.close()

    def execute(self, statement: str, params=None, multi=False) -> None:
        """MySQL execute statement (typically insert)"""
        params = params or {}
        with closing(self.cnx.cursor()) as cursor:
            try:
                if multi:
                    list(cursor.execute(statement, multi=True))
                else:
                    cursor.execute(statement, params, multi=False)
            except mysql.connector.errors.Error as err:
                err.add_note(f"statement {statement}")
                raise

    def executemany(self, statement: str, rows: Rows) -> None:
        """MySQL execute statement (typically insert) with multiple rows.
        :param statement: e.g. insert into table t (a,b,c) values(%s,%s,%s)
        :param rows: e.g.: [(0,1,2),(3,4,5),]
        """
        with closing(self.cnx.cursor()) as cursor:
            try:
                cursor.executemany(statement, rows)
            except mysql.connector.errors.Error as err:
                err.add_note(f"statement {statement}")
                raise

    def fetchone(
        self, statement, params: Optional[Dict[str, Any]] = None
    ) -> Tuple[Any]:
        """MySQL query (typically select) with one row result expected
        Can parameterise values but not table name.
        :param statement: e.g. select * from table t where id = 1
        :param params: dict e.g. {'id':id,'timestamp':timestamp,}
        :return: e.g. (0,1,2)
        """
        if params is None:
            params = {}
        debug("statement: %(statement)s", {"statement": statement})
        debug("params: %(params)s", {"params": params})
        with closing(self.cnx.cursor()) as cursor:
            try:
                cursor.execute(statement, params=params)
                return cursor.fetchone()
            except mysql.connector.errors.Error as err:
                err.add_note(f"statement {statement}")
                raise

    def fetchall(
        self,
        statement: str,
        params: Optional[Dict[str, Any]] = None,
        multi: bool = False,
    ) -> Rows:
        """MySQL query (typically select) with many rows expected
         (but not so many as to exhaust memory)
        Can parameterise values but not table name.
        :param statement: e.g. select * from table t
        :param params: dict e.g. {'id':id,'timestamp':timestamp,}
        :return: e.g. [(0,1,2),(3,4,5),]
        """
        debug("statement: %(statement)s", {"statement": statement})
        debug("params: %(params)s", {"params": params})
        if params is None:
            params = {}
        with closing(self.cnx.cursor()) as cursor:
            try:
                cursor.execute(statement, params=params, multi=multi)
                return cursor.fetchall()
            except mysql.connector.errors.Error as err:
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
        """MySQL insert ... on duplicate key update
        :param table: table to insert into
        :param cols: columns to insert
        :param keys: keys
        :param rows: list of tuples [(0,1,2,),(3,4,5,),]
        :param on_dup: on duplicate key string, e.g. 'a=vals.a,b=vals.b'
        """
        statement = self.insert_on_duplicate_key_update_statement(
            table, cols, keys, on_dup=on_dup
        )
        debug(statement)
        self.executemany(statement, rows)

    def insert_on_duplicate_key_update_statement(
        self, table: str, cols: Tuple[str, ...], keys: Tuple[str, ...], on_dup: str = ""
    ) -> str:
        # def insert_on_duplicate_key_update_statement(self, table: str,
        # col_names: Tuple[str, ...], on_dup_str: str) -> str:
        """Insert data into database table.
        Use post MySQL-8.0.19 syntax (use alias vals.X not values(X) for values for): insert ... on duplicate key insert
        https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html

        :param table: into which to insert data
        :param cols: columns to insert
        :param keys: keys
        :param on_dup: on duplicate key string, e.g. 'a=vals.a,b=vals.b'
        """
        cols_str = ",".join(cols)
        placeholders = ",".join(["%s"] * len(cols))  #'%s,%s,...'
        cols_on_dup = tuple([col for col in cols if not col in keys])
        on_dup = on_dup or self.on_dup(cols_on_dup)
        statement = f"insert into {table} ({cols_str}) values ({placeholders}) as vals on duplicate key update {on_dup}"
        debug("statement %(statement)s", {"statement": statement})
        return statement

    def insert_select_on_duplicate_key_update(
        self, table_from: str, table_into: str, colmap: Dict[str, str], keys: str
    ) -> None:
        """Execute insert on duplicate key update statement.
        Use pre or post MySQL-8.0.19 form for: insert ... on duplicate key insert
        https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html
        :param table_from: table from which to select data
        :param table_into: table into which to insert data
        :param colmap: dict mapping cols in table_from to cols in table_into
        :param keys: keys
        :returns: statement eg insert into t1 (d0,d1,d2,d3) select * from (select s0,s1,s2,s3 from t0) as vals(a0,a1,a2,a3) on duplicate key update d2=vals.a2,d3=vals.a3
        """
        debug(table_from)
        debug(table_into)
        debug(colmap)
        debug(keys)
        statement = MysqlHandler.insert_select_on_duplicate_key_update_statement(
            table_from, table_into, colmap, keys
        )
        debug(statement)
        self.execute(statement)

    @staticmethod
    def insert_select_on_duplicate_key_update_statement(
        table_from: str, table_into: str, colmap: Dict[str, str], keys: str
    ) -> str:
        """Create insert on duplicate key update statement.
        https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html
        :param table_from: table from which to select data
        :param table_into: table into which to insert data
        :param colmap: dict mapping cols in table_from to cols in table_into
        :param keys: keys
        :returns: statement eg insert into t1 (d0,d1,d2,d3) select * from (select s0,s1,s2,s3 from t0) as vals(a0,a1,a2,a3) on duplicate key update d2=vals.a2,d3=vals.a3
        Google Cloud SQL defaults to MySQL-8.0.18 but can upgrade: gcloud sql instances patch sheffieldsolar --database-version=MYSQL_8_0_28
        """
        debug(table_from)
        debug(table_into)
        debug(colmap)
        debug(keys)

        cols_from = colmap.keys()
        cols_into = colmap.values()

        col2alias = {col: f"alias{i}" for (i, col) in enumerate(cols_into)}
        aliases = col2alias.values()

        aliases_str = ",".join(aliases)
        cols_into_str = ",".join(cols_into)
        cols_from_str = ",".join(cols_from)

        on_dup = [
            f"{col_into}=vals.{col2alias[col_into]}"
            for col_into in cols_into
            if not col_into in keys
        ]
        on_dup_str = ",".join(on_dup)
        statement = (
            f"insert into {table_into} ({cols_into_str}) select * from "
            f"(select {cols_from_str} from {table_from}) as vals({aliases_str}) "
            f"on duplicate key update {on_dup_str}"
        )
        debug("statement %(statement)s", {"statement": statement})
        return statement

    def on_dup(self, col_names: Tuple[str, ...]) -> str:
        """
        Use post MySQL-8.0.19 syntax (use alias vals.X not values(X) for values for): insert ... on duplicate key insert
        https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html
        :param col_names: database table column names
        :returns: on duplicate key update string
        """
        on_dup_array = [f"{col_name}=vals.{col_name}" for col_name in col_names]
        on_dup = ",".join(on_dup_array)
        debug("on_dup %(on_dup)s", {"on_dup": on_dup})
        return on_dup

    def reset_auto_increment(self, table: str, col: str) -> int:
        """Reset autoincrement to next above max.
        (Dangerous as another process may insert row(s) between select max and set auto_increment.)
        """
        statement = f"select max({col}) from {table}"
        max_val = self.fetchone(statement)[0]
        statement = f"alter table {table} auto_increment = {max_val+1}"
        print(statement)
        self.execute(statement)
        statement = f'select auto_increment from information_schema.tables where table_schema = database() and table_name = "{table}"'
        auto_increment = self.fetchone(statement)[0]
        return auto_increment
