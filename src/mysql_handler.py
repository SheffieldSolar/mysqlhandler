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
from logging import debug, warning
from time import sleep
from typing import Any, Dict, Tuple, Sequence, Optional

import mysql.connector
from mysql.connector.errors import DatabaseError


Rows = Sequence[Tuple[Any, ...]]


class MysqlHandler(AbstractContextManager):

    """Insert and query database.
    constructor takes database connection to allow testing with sqlite and running with mysql.
    Create cnx if cnx=None and mysql_options are passed in
    """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # pylint: disable=unused-argument
        self.cnx.close()

    def __init__(self, mysql_options, cnx=None):
        self.mysql_options = mysql_options
        self.mysql_options_redacted = self.mysql_options.copy()
        self.mysql_options_redacted.update({"password": "REDACTED"})
        if cnx:
            self.cnx = cnx
        else:
            debug(
                "MysqlHandler.__init__.mysql_options: %(mysql_options_redacted)s",
                {"mysql_options_redacted": self.mysql_options_redacted},
            )
            self.cnx = mysql.connector.connect(**self.mysql_options)

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
                    cursor.execute(statement, params, multi=multi)
            except DatabaseError as unused:
                warning("statement %(statement)s", {"statement": statement})
                raise

    def executemany(self, statement: str, rows: Rows) -> None:
        """MySQL execute statement (typically insert) with multiple rows.
        :param statement: e.g. insert into table t (a,b,c) values(%s,%s,%s)
        :param rows: e.g.: [(0,1,2),(3,4,5),]
        """
        with closing(self.cnx.cursor()) as cursor:
            try:
                cursor.executemany(statement, rows)
            except DatabaseError as unused:
                warning("statement %(statement)s", {"statement": statement})
                raise

    def fetchone(
        self, statement, params: Optional[Dict[str, Any]] = None
    ) -> Tuple[Any]:
        """MySQL query (typically select) with one row result expected
         Cannot parameterise table name. Can only parameterise values.
        :param statement: e.g. select * from table t where id = 1
        :param params: dict e.g. {'table0':table0,'table1':table1,}
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
            except DatabaseError as unused:
                warning("statement %(statement)s", {"statement": statement})
                raise

    def fetchall(
        self,
        statement: str,
        params: Optional[Dict[str, Any]] = None,
        multi: bool = False,
    ) -> Rows:
        """MySQL query (typically select) with many rows expected
         (but not so many as to exhaust memory)
         Cannot parameterise table name. Can only parameterise values.
        :param statement: e.g. select * from table t
        :param params: dict e.g. {'table0':table0,'table1':table1,}
        :return: e.g. [(0,1,2),(3,4,5),]
        """
        debug("self.cnx: %(cnx)s", {"cnx": self.cnx})
        debug("statement: %(statement)s", {"statement": statement})
        debug("params: %(params)s", {"params": params})
        if params is None:
            params = {}
        with closing(self.cnx.cursor()) as cursor:
            try:
                cursor.execute(statement, params=params, multi=multi)
                return cursor.fetchall()
            except DatabaseError as unused:
                warning("statement %(statement)s", {"statement": statement})
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
        Use pre or post MySQL-8.0.19 form for: insert ... on duplicate key insert
        https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html

        :param table: into which to insert data
        :param cols: columns to insert
        :param keys: keys
        :param rows: list of tuples [(0,1,2,),(3,4,5,),]
        :param on_dup: on duplicate key string, e.g. 'a=vals.a,b=vals.b'
        """
        cols_str = ",".join(cols)
        placeholders = ",".join(["%s"] * len(cols))
        cols_on_dup = tuple([col for col in cols if not col in keys])
        on_dup = on_dup or self.on_dup(cols_on_dup)
        # if self.mysql_version < '8.0.19':
        #     alias_str = ''
        # else:
        alias_str = "as vals"
        statement = f"insert into {table} ({cols_str}) values ({placeholders}) {alias_str} on duplicate key update {on_dup}"
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
        :param colmap_on_dup: dict mapping cols in table_from to cols in table_into (on duplicate key assignments)
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
        Use pre or post MySQL-8.0.19 form for: insert ... on duplicate key insert
        https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html
        :param table_from: table from which to select data
        :param table_into: table into which to insert data
        :param colmap: dict mapping cols in table_from to cols in table_into
        :param colmap_on_dup: dict mapping cols in table_from to cols in table_into (on duplicate key assignments)
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
        Use pre or post MySQL-8.0.19 form for: insert ... on duplicate key insert
        https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html
        :param col_names: database table column names
        :returns: on duplicate key update string
        """
        # if self.mysql_version < '8.0.19':
        #     # pre 8.0.19
        #     on_dup_array = [f'{col_name}=values({col_name})' for col_name in col_names]
        # else:
        #     # 8.0.19 and after
        on_dup_array = [f"{col_name}=vals.{col_name}" for col_name in col_names]
        on_dup = ",".join(on_dup_array)
        debug("on_dup %(on_dup)s", {"on_dup": on_dup})
        return on_dup

    def retry(self, method, *args, base: float = 2, nretries: int = 8, **kwargs):
        # pylint: disable=unused-argument
        """Wrapper to retry MySQL queries up to nretries times with exponential back-off
        eg if nretries = 6 (default) retry (on error) after 1, 2, 4, 8, 16, 32, 64, 128 seconds
        Call this: retry(query, statement, params)
        :param method: database table column names
        :param args: positional args to method
        :param nretries: number of retries
        :param kwargs: keyword args to method
        :returns: whatever the method returns
        :raises connector.Error: if connector.Error persists after all retries
        """
        ex_last = mysql.connector.Error("")
        for i in range(nretries):
            try:
                print(f"retry: i {i}")
                return method(*args, **kwargs)
            except mysql.connector.Error as ex:
                ex_last = ex
                sleep(base**i)  # retry
        raise ex_last  # raise exception (exits the for loop)

    def reset_auto_increment(self, table: str, col: str) -> int:
        """Reset autoincrement to next above max.
        (Dangerous is another process may insert row(s) between select max and set auto_increment.)
        """
        statement = f"select max({col}) from {table}"
        max_val = self.fetchone(statement)[0]
        statement = f"alter table {table} auto_increment = {max_val+1}"
        print(statement)
        self.execute(statement)
        statement = f'select auto_increment from information_schema.tables where table_schema = database() and table_name = "{table}"'
        auto_increment = self.fetchone(statement)[0]
        return auto_increment

    def truncate(self, table: str, foreign_key_checks: int = 1) -> None:
        """Truncate table (Disable foreign key checks to allow truncate table if foreign keys point to it)"""
        statement = (
            f"SET FOREIGN_KEY_CHECKS = {foreign_key_checks};"  # Set foreign key checks (0 disables, 1 enables)
            f"truncate table {table};"
            f"SET FOREIGN_KEY_CHECKS = 1;"  # Enable foreign key checks
        )
        self.execute(statement, multi=True)
