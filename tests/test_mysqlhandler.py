"""
Created on 2 Mar 2015

@author: ph1jb

"""
from datetime import timezone
import datetime
from logging import warning
import logging
import os
from os.path import dirname
from pathlib import Path
from typing import List, Dict, Any
import unittest
from unittest.mock import MagicMock, patch

import mysql.connector
from mysql.connector.errors import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
import pytest
import yaml

from mysql_handler import MysqlHandler


# import _mysql_connector  # type: ignore
MYSQL_OPTIONS_DEFAULT = {
    "autocommit": True,
    "database": "mysql database not set",
    "host": "mysql host not set",
    "password": "mysql password not set",
    "raise_on_warnings": True,
    "time_zone": "UTC",
    "user": "mysql user not set",
}

secrets_file = Path.joinpath(Path(__file__).parent, "..", "secrets", "test_secrets.yml")


@pytest.fixture(autouse=True, scope="module")
def secrets() -> Dict[str, Any]:
    """Load secrets file.
    :return: secrets dict
    """
    os.environ["loglevel"] = "DEBUG"

    with open(secrets_file, "r") as fin:
        try:
            # return yaml.safe_load(fin)
            return yaml.safe_load(fin)
        except yaml.YAMLError as ex:
            warning(ex)
            raise


@pytest.fixture(autouse=True, scope="module")
def mysql_options(secrets) -> Dict[str, Any]:
    """Load mysql_options.
    :return: mysql_options
    """
    os.environ["loglevel"] = "DEBUG"
    mysql_options = MYSQL_OPTIONS_DEFAULT
    mysql_options.update(secrets["mysql_options"])
    return mysql_options


def setUpModule():
    """Connect to database server, create temporary tables (once) because "if not exist"."""
    global CURSOR
    global MYSQL_HANDLER
    global MYSQL_OPTIONS
    global ROWS
    global TABLE
    MYSQL_OPTIONS = {
        "autocommit": True,
        "database": "database_is_not_set",
        "host": "database_host_is_not_set",
        "password": "database_password_is_not_set",
        "raise_on_warnings": True,
        "time_zone": "UTC",
        "user": "database_user_is_not_set",
    }
    secrets_file = os.path.join(dirname(__file__), "../secrets/", "test_secrets.yml")
    with open(secrets_file, "r") as fin:
        try:
            secrets = yaml.safe_load(fin)
        except yaml.YAMLError as ex:
            warning(ex)
    MYSQL_OPTIONS.update(secrets.get("mysql_options"))
    cnx = mysql.connector.connect(**MYSQL_OPTIONS)
    CURSOR = cnx.cursor()
    TABLE = "testtable"
    statement = f"""CREATE TEMPORARY TABLE if not exists {TABLE} (
                id int NOT NULL AUTO_INCREMENT,
                first_name varchar(45) default "AA" NOT NULL,
                last_name varchar(45) default "BB"  NOT NULL,
                 PRIMARY KEY (id)
                ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
                """
    CURSOR.execute(statement)
    statement = f"""CREATE TEMPORARY TABLE if not exists reading30compact (
                date timestamp NOT NULL DEFAULT '1970-01-02 00:00:00',
                ss_id int unsigned NOT NULL,
                t1 float DEFAULT NULL,
                t2 float DEFAULT NULL,
                t3 float DEFAULT NULL,
                t4 float DEFAULT NULL,
                t5 float DEFAULT NULL,
                t6 float DEFAULT NULL,
                t7 float DEFAULT NULL,
                t8 float DEFAULT NULL,
                t9 float DEFAULT NULL,
                t10 float DEFAULT NULL,
                t11 float DEFAULT NULL,
                t12 float DEFAULT NULL,
                t13 float DEFAULT NULL,
                t14 float DEFAULT NULL,
                t15 float DEFAULT NULL,
                t16 float DEFAULT NULL,
                t17 float DEFAULT NULL,
                t18 float DEFAULT NULL,
                t19 float DEFAULT NULL,
                t20 float DEFAULT NULL,
                t21 float DEFAULT NULL,
                t22 float DEFAULT NULL,
                t23 float DEFAULT NULL,
                t24 float DEFAULT NULL,
                t25 float DEFAULT NULL,
                t26 float DEFAULT NULL,
                t27 float DEFAULT NULL,
                t28 float DEFAULT NULL,
                t29 float DEFAULT NULL,
                t30 float DEFAULT NULL,
                PRIMARY KEY (date,ss_id)
                ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC COMMENT='Prepared Passiv Historic data: SSF ss_id (not Passiv install_id). Omits cols: meter_id,missing_periods, daily_total';
                """
    CURSOR.execute(statement)
    MYSQL_HANDLER = MysqlHandler(mysql_options={}, cnx=cnx)


class Fixture(unittest.TestCase):
    def setUp(self):
        global CURSOR
        global MYSQL_HANDLER
        global MYSQL_OPTIONS
        global ROWS
        global TABLE
        self.cursor = CURSOR
        self.mysql_handler = MYSQL_HANDLER
        self.mysql_options = MYSQL_OPTIONS
        self.rows = [
            (
                1,
                "Ann",
                "Awk",
            ),
            (2, "Bob", "Bash"),
            (
                3,
                "Cath",
                "Curl",
            ),
            (4, "Dave", "Dig"),
            (5, "Eve", "Other"),
        ]
        self.table = TABLE


# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring


class TestMysqlHandlerInit(Fixture):
    """Test MysqlHandler.__init__"""

    def setUp(self):
        super().setUp()

    def test__init__cnx(self):
        cnx = mysql.connector.connect(**self.mysql_options)
        mh = MysqlHandler(self.mysql_options, cnx=cnx)
        self.assertEqual(mh.cnx, cnx)
        self.assertDictEqual(mh.mysql_options, self.mysql_options)

    def test_with_mysql_handler(self):
        with MysqlHandler(self.mysql_options) as mh:
            self.assertIsInstance(mh, MysqlHandler)

    def test_connection_bad_host(self):
        self.mysql_options = {
            "database": "mysql database not set",
            "host": "mysql host not set",
            "password": "mysql password not set",
            "user": "mysql user not set",
        }
        self.assertRaises(Error, MysqlHandler, mysql_options=self.mysql_options)


# pylint: disable=missing-function-docstring


class TestMysqlHandlerTruncated(Fixture):
    """Test methods needing truncated (empty) tables."""

    def setUp(self):
        super().setUp()
        self.mysql_handler.truncate(self.table)

    def test_execute(self):
        statement = f'insert into {self.table} (first_name,last_name) values("A","B")'
        self.mysql_handler.execute(statement)
        #
        # Check rows have been inserted
        statement = f"select first_name, last_name from {self.table} limit 1"
        row = self.mysql_handler.fetchone(statement)
        expected = ("A", "B")
        self.assertTupleEqual(row, expected)

    def test_executemany(self):
        self.mysql_handler.truncate(self.table)
        statement = (
            f"insert into {self.table} (id, first_name, last_name) values (%s,%s,%s)"
        )
        self.mysql_handler.executemany(statement, self.rows)
        # Check row3 and row4 have been inserted
        statement = f"select id, first_name, last_name from {self.table}"
        rows = self.mysql_handler.fetchall(statement)
        self.assertListEqual(rows, self.rows)

    def test_executemulti(self):
        self.mysql_handler.truncate(self.table)
        statements = (
            f'insert into {self.table} (first_name,last_name) values("A","B");'
            f'insert into {self.table} (first_name,last_name) values("C","D")'
        )
        self.mysql_handler.execute(statements, multi=True)
        # Check rows have been inserted
        statement = f"select first_name, last_name from {self.table}"
        rows = self.mysql_handler.fetchall(statement)
        expected = [
            ("A", "B"),
            ("C", "D"),
        ]
        self.assertListEqual(rows, expected)


class TestMysqlHandlerPopulated(Fixture):
    """Tests which need a populated table."""

    def setUp(self):
        super().setUp()
        self.mysql_handler.truncate(self.table)
        # Populate table
        self.cols = ("id", "first_name", "last_name")
        self.cols_str = ",".join(self.cols)
        statement = f"insert into {self.table} ({self.cols_str}) values (%s,%s,%s)"
        self.cursor.executemany(statement, self.rows)

    def test_insert_on_duplicate_key_update_statement(self):
        table = "mytable"
        cols = ["a", "b", "c", "d"]
        keys = ["a", "b"]
        on_dup_str = "c=vals.c,d=vals.d"
        actual = self.mysql_handler.insert_on_duplicate_key_update_statement(
            table, cols, keys
        )
        expected = f"insert into {table} (a,b,c,d) values (%s,%s,%s,%s) as vals on duplicate key update {on_dup_str}"
        self.assertEqual(actual, expected)

    def test_insert_select_on_duplicate_key_update_statement(self):
        table_from = "table_from"
        table_into = "table_into"
        colmap = {
            "col_from0": "col_into0",
            "col_from1": "col_into1",
            "col_from2": "col_into2",
            "col_from3*2": "col_into3",
        }
        keys = [
            "col_into0",
            "col_into1",
        ]
        actual = self.mysql_handler.insert_select_on_duplicate_key_update_statement(
            table_from, table_into, colmap, keys
        )
        expected = (
            "insert into table_into (col_into0,col_into1,col_into2,col_into3) "
            "select * from (select col_from0,col_from1,col_from2,col_from3*2 from table_from) "
            "as vals(alias0,alias1,alias2,alias3) "
            "on duplicate key update col_into2=vals.alias2,col_into3=vals.alias3"
        )
        self.assertEqual(actual, expected)

    def test_fetchall(self):
        statement = f"select id, first_name, last_name from {self.table} order by id"
        actual = self.mysql_handler.fetchall(statement)
        self.assertListEqual(actual, self.rows)

    def test_fetchall_data(self):
        statement = f"select id, first_name, last_name from {self.table} where id in(%s,%s,%s,%s,%s) order by id"
        params = (1, 2, 3, 4, 5)  # [(1,), (2,), (3,), (4,), (5,), ]
        actual = self.mysql_handler.fetchall(statement, params=params)
        self.assertListEqual(actual, self.rows)

    def test_fetchone(self):
        statement = f"select id, first_name, last_name from {self.table} where id = 1  order by id"
        row = self.mysql_handler.fetchone(statement)
        self.assertTupleEqual(row, self.rows[0])

    def test_fetchone_param_table_name(self):
        """Cannot parameterise table name. Can only parameterise values."""
        statement = (
            "select id, first_name, last_name from %(table)s where id = 1  order by id"
        )
        params = {"table": self.table}
        self.assertRaises(
            ProgrammingError,
            self.mysql_handler.fetchone,
            statement,
            params=params,
        )

    def test_fetchone_params(self):
        statement = f"select id, first_name, last_name from {self.table} where id = %s  order by id"
        params = (1,)
        row = self.mysql_handler.fetchone(statement, params=params)
        self.assertTupleEqual(row, self.rows[0])

    def test_fetchone_no_rows_found(self):
        statement = (
            f"select id, first_name, last_name from {self.table} where id is null"
        )
        row = self.mysql_handler.fetchone(statement)
        self.assertIsNone(row)

    def test_insert_on_duplicate_key_update(self):
        # self.truncate()
        self.mysql_handler.insert_on_duplicate_key_update(
            self.table, self.cols, self.cols[:1], self.rows
        )
        # Re-insert to show that on duplicate key update works
        self.mysql_handler.insert_on_duplicate_key_update(
            self.table, self.cols, self.cols[:1], self.rows
        )
        # Check rows have been inserted
        statement = f"select * from {self.table}"
        rows = self.mysql_handler.fetchall(statement)
        self.assertListEqual(rows, self.rows)

    def test_insert_on_duplicate_key_update_on_dup(self):
        # self.truncate()
        on_dup = "first_name=vals.first_name,last_name=UPPER(vals.last_name)"
        self.mysql_handler.insert_on_duplicate_key_update(
            self.table, self.cols, self.cols[:1], self.rows, on_dup=on_dup
        )
        # Re-insert to show that on duplicate key update works
        self.mysql_handler.insert_on_duplicate_key_update(
            self.table, self.cols, self.cols[:1], self.rows, on_dup=on_dup
        )
        # Check rows have been inserted
        statement = f"select * from {self.table}"
        rows = self.mysql_handler.fetchall(statement)
        rows_uc = [
            (
                1,
                "Ann",
                "AWK",
            ),
            (2, "Bob", "BASH"),
            (
                3,
                "Cath",
                "CURL",
            ),
            (4, "Dave", "DIG"),
            (5, "Eve", "OTHER"),
        ]

        self.assertListEqual(rows, rows_uc)

    def test_on_dup(self):
        col_names = [
            "a",
            "b",
            "c",
        ]
        actual = self.mysql_handler.on_dup(col_names)
        expected = "a=vals.a,b=vals.b,c=vals.c"
        self.assertEqual(actual, expected)

    def test_close_cursor(self):
        """
        https://dev.mysql.com/doc/connector-python/en/connector-python-api-errors-error.html
        # Error handling
        DataError, DatabaseError, Error, IntegrityError, InterfaceError, InternalError,
        NotSupportedError, OperationalError, ProgrammingError, Warning
        """
        with patch.object(
            self.mysql_handler.cnx, "cursor"
        ) as cursor_fn:  # pylint: disable:undefined-variable
            cursor = cursor_fn()
            cursor.fetchall = MagicMock(return_value=self.rows)
            statement = f"select * from {self.table}"
            actual = self.mysql_handler.fetchall(statement)
            self.assertListEqual(actual, self.rows)
            cursor.close.assert_called_once_with()

    def test_close_cursor_on_exception(self):
        """
        https://dev.mysql.com/doc/connector-python/en/connector-python-api-errors-error.html
        # Error handling
        DataError, DatabaseError, Error, IntegrityError, InterfaceError, InternalError,
        NotSupportedError, OperationalError, ProgrammingError, Warning
        """
        for ex in (
            DataError,
            DatabaseError,
            Error,
            IntegrityError,
            InterfaceError,
            InternalError,
            NotSupportedError,
            OperationalError,
            ProgrammingError,
            Warning,
        ):
            with self.subTest():
                with patch.object(
                    self.mysql_handler.cnx, "cursor"
                ) as cursor_fn:  # pylint: disable:undefined-variable
                    cursor = cursor_fn()
                    cursor.execute = MagicMock(side_effect=ex())
                    statement = "select * from XXX"  # non existent table
                    self.assertRaises(ex, self.mysql_handler.execute, statement)
                    cursor.close.assert_called_once_with()

    # def test_reset_auto_increment(self):
    #     table = 'testtable'
    #     col = 'id'
    #     statement = f'select max({col}) from {table}'
    #     max_val = self.mysql_handler.fetchone(statement)[0]
    #     auto_increment = self.mysql_handler.reset_auto_increment(table, col)
    #     self.assertEqual(auto_increment, max_val + 1)

    def test_truncate(self):
        table = "testtable"
        self.mysql_handler.truncate(table)
        statement = f"select * from {table}"
        rows = self.mysql_handler.fetchall(statement)
        self.assertListEqual(rows, [])

    def test_type_conf(self):
        def lister(list_arg: List[str]):
            return list_arg

        list_arg = lister(0)
        print(type(list_arg))
        print(list_arg)
        self.assertEqual(list_arg, 0)

    def test_insert_on_duplicate_key_long(self):
        # n = 6
        # n = 24
        n = 26
        table = "reading30compact"
        keys = ["date", "ss_id"]
        vals = range(1, n)
        cols = keys + [f"t{i}" for i in vals]
        date = "2022-01-01"
        ss_id = 1
        rows = [(date, ss_id, *vals)]
        self.mysql_handler.insert_on_duplicate_key_update(table, cols, keys, rows)
        # print(self.mysql_handler.execute('show tables'))

    def test_timestamp_types(self):
        """
        Determining if an Object is Aware or Naive
        Objects of the date type are always naive.
        An object of type time or datetime may be aware or naive.
        A datetime object d is aware if both of the following hold:
            d.tzinfo is not None
            d.tzinfo.utcoffset(d) does not return None
        Otherwise, d is naive.
        A time object t is aware if both of the following hold:
            t.tzinfo is not None
            t.tzinfo.utcoffset(None) does not return None.
        Otherwise, t is naive.
        The distinction between aware and naive doesn’t apply to timedelta objects.
        """
        statement = "select date from reading30compact"
        (timestamp,) = self.mysql_handler.fetchone(statement)

        print(timestamp, type(timestamp), timestamp.tzinfo)
        timestamp_aware = timestamp.astimezone(timezone.utc)
        print(timestamp_aware, type(timestamp_aware), timestamp_aware.tzinfo)

        (timestamp2,) = self.mysql_handler.fetchone(statement)
        print(timestamp2, type(timestamp2), timestamp2.tzinfo)


class TestMysqlExceptionHandling:
    """TestMysqlExceptionHandling.
    pytest"""

    def test_context_manager(self, caplog, mysql_options):
        caplog.set_level(logging.INFO)
        msg = "hi"
        errno = 123
        error = Error(msg, 123)
        with pytest.raises(Error, match=msg) as cm_ex:
            with MysqlHandler(mysql_options):
                raise Error(msg=msg, errno=errno)
            # Exception
            self.assertEqual(cm_ex.type, Error)
            self.assertEqual(str(cm_ex.value), str(error))
        # Logging
        for record in caplog.records:
            assert record.levelname == "CRITICAL"
        assert msg in caplog.text

    def test_execute_exception(self, caplog, mysql_options):
        caplog.set_level(logging.INFO)
        with pytest.raises(Error) as cm_ex:
            with MysqlHandler(mysql_options) as mh:
                statement = "select no_col from no_table"
                mh.execute(statement)
            # Exception
            assert cm_ex.type == Error
        # Logging
        for record in caplog.records:
            assert record.levelname == "CRITICAL"
            print(record)

    def test_fetchone_exception(self, caplog, mysql_options):
        caplog.set_level(logging.INFO)
        with pytest.raises(Error) as cm_ex:
            with MysqlHandler(mysql_options) as mh:
                statement = "select no_col from no_table"
                mh.fetchone(statement)
            # Exception
            assert cm_ex.type == Error
        # Logging
        for record in caplog.records:
            assert record.levelname == "CRITICAL"
            print(record)

    def test_fetchall_exception(self, caplog, mysql_options):
        caplog.set_level(logging.INFO)
        with pytest.raises(Error) as cm_ex:
            with MysqlHandler(mysql_options) as mh:
                statement = "select no_col from no_table"
                mh.fetchall(statement)
            # Exception
            assert cm_ex.type == Error
        # Logging
        for record in caplog.records:
            assert record.levelname == "CRITICAL"
            print(record)

    def test_insert_select_on_duplicate_key_update_exception(
        self, caplog, mysql_options
    ):
        caplog.set_level(logging.INFO)
        table_from = "table_from"
        table_into = "table_into"
        colmap = {
            "col_from0": "col_into0",
            "col_from1": "col_into1",
            "col_from2": "col_into2",
            "col_from3*2": "col_into3",
        }
        keys = [
            "col_into0",
            "col_into1",
        ]
        with pytest.raises(Error) as cm_ex, MysqlHandler(mysql_options) as mh:
            mh.insert_on_duplicate_key_update(table_from, table_into, colmap, keys)
        # Exception
        assert cm_ex.type == InterfaceError
        # Logging
        for record in caplog.records:
            assert record.levelname == "CRITICAL"

    def test_truncate_exception(self, caplog, mysql_options):
        caplog.set_level(logging.INFO)
        with pytest.raises(Error) as cm_ex:
            with MysqlHandler(mysql_options) as mh:
                table = "no_table"
                mh.truncate(table)
            # Exception
            assert cm_ex.type == Error
        # Logging
        for record in caplog.records:
            assert record.levelname == "CRITICAL"
            print(record)


if __name__ == "__main__":
    unittest.main()
