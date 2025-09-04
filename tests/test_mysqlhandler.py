"""
Created on 2 Mar 2015

@author: ph1jb

"""

from argparse import Namespace
from copy import deepcopy
from datetime import timezone
from logging import warning
from mysql import connector
from mysql.connector.errors import (
    DatabaseError,
    DataError,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from mysqlhandler.mysql_handler import MysqlHandler
from pathlib import Path
from unittest.mock import MagicMock, patch
import logging
import pytest
import yaml


# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
TABLE = "testtable"


###########################################
##Fixtures: scope Module
###########################################


@pytest.fixture(scope="module")
def secrets_file():
    return Path(Path(__file__).parent.parent, "secrets", "test_secrets.yml")


@pytest.fixture(scope="module")
def secrets(secrets_file):
    """Read once. Do not update."""
    with open(secrets_file, "r", encoding="utf8") as fin:
        try:
            return yaml.safe_load(fin)
        except yaml.YAMLError as ex:
            warning(ex)


@pytest.fixture(scope="module")
def mysql_options_orig(secrets):
    """Read once. Do not update."""
    return secrets.get("mysql_options")


@pytest.fixture(scope="module")
def mysql_handler(mysql_options_orig):
    """Connect to database server, create temporary tables (once) because "if not exist"."""
    mh = MysqlHandler(mysql_options_orig)
    yield mh
    mh.close()


@pytest.fixture(autouse=True, scope="module")
def db_init(mysql_handler):
    """Create temporary tables (once) because "if not exist".
    autouse=True ensures it is called.
    Scope module ensures it only runs once."""
    statement = (
        f"CREATE TEMPORARY TABLE if not exists {TABLE} ("
        "id int NOT NULL AUTO_INCREMENT,"
        'first_name varchar(45) default "AA" NOT NULL,'
        'last_name varchar(45) default "BB"  NOT NULL,'
        " PRIMARY KEY (id)) ENGINE=InnoDB;"
    )
    mysql_handler.execute(statement)
    t_str = ",".join([f"t{i} float DEFAULT NULL" for i in range(1, 30)])
    statement = (
        "CREATE TEMPORARY TABLE if not exists reading30compact ("
        "date timestamp NOT NULL DEFAULT '1970-01-02 00:00:00',"
        "ss_id int unsigned NOT NULL,"
        f"{t_str},"
        "PRIMARY KEY (date,ss_id)) ENGINE=InnoDB;"
    )
    mysql_handler.execute(statement)


###########################################
##Fixtures (default scope: function)
###########################################


@pytest.fixture()
def mysql_options_default():
    """Refreshed before each use. OK to update."""
    return {
        "autocommit": True,
        "database": "database_is_not_set",
        "host": "database_host_is_not_set",
        "password": "database_password_is_not_set",
        "raise_on_warnings": True,
        "time_zone": "UTC",
        "user": "database_user_is_not_set",
    }


@pytest.fixture()
def mysql_options(mysql_options_orig):
    """Refreshed before each use. OK to update."""
    return deepcopy(mysql_options_orig)


@pytest.fixture()
def fixture():
    class Fixture:
        def __init__(self):
            self.rows = [
                (1, "Ann", "Awk"),
                (2, "Bob", "Bash"),
                (3, "Cath", "Curl"),
                (4, "Dave", "Dig"),
                (5, "Eve", "Other"),
            ]
            self.table = TABLE
            self.cols = ("id", "first_name", "last_name")
            self.cols_str = ",".join(self.cols)

    return Fixture()


@pytest.fixture()
def truncate(mysql_handler):
    mysql_handler.execute("truncate test_mysql.testtable")


@pytest.fixture()
def populate(fixture, mysql_handler, truncate):
    statement = f"insert into {fixture.table} ({fixture.cols_str}) values (%s,%s,%s)"
    mysql_handler.executemany(statement, fixture.rows)


class TestMysqlHandlerInit:
    """Test MysqlHandler.__init__"""

    def test__init__cnx(self, mysql_options):
        cnx = connector.connect(**mysql_options)
        mh = MysqlHandler(mysql_options, cnx=cnx)
        assert mh.cnx == cnx
        assert mh.mysql_options == mysql_options

    def test_with_mysql_handler(self, mysql_options):
        cnx = connector.connect(**mysql_options)
        with MysqlHandler(mysql_options, cnx=cnx) as mh:
            assert isinstance(mh, MysqlHandler)
            assert mh.cnx == cnx
            assert mh.mysql_options == mysql_options

    @pytest.mark.parametrize(
        "k,v",
        [
            ("database", "mysql database not set"),
            ("host", "mysql host not set"),
            ("password", "mysql password not set"),
            ("user", "mysql user not set"),
        ],
    )
    def test_connection_error(self, k, v, mysql_options):
        mysql_options.update({k: v})
        with pytest.raises(connector.errors.Error):
            MysqlHandler(mysql_options=mysql_options)

    @pytest.mark.parametrize(
        "arg, key",
        [
            ("mysql_database", "database"),
            ("mysql_host", "host"),
            ("mysql_password", "password"),
            ("mysql_user", "user"),
        ],
    )
    def test_override_mysql_options_env(self, arg, key, mysql_options_default) -> None:
        mysql_args = {
            "mysql_database": None,
            "mysql_host": None,
            "mysql_password": None,
            "mysql_user": None,
        }
        mysql_args.update({arg: arg})
        config: Namespace = Namespace(**mysql_args, mysql_options=mysql_options_default)
        MysqlHandler.override_mysql_options(config)
        assert config.mysql_options.get(key) == arg


class TestMysqlHandlerTruncated:
    """Test methods needing truncated (empty) tables."""

    def test_execute(self, fixture, mysql_handler, truncate):
        statement = f'insert into {fixture.table} (first_name,last_name) values("A","B")'
        mysql_handler.execute(statement)
        #
        # Check rows have been inserted
        statement = f"select first_name, last_name from {fixture.table} limit 1"
        row = mysql_handler.fetchone(statement)
        expected = ("A", "B")
        assert row == expected

    def test_executemany(self, fixture, mysql_handler, truncate):
        statement = f"insert into {fixture.table} (id, first_name, last_name) values (%s,%s,%s)"
        mysql_handler.executemany(statement, fixture.rows)
        # Check row3 and row4 have been inserted
        statement = f"select id, first_name, last_name from {fixture.table}"
        rows = mysql_handler.fetchall(statement)
        assert rows == fixture.rows


class TestMysqlHandlerPopulated:
    """Tests which need a populated table."""

    def test_insert_on_duplicate_key_update_statement(self, fixture, mysql_handler, populate):
        table = "mytable"
        cols = ["a", "b", "c", "d"]
        keys = ["a", "b"]
        on_dup_str = "c=vals.c,d=vals.d"
        actual = mysql_handler.insert_on_duplicate_key_update_statement(table, cols, keys)
        expected = f"insert into {table} (a,b,c,d) values (%s,%s,%s,%s) as vals on duplicate key update {on_dup_str}"
        assert actual == expected

    def test_insert_select_on_duplicate_key_update_statement(
        self, fixture, mysql_handler, populate
    ):
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
        actual = mysql_handler.insert_select_on_duplicate_key_update_statement(
            table_from, table_into, colmap, keys
        )
        expected = (
            "insert into table_into (col_into0,col_into1,col_into2,col_into3) "
            "select * from (select col_from0,col_from1,col_from2,col_from3*2 from table_from) "
            "as vals(alias0,alias1,alias2,alias3) "
            "on duplicate key update col_into2=vals.alias2,col_into3=vals.alias3"
        )
        assert actual == expected

    def test_fetchall(self, fixture, mysql_handler, populate):
        statement = f"select id, first_name, last_name from {fixture.table} order by id"
        actual = mysql_handler.fetchall(statement)
        assert actual == fixture.rows

    def test_fetchall_data(self, fixture, mysql_handler, populate):
        statement = f"select id, first_name, last_name from {fixture.table} where id in(%s,%s,%s,%s,%s) order by id"
        params = (1, 2, 3, 4, 5)  # [(1,), (2,), (3,), (4,), (5,), ]
        actual = mysql_handler.fetchall(statement, params=params)
        assert actual == fixture.rows

    def test_fetchone(self, fixture, mysql_handler, populate):
        statement = (
            f"select id, first_name, last_name from {fixture.table} where id = 1  order by id"
        )
        row = mysql_handler.fetchone(statement)
        assert row == fixture.rows[0]

    def test_fetchone_param_table_name(self, fixture, mysql_handler, populate):
        """Cannot parameterise table name. Can only parameterise values."""
        statement = "select id, first_name, last_name from %(table)s where id = 1  order by id"
        params = {"table": fixture.table}
        with pytest.raises(connector.errors.ProgrammingError):
            mysql_handler.fetchone(
                statement,
                params=params,
            )

    def test_fetchone_params(self, fixture, mysql_handler, populate):
        statement = (
            f"select id, first_name, last_name from {fixture.table} where id = %s  order by id"
        )
        params = (1,)
        row = mysql_handler.fetchone(statement, params=params)
        assert row == fixture.rows[0]

    def test_fetchone_no_rows_found(self, fixture, mysql_handler, populate):
        statement = f"select id, first_name, last_name from {fixture.table} where id is null"
        row = mysql_handler.fetchone(statement)
        assert row is None

    def test_insert_on_duplicate_key_update(self, fixture, mysql_handler, populate):
        mysql_handler.insert_on_duplicate_key_update(
            fixture.table, fixture.cols, fixture.cols[:1], fixture.rows
        )
        # Re-insert to show that on duplicate key update works
        mysql_handler.insert_on_duplicate_key_update(
            fixture.table, fixture.cols, fixture.cols[:1], fixture.rows
        )
        # Check rows have been inserted
        statement = f"select * from {fixture.table}"
        rows = mysql_handler.fetchall(statement)
        assert rows == fixture.rows

    def test_insert_on_duplicate_key_update_on_dup(self, fixture, mysql_handler, populate):
        on_dup = "first_name=vals.first_name,last_name=UPPER(vals.last_name)"
        mysql_handler.insert_on_duplicate_key_update(
            fixture.table, fixture.cols, fixture.cols[:1], fixture.rows, on_dup=on_dup
        )
        # Re-insert to show that on duplicate key update works
        mysql_handler.insert_on_duplicate_key_update(
            fixture.table, fixture.cols, fixture.cols[:1], fixture.rows, on_dup=on_dup
        )
        # Check rows have been inserted
        statement = f"select * from {fixture.table}"
        rows = mysql_handler.fetchall(statement)
        rows_uc = [
            (1, "Ann", "AWK"),
            (2, "Bob", "BASH"),
            (3, "Cath", "CURL"),
            (4, "Dave", "DIG"),
            (5, "Eve", "OTHER"),
        ]
        assert rows == rows_uc

    def test_on_dup(self, fixture, mysql_handler, populate):
        col_names = ["a", "b", "c"]
        actual = mysql_handler.on_dup(col_names)
        expected = "a=vals.a,b=vals.b,c=vals.c"
        assert actual == expected

    def test_close_cursor(self, fixture, mysql_handler, populate):
        """
        https://dev.mysql.com/doc/connector-python/en/connector-python-api-errors-error.html
        # Error handling
        DataError, DatabaseError, Error, IntegrityError, InterfaceError, InternalError,
        NotSupportedError, OperationalError, ProgrammingError, Warning
        """
        with patch.object(
            mysql_handler.cnx, "cursor"
        ) as cursor_fn:  # pylint: disable:undefined-variable
            cursor = cursor_fn()
            cursor.fetchall = MagicMock(return_value=fixture.rows)
            statement = f"select * from {fixture.table}"
            actual = mysql_handler.fetchall(statement)
            assert actual == fixture.rows
            cursor.close.assert_called_once_with()

    @pytest.mark.parametrize(
        "ex",
        [
            DataError,
            DatabaseError,
            connector.errors.Error,
            IntegrityError,
            InterfaceError,
            InternalError,
            NotSupportedError,
            OperationalError,
            ProgrammingError,
            Warning,
        ],
    )
    def test_close_cursor_on_exception(self, ex, fixture, mysql_handler, populate):
        """
        https://dev.mysql.com/doc/connector-python/en/connector-python-api-errors-error.html
        # Error handling
        DataError, DatabaseError, Error, IntegrityError, InterfaceError, InternalError,
        NotSupportedError, OperationalError, ProgrammingError, Warning
        """
        with patch.object(
            mysql_handler.cnx, "cursor"
        ) as cursor_fn:  # pylint: disable:undefined-variable
            cursor = cursor_fn()
            cursor.execute = MagicMock(side_effect=ex())
            statement = "select * from XXX"  # non existent table
            with pytest.raises(ex):
                mysql_handler.execute(statement)
            cursor.close.assert_called_once_with()

    # def test_reset_auto_increment(self):
    #     table = 'testtable'
    #     col = 'id'
    #     statement = f'select max({col}) from {table}'
    #     max_val = mysql_handler.fetchone(statement)[0]
    #     auto_increment = mysql_handler.reset_auto_increment(table, col)
    #     self.assertEqual(auto_increment, max_val + 1)

    def test_insert_on_duplicate_key_long(self, fixture, mysql_handler, populate):
        n = 26
        table = "reading30compact"
        keys = ["date", "ss_id"]
        vals = range(1, n)
        cols = keys + [f"t{i}" for i in vals]
        date = "2022-01-01"
        ss_id = 1
        rows = [(date, ss_id, *vals)]
        mysql_handler.insert_on_duplicate_key_update(table, cols, keys, rows)

    def test_timestamp_types(self, fixture, mysql_handler, populate):
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
        (timestamp,) = mysql_handler.fetchone(statement)

        print(timestamp, type(timestamp), timestamp.tzinfo)
        timestamp_aware = timestamp.astimezone(timezone.utc)
        print(timestamp_aware, type(timestamp_aware), timestamp_aware.tzinfo)

        (timestamp2,) = mysql_handler.fetchone(statement)
        print(timestamp2, type(timestamp2), timestamp2.tzinfo)


class TestMysqlExceptionHandling:
    """TestMysqlExceptionHandling.
    pytest"""

    def test_context_manager(self, caplog, mysql_options):
        caplog.set_level(logging.INFO)
        msg = "hi"
        errno = 123
        error = connector.Error(msg, 123)
        with pytest.raises(connector.Error, match=msg) as cm_ex:
            with MysqlHandler(mysql_options):
                raise connector.Error(msg=msg, errno=errno)
            # Exception
            assert cm_ex.type == connector.Error
            assert str(cm_ex.value) == str(error)
        # Logging
        for record in caplog.records:
            assert record.levelname == "CRITICAL"
        assert msg in caplog.text

    def test_execute_exception(self, caplog, mysql_options):
        caplog.set_level(logging.INFO)
        with pytest.raises(connector.Error) as cm_ex:
            with MysqlHandler(mysql_options) as mh:
                statement = "select no_col from no_table"
                mh.execute(statement)
            # Exception
            assert cm_ex.type == connector.Error
        # Logging
        for record in caplog.records:
            assert record.levelname == "CRITICAL"
            print(record)

    def test_fetchone_exception(self, caplog, mysql_options):
        caplog.set_level(logging.INFO)
        with pytest.raises(connector.Error) as cm_ex:
            with MysqlHandler(mysql_options) as mh:
                statement = "select no_col from no_table"
                mh.fetchone(statement)
            # Exception
            assert cm_ex.type == connector.Error
        # Logging
        for record in caplog.records:
            assert record.levelname == "CRITICAL"
            print(record)

    def test_fetchall_exception(self, caplog, mysql_options):
        caplog.set_level(logging.INFO)
        with pytest.raises(connector.Error) as cm_ex:
            with MysqlHandler(mysql_options) as mh:
                statement = "select no_col from no_table"
                mh.fetchall(statement)
            # Exception
            assert cm_ex.type == connector.Error
        # Logging
        for record in caplog.records:
            assert record.levelname == "CRITICAL"
            print(record)

    def test_insert_select_on_duplicate_key_update_exception(self, caplog, mysql_options):
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
        with pytest.raises(connector.Error) as cm_ex, MysqlHandler(mysql_options) as mh:
            mh.insert_on_duplicate_key_update(table_from, table_into, colmap, keys)
        # Exception
        assert cm_ex.type == connector.errors.InterfaceError
        # Logging
        for record in caplog.records:
            assert record.levelname == "CRITICAL"
