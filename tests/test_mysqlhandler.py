'''
Created on 2 Mar 2015

@author: ph1jb

'''

from datetime import timezone
import datetime
from logging import warning
from mysql import connector
from mysql.connector.errorcode import (CR_SERVER_LOST, CR_SERVER_LOST_EXTENDED,
                                       ER_LOCK_WAIT_TIMEOUT)
from mysql.connector.errors import (DatabaseError, DataError, Error,
                                    IntegrityError, InterfaceError,
                                    InternalError, NotSupportedError,
                                    OperationalError, ProgrammingError)
import os
from os.path import dirname
from typing import List
import unittest
from unittest.case import skip
from unittest.mock import MagicMock, call, patch
import yaml

from mysql_handler import MysqlHandler


# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
class TestMysqlHandler(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        '''Create database and tables for testing mysql_handler.
        Uses sql file
        Does not use mysql_handler.
        '''
        cls.mysql_options = {
            'autocommit': True,
            'database': 'database_is_not_set',
            'get_warnings': True,
            'host': 'database_host_is_not_set',
            'password': 'database_password_is_not_set',
            'raise_on_warnings': False,
            'time_zone': 'UTC',
            'user': 'database_user_is_not_set',
            }
        secrets_file = os.path.join(dirname(__file__), '../secrets/', 'test_secrets.yml')
        with open(secrets_file, 'r') as fin:
            try:
                secrets = yaml.safe_load(fin)
            except yaml.YAMLError as ex:
                warning(ex)
        cls.mysql_options.update(secrets.get('mysql_options'))
        # print(f'cls.mysql_options: {cls.mysql_options}')
        cls.cnx = connector.connect(**cls.mysql_options)
        cursor = cls.cnx.cursor()
        # Create database and tables for mysql_handler tests
        schema = f'{dirname(__file__)}/test_mysql_schema.sql'
        with open(schema) as fin:
            unused = list(cursor.execute(fin.read(), multi=True))

        cursor.close()

    @classmethod
    def tearDownClass(cls):
        '''Drop database and tables for testing mysql_handler; close database connection
        Does not use mysql_handler.
        '''
        cursor = cls.cnx.cursor()
        cls.table = 'testtable'
        statement = f'drop table if exists {cls.table}'
        # cursor.execute(statement)
        cursor.close()
        cls.cnx.close()

    def setUp(self):
        self.table = 'testtable'

        # Populate tables
        self.cols = ('id', 'first_name', 'last_name')
        self.cols_str = ','.join(self.cols)
        statement = f'insert into {self.table} ({self.cols_str}) values (%s,%s,%s)'
        self.rows = [
            (1, 'Ann', 'Awk',),
            (2, 'Bob', 'Bash'),
            (3, 'Cath', 'Curl',),
            (4, 'Dave', 'Dig'),
            (5, 'Eve', 'Other'),
            ]
        # Create MysqlHandler instance afresh for each test
        self.mysql_options = TestMysqlHandler.mysql_options
        self.mysql_options.update({'raise_on_warnings': True, })
        self.mysql_handler = MysqlHandler(self.mysql_options)

        # self.cnx = TestMysqlHandler.cnx
        self.cnx = self.mysql_handler.cnx
        self.truncate()
        cursor = self.cnx.cursor()
        cursor.executemany(statement, self.rows)
        cursor.close()

    def tearDown(self):
        self.truncate()

    def truncate(self):
        '''Truncate the test table after each test.
        '''
        statement = f'truncate table {self.table}'
        cursor = self.cnx.cursor()
        unused = cursor.execute(statement)
        cursor.close()

    def test_connection_missing_mysql_options(self):
        self.assertRaises(connector.errors.OperationalError, MysqlHandler, {})

    def test__init__cnx(self):
        cnx = 1
        mh = MysqlHandler(self.mysql_options, cnx=cnx)
        self.assertEqual(mh.cnx, cnx)
        self.assertDictEqual(mh.mysql_options, self.mysql_options)

    def test_with_mysql_handler(self):
        with MysqlHandler(self.mysql_options) as mh:
            self.assertIsInstance(mh, MysqlHandler)

    # @skip('Fouls up truncate')
    @patch('mysql_handler.MysqlHandler.close')
    def test_close(self, close):
        '''Raises no exception.
        https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlconnection-disconnect.html
        '''
        self.mysql_handler.close()
        close.assert_called_once_with()

    def test_connection_bad_host(self):
        mysql_options = {
            'database': 'mysql database not set',
            'host': 'mysql host not set',
            'password': 'mysql password not set',
            'user': 'mysql user not set',
        }
        self.assertRaises(connector.errors.Error, MysqlHandler, mysql_options=mysql_options)

    def test_insert_on_duplicate_key_update_statement(self):
        table = 'mytable'
        cols = ['a', 'b', 'c', 'd' ]
        keys = ['a', 'b']
        on_dup_str = 'c=vals.c,d=vals.d'
        actual = self.mysql_handler.insert_on_duplicate_key_update_statement(table, cols, keys)
        expected = f'insert into {table} (a,b,c,d) values (%s,%s,%s,%s) as vals on duplicate key update {on_dup_str}'
        self.assertEqual(actual, expected)

    def test_insert_on_duplicate_key_update_statement_pre_mysql8019(self):
        self.mysql_handler.mysql_version = '8.0.18'
        table = 'mytable'
        cols = ['a', 'b', 'c', 'd' ]
        keys = ['a', 'b']
        on_dup = 'c=VALUES(c),d=VALUES(d)'
        actual = self.mysql_handler.insert_on_duplicate_key_update_statement(table, cols, keys, on_dup)
        expected = f'insert into {table} (a,b,c,d) values (%s,%s,%s,%s)  on duplicate key update {on_dup}'
        self.assertEqual(actual, expected)

    def test_insert_select_on_duplicate_key_update_statement(self):
        table_from = 'table_from'
        table_into = 'table_into'
        colmap = {'col_from0':'col_into0', 'col_from1':'col_into1', 'col_from2':'col_into2', 'col_from3*2':'col_into3', }
        keys = ['col_into0', 'col_into1', ]
        actual = self.mysql_handler.insert_select_on_duplicate_key_update_statement(table_from, table_into, colmap, keys)
        expected = ('insert into table_into (col_into0,col_into1,col_into2,col_into3) '
                    'select * from (select col_from0,col_from1,col_from2,col_from3*2 from table_from) '
                    'as vals(alias0,alias1,alias2,alias3) '
                    'on duplicate key update col_into2=vals.alias2,col_into3=vals.alias3')
        self.assertEqual(actual, expected)

    def test_execute(self):
        self.truncate()
        statement = f'insert into {self.table} (first_name,last_name) values("A","B")'
        self.mysql_handler.execute(statement)
        #
        # Check rows have been inserted
        statement = f'select first_name, last_name from {self.table}'
        row = self.mysql_handler.fetchone(statement)
        expected = ('A', 'B')
        self.assertTupleEqual(row, expected)

    def test_executemany(self):
        self.truncate()
        statement = f'insert into {self.table} (id, first_name, last_name) values (%s,%s,%s)'
        self.mysql_handler.executemany(statement, self.rows)
        # Check row3 and row4 have been inserted
        statement = f'select id, first_name, last_name from {self.table}'
        cursor = self.cnx.cursor()
        cursor.execute(statement)
        rows = cursor.fetchall()
        cursor.close()
        self.assertListEqual(rows, self.rows)

    def test_executemany_chunked(self):
        self.truncate()
        chunk_size = 2
        statement = f'insert into {self.table} (id, first_name, last_name) values (%s,%s,%s)'
        self.mysql_handler.executemany_chunked(statement, self.rows, chunk_size)
        statement = f'select id, first_name, last_name from {self.table} order by id'
        cursor = self.cnx.cursor()
        cursor.execute(statement)
        rows = cursor.fetchall()
        cursor.close()
        self.assertListEqual(rows, self.rows)

    def test_executemulti(self):
        self.truncate()
        statements = (f'insert into {self.table} (first_name,last_name) values("A","B");'
                      f'insert into {self.table} (first_name,last_name) values("C","D")')
        self.mysql_handler.executemulti(statements)
        # Check rows have been inserted
        statement = f'select first_name, last_name from {self.table}'
        cursor = self.cnx.cursor()
        cursor.execute(statement)
        rows = cursor.fetchall()
        cursor.close()
        expected = [('A', 'B'), ('C', 'D'), ]
        self.assertListEqual(rows, expected)

    def test_fetchall(self):
        statement = f'select id, first_name, last_name from {self.table} order by id'
        actual = self.mysql_handler.fetchall(statement)
        print(actual)
        self.assertListEqual(actual, self.rows)

    def test_fetchall_data(self):
        statement = f'select id, first_name, last_name from {self.table} where id in(%s,%s,%s,%s,%s) order by id'
        params = (1, 2, 3, 4, 5)  # [(1,), (2,), (3,), (4,), (5,), ]
        actual = self.mysql_handler.fetchall(statement, params=params)
        self.assertListEqual(actual, self.rows)

    def test_fetchone(self):
        statement = f'select id, first_name, last_name from {self.table} where id = 1  order by id'
        row = self.mysql_handler.fetchone(statement)
        self.assertTupleEqual(row, self.rows[0])

    def test_fetchone_param_table_name(self):
        '''Cannot parameterise table name. Can only parameterise values.
        '''
        statement = 'select id, first_name, last_name from %(table)s where id = 1  order by id'
        params = {'table': self.table}
        self.assertRaises(connector.errors.ProgrammingError, self.mysql_handler.fetchone, statement, params=params)

    def test_fetchone_params(self):
        statement = f'select id, first_name, last_name from {self.table} where id = %s  order by id'
        params = (1,)
        row = self.mysql_handler.fetchone(statement, params=params)
        self.assertTupleEqual(row, self.rows[0])

    def test_fetchone_no_rows_found(self):
        statement = f'select id, first_name, last_name from {self.table} where id is null'
        row = self.mysql_handler.fetchone(statement)
        self.assertIsNone(row)

    def test_insert_on_duplicate_key_update(self):
        self.truncate()
        self.mysql_handler.insert_on_duplicate_key_update(self.table, self.cols, self.cols[:1], self.rows)
        # Re-insert to show that on duplicate key update works
        self.mysql_handler.insert_on_duplicate_key_update(self.table, self.cols, self.cols[:1], self.rows)
        # Check rows have been inserted
        statement = f'select * from {self.table}'
        rows = self.mysql_handler.fetchall(statement)
        self.assertListEqual(rows, self.rows)

    def test_insert_on_duplicate_key_update_on_dup(self):
        self.truncate()
        on_dup = 'first_name=vals.first_name,last_name=UPPER(vals.last_name)'
        self.mysql_handler.insert_on_duplicate_key_update(self.table, self.cols, self.cols[:1], self.rows, on_dup=on_dup)
        # Re-insert to show that on duplicate key update works
        self.mysql_handler.insert_on_duplicate_key_update(self.table, self.cols, self.cols[:1], self.rows, on_dup=on_dup)
        # Check rows have been inserted
        statement = f'select * from {self.table}'
        rows = self.mysql_handler.fetchall(statement)
        rows_uc = self.rows = [
            (1, 'Ann', 'AWK',),
            (2, 'Bob', 'BASH'),
            (3, 'Cath', 'CURL',),
            (4, 'Dave', 'DIG'),
            (5, 'Eve', 'OTHER'),
            ]

        self.assertListEqual(rows, self.rows)

    def test_on_dup(self):
        col_names = ['a', 'b', 'c', ]
        actual = self.mysql_handler.on_dup(col_names)
        expected = 'a=vals.a,b=vals.b,c=vals.c'
        self.assertEqual(actual, expected)

    def test_on_dup_pre_mysql8019(self):
        self.mysql_handler.mysql_version = '8.0.18'
        col_names = ['a', 'b', 'c', ]
        actual = self.mysql_handler.on_dup(col_names)
        expected = 'a=VALUES(a),b=VALUES(b),c=VALUES(c)'
        self.assertEqual(actual, expected)

    def test_retry(self):

        def my_method(statement, params):
            return (statement, params)

        statement = "select {};"
        params = '123'
        response = self.mysql_handler.retry(my_method, statement, params, nretries=2)
        expected = (statement, params)
        self.assertTupleEqual(response, expected)

    def test_retry_ERRORS(self):
        for errno in (CR_SERVER_LOST, CR_SERVER_LOST_EXTENDED, ER_LOCK_WAIT_TIMEOUT):
            mock_method = MagicMock(side_effect=connector.Error(errno=errno))
            statement = "select {};"
            params = '123'
            with self.assertRaises(connector.Error) as cm:
                self.mysql_handler.retry(mock_method, statement, params, nretries=1)
                mock_method.assert_called_with(statement, params)
                self.assertEqual(cm.exception.errno, errno)

    def test_retry_bad_database_host(self):
        mock_method = MagicMock(side_effect=connector.Error)
        statement = 'select {}'
        params = '123'
        with self.assertRaises(connector.Error) as cm:
            self.mysql_handler.retry(mock_method, statement, params, nretries=2)
        calls = [call(statement, params), ] * 2
        mock_method.assert_has_calls(calls)

    def test_close_cursor(self):
        '''
        https://dev.mysql.com/doc/connector-python/en/connector-python-api-errors-error.html
        # Error handling
        DataError, DatabaseError, Error, IntegrityError, InterfaceError, InternalError,
        NotSupportedError, OperationalError, ProgrammingError, Warning
        '''
        with patch.object(self.mysql_handler.cnx, 'cursor') as cursor_fn:  # pylint: disable:undefined-variable
            cursor = cursor_fn()
            cursor.fetchall = MagicMock(return_value=self.rows)
            statement = f'select * from {self.table}'
            actual = self.mysql_handler.fetchall(statement)
            self.assertListEqual(actual, self.rows)
            cursor.close.assert_called_once_with()

    def test_close_cursor_on_exception(self):
        '''
        https://dev.mysql.com/doc/connector-python/en/connector-python-api-errors-error.html
        # Error handling
        DataError, DatabaseError, Error, IntegrityError, InterfaceError, InternalError,
        NotSupportedError, OperationalError, ProgrammingError, Warning
        '''
        for ex in (DataError, DatabaseError, Error, IntegrityError, InterfaceError, InternalError,
                   NotSupportedError, OperationalError, ProgrammingError, Warning):
            with self.subTest():
                with patch.object(self.mysql_handler.cnx, 'cursor') as cursor_fn:  # pylint: disable:undefined-variable
                    cursor = cursor_fn()
                    cursor.execute = MagicMock(side_effect=ex())
                    statement = 'select * from XXX'  # non existent table
                    self.assertRaises(ex, self.mysql_handler.execute, statement)
                    cursor.close.assert_called_once_with()

    def test_reset_auto_increment(self):
        table = 'testtable'
        col = 'id'
        statement = f'select max({col}) from {table}'
        max_val = self.mysql_handler.fetchone(statement)[0]
        auto_increment = self.mysql_handler.reset_auto_increment(table, col)
        self.assertEqual(auto_increment, max_val + 1)

    def test_truncate(self):
        table = 'testtable'
        self.mysql_handler.truncate(table)
        statement = f'select * from {table}'
        rows = self.mysql_handler.fetchall(statement)
        self.assertListEqual(rows, [])

    def test_type_conf(self):

        def lister(list_arg:List[str]):
            return list_arg

        list_arg = lister(0)
        print(type(list_arg))
        print(list_arg)
        self.assertEqual(list_arg, 0)

    def test_insert_on_duplicate_key_long(self):
        # n = 24
        n = 6
        table = 'reading30compact'
        keys = ['date', 'ss_id']
        vals = range(1, n)
        cols = keys + [f't{i}' for i in vals]
        date = '2022-01-01'
        ss_id = 1
        rows = [(date, ss_id, *vals)]
        self.mysql_handler.insert_on_duplicate_key_update(table, cols, keys, rows)

    def test_timestamp_types(self):
        '''
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
        '''
        statement='select date from reading30compact'
        (timestamp,) =self.mysql_handler.fetchone(statement)
        
        print(timestamp,type(timestamp),timestamp.tzinfo)
        timestamp_aware=timestamp.astimezone(timezone.utc)
        print(timestamp_aware,type(timestamp_aware),timestamp_aware.tzinfo)
        
        timestamp2:datetime.datetime
        (timestamp2,) =self.mysql_handler.fetchone(statement)
        print(timestamp2,type(timestamp2),timestamp2.tzinfo)
        

if __name__ == "__main__":
    unittest.main()
