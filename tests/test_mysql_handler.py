'''
Created on 2 Mar 2015

@author: ph1jb
'''
import unittest
from unittest.case import skip
from unittest.mock import MagicMock

from mysql import connector
from mysql.connector.errorcode import ER_LOCK_WAIT_TIMEOUT, CR_SERVER_LOST, \
    CR_SERVER_LOST_EXTENDED

from mysql_handler import MysqlHandler


class TestMysqlHandler(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        '''Create database and tables for testing mysql_handler.
        Uses sql file
        Does not use mysql_handler.
        '''
        cls.mysql_options = {
            'autocommit': True,
            'database': 'test',
            'get_warnings': True,
            'host': 'ssfdb2',
            'password': 'bee5Ai[b',
            'raise_on_warnings': False,
            'time_zone': 'UTC',
            'user': 'tester',
            }

        cls.cnx = connector.connect(**cls.mysql_options)
        cursor = cls.cnx.cursor()
        # Create database and tables for mysql_handler tests
        schema = 'test_mysql_schema.sql'
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
        cursor.execute(statement)
        cursor.close()
        cls.cnx.close()

    def setUp(self):
        self.table = 'testtable'

        # Populate tables
        self.cols = ('id', 'first_name', 'last_name')
        self.cols_str = ','.join(self.cols)
        statement = f'insert into {self.table} ({self.cols_str}) values (%s,%s,%s)'
        self.rows = [
                (1, 'Julian', 'Briggs',),
                (2, 'Aldous', 'Everard'),
                (3, 'Jamie', 'Taylor',),
                (4, 'Al', 'Buckley'),
                (5, 'An', 'Other'),
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
        statement = f'truncate table {self.table}'
        cursor = self.cnx.cursor()
        unused = cursor.execute(statement)
        cursor.close()

    def test_connection_missing_mysql_options(self):
        self.assertRaises(connector.errors.OperationalError, MysqlHandler, {})

    def test__init__cnx(self):
        cnx = 1
        mysql_handler = MysqlHandler(self.mysql_options, cnx=cnx)
        self.assertEqual(mysql_handler.cnx, cnx)
        self.assertDictEqual(mysql_handler.mysql_options, self.mysql_options)

    def test_with_mysql_handler(self):
        with MysqlHandler(self.mysql_options) as mysql_handler:
            self.assertIsInstance(mysql_handler, MysqlHandler)

    @unittest.skip('Closing connection raises error from tearDown which calls truncate')
    def test_close(self):
        self.mysql_handler.close()

    def test_close_twice(self):
        '''Raises no exception.
        https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlconnection-disconnect.html
        '''
        self.truncate = MagicMock()
        self.mysql_handler.close()
        self.mysql_handler.close()

    def test_connection_bad_host(self):
        mysql_options = {
            'database': 'mysql database not set',
            'host': 'mysql host not set',
            'password': 'mysql password not set',
            'user': 'mysql user not set',
        }
        self.assertRaises(connector.errors.Error, MysqlHandler, mysql_options=mysql_options)

    @skip('fails due to lack of sqlite')
    def test_context_handler(self):
        connector.connect = MagicMock()
        with MysqlHandler(cnx=self.cnx) as cnx:
            self.assertEqual(cnx, self.cnx)

    def test_create_on_duplicate_key_update_statement(self):
        table = 'mytable'
        col_names = ['a', 'b', 'c', ]
        on_dup_str = 'a=vals.a,b=vals.b,c=vals.c'
        actual = self.mysql_handler.create_on_duplicate_key_update_statement(table, col_names, on_dup_str)
        expected = f'insert into {table} (a,b,c) values (%s,%s,%s) as vals on duplicate key update {on_dup_str}'
        self.assertEqual(actual, expected)

    def test_create_on_duplicate_key_update_statement_pre_mysql8019(self):
        self.mysql_handler.mysql_version = '8.0.18'
        table = 'mytable'
        col_names = ['a', 'b', 'c', ]
        on_dup = 'a=VALUES(a),b=VALUES(b),c=VALUES(c)'
        actual = self.mysql_handler.create_on_duplicate_key_update_statement(table, col_names, on_dup)
        expected = f'insert into {table} (a,b,c) values (%s,%s,%s) on duplicate key update {on_dup}'
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
        rows = self.mysql_handler.fetchall(statement)
        self.assertListEqual(rows, self.rows)

    def test_executemany_chunked(self):
        self.truncate()
        chunk_size = 2
        statement = f'insert into {self.table} (id, first_name, last_name) values (%s,%s,%s)'
        self.mysql_handler.executemany_chunked(statement, self.rows, chunk_size)
        statement = f'select id, first_name, last_name from {self.table} order by id'
        actual = self.mysql_handler.fetchall(statement)
        self.assertListEqual(actual, self.rows)

    def test_executemulti(self):
        self.truncate()
        statements = (f'insert into {self.table} (first_name,last_name) values("A","B");'
                      f'insert into {self.table} (first_name,last_name) values("C","D")')
        self.mysql_handler.executemulti(statements)
        # Check rows have been inserted
        statement = f'select first_name, last_name from {self.table}'
        rows = self.mysql_handler.fetchall(statement)
        expected = [('A', 'B'), ('C', 'D'), ]
        self.assertListEqual(rows, expected)

    def test_fetchall(self):
        statement = f'select id, first_name, last_name from {self.table} order by id'
        actual = self.mysql_handler.fetchall(statement)
        self.assertListEqual(actual, self.rows)

    def test_fetchone(self):
        statement = f'select id, first_name, last_name from {self.table} where id = 1  order by id'
        row = self.mysql_handler.fetchone(statement)
        self.assertTupleEqual(row, self.rows[0])

    def test_insert_on_duplicate_key_update(self):
        self.truncate()
        self.mysql_handler.insert_on_duplicate_key_update(self.table, self.cols, self.cols[1:], self.rows)
        # Re-insert to show that on duplicate key update works
        self.mysql_handler.insert_on_duplicate_key_update(self.table, self.cols, self.cols[1:], self.rows)
        # Check rows have been inserted
        statement = f'select * from {self.table}'
        rows = self.mysql_handler.fetchall(statement)
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

        def my_method(statement, data):
            return (statement, data)

        statement = "select {};"
        data = '123'
        response = self.mysql_handler.retry(my_method, statement, data, nretries=2)
        expected = (statement, data)
        self.assertTupleEqual(response, expected)

    def test_retry2(self):

        def my_method(statement, data):
            return (statement, data)

        statement = "select {};"
        data = '123'
        response = self.mysql_handler.retry2(my_method, statement, data, nretries=2)
        expected = (statement, data)
        self.assertTupleEqual(response, expected)

    def test_retry_ERRORS(self):

        def my_method(statement, data):
            raise connector.Error(errno=errno)

        for errno in (CR_SERVER_LOST, CR_SERVER_LOST_EXTENDED, ER_LOCK_WAIT_TIMEOUT):

            statement = "select {};"
            data = '123'
            with self.assertRaises(connector.Error) as cm:
                self.mysql_handler.retry(my_method, statement, data, nretries=1)
            self.assertEqual(cm.exception.errno, errno)


if __name__ == "__main__":
    unittest.main()
