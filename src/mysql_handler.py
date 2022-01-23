'''
Created on 2 Mar 2015

@author: ph1jb

Docs on installing and using 3 Python mysql connectors: mysql.connector, mysqlDB, pymysql
https://www.a2hosting.co.uk/kb/developer-corner/mysql/connecting-to-mysql-using-python

'''
from logging import debug
from time import sleep
from typing import  Tuple, List, Any

from mysql import connector
from mysql.connector.errorcode import ER_LOCK_WAIT_TIMEOUT, CR_SERVER_LOST, CR_SERVER_LOST_EXTENDED

Rows = List[Tuple[Any, ...]]


class MysqlHandler():

    '''Insert and query database.
    constructor takes database connection to allow testing with sqlite and running with mysql.
    Create cnx if cnx=None and mysql_options are passed in
    '''

    def __init__(self, mysql_options, cnx=None):
        self.cnx = cnx
        self.mysql_options = mysql_options
        if cnx:
            pass
        else:
            debug('MysqlHandler.__init__.mysql_options: %(mysql_options)s', {'mysql_options':self.mysql_options})
            self.cnx = connector.connect(**self.mysql_options)
            self.mysql_version = self.fetchone('SELECT VERSION();')[0]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cnx.close()

    def close(self):
        '''Close database connection.
        Raises no exception.
        https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlconnection-disconnect.html
        '''
        self.cnx.close()

    def create_on_duplicate_key_update_statement(self, table: str, col_names: Tuple[str, ...], on_dup_str: str) -> str:
        '''Insert data into database table.
        Use pre or post MySQL-8.0.19 form for: insert ... on duplicate key insert
        https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html

        :param table: into which to insert data
        :param rows:
        :type rows: list of tuples [(0,1,2,),(3,4,5,),]
        :param col_names: database table column names
        :param on_dup_str: on duplicate key string, e.g. 'a=vals.a,b=vals.b'
        '''
        col_names_str = ','.join(col_names)
        placeholders = ','.join(['%s'] * len(col_names))
        if self.mysql_version < '8.0.19':
            statement = f'insert into {table} ({col_names_str}) values ({placeholders}) on duplicate key update {on_dup_str}'
        else:
            statement = f'insert into {table} ({col_names_str}) values ({placeholders}) as vals on duplicate key update {on_dup_str}'
        debug('statement %(statement)s', {'statement':statement})
        return statement

    def execute(self, statement) -> None:
        '''MySQL execute statement (typically insert)
        '''
        cursor = self.cnx.cursor()
        cursor.execute(statement)
        cursor.close()

    def executemany(self, statement:str, rows:Rows) -> None:
        '''MySQL execute statement (typically insert) with multiple rows.
        :param statement: e.g. insert into table t (a,b,c) values(%s,%s,%s)
        :param rows: e.g.: [(0,1,2),(3,4,5),]
        '''
        cursor = self.cnx.cursor()
        cursor.executemany(statement, rows)
        cursor.close()

    def executemany_chunked(self, statement, rows, chunk_size) -> None:
        '''MySQL execute statement (typically insert) with multiple rows.
        Break data into chunks to avoid max packet size errors
        :param statement: e.g. insert into table t (a,b,c) values(%s,%s,%s)
        :param rows: e.g.: [(0,1,2),(3,4,5),]
        :param chunk_size: 123456
        '''
        len_rows = len(rows)
        for i in range(0, len_rows, chunk_size):
            i_1 = min((i + chunk_size, len_rows))
            self.executemany(statement, rows[i:i_1])

    def executemulti(self, statement):
        '''MySQL query with multiple statements.
        Use this rarely, perhaps to execute an SQL script to create tables etc.
        :param statement: e.g create table t1 ...; create table t2 ...
        '''
        debug('query: %(statement)s', {'statement':statement})
        cursor = self.cnx.cursor()
        result_iterator = cursor.execute(statement, multi=True)
        results = list(result_iterator)
        cursor.close()
        return results

    def fetchone(self, statement) -> Tuple[Any]:
        '''MySQL query (typically select) with one row result expected
        :param statement: e.g. select * from table t where id = 1
        :return: e.g. (0,1,2)
        '''
        debug('query: %(statement)s', {'statement':statement})
        cursor = self.cnx.cursor()
        cursor.execute(statement)
        row = cursor.fetchone()
        cursor.close()
        return row

    def fetchall(self, statement) -> Rows:
        '''MySQL query (typically select) with many rows expected
         (but not so many as to exhaust memory)
        :param statement: e.g. select * from table t
        :return: e.g. [(0,1,2),(3,4,5),]
        '''
        debug('self.cnx: %(cnx)s', {'cnx':self.cnx})
        debug(statement)
        cursor = self.cnx.cursor()
        cursor.execute(statement)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def insert_on_duplicate_key_update(self, table:str, cols:List[str], cols_on_dup:List[str], rows:Rows) -> None:
        '''MySQL insert query
        :param table: table to insert into
        :param cols: columns to insert
        :param cols_on_dup: on duplicate key update columns
        :param rows: list of tuples of data to insert
        '''
        on_dup = self.on_dup(cols_on_dup)
        statement = self.create_on_duplicate_key_update_statement(table, cols, on_dup)
        debug(statement)
        self.executemany(statement, rows)

    def on_dup(self, col_names: Tuple[str, ...]) -> str:
        '''
        Use pre or post MySQL-8.0.19 form for: insert ... on duplicate key insert
        https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html
        :param col_names: database table column names
        :returns: on duplicate key update string
        '''
        if self.mysql_version < '8.0.19':
            # pre 8.0.19
            on_dup_array = [f'{col_name}=VALUES({col_name})' for col_name in col_names]
        else:
            # 8.0.19 and after
            on_dup_array = [f'{col_name}=vals.{col_name}' for col_name in col_names]
        on_dup = ','.join(on_dup_array)
        debug('on_dup %(on_dup)s', {'on_dup':on_dup})
        return on_dup

    def retry(self, method, statement, data=None, nretries=8):
        '''Wrapper to retry MySQL queries up to nretries times with exponential back-off
        eg if nretries = 8 (default) retry (on error) after 1, 2, 4, 8, 16, 32, 64 and 128 seconds
        Usage: retry(query, statement, data)
        :param method: database table column names
        :param statement: database table column names
        :param data: database table column names
        :param nretries: database table column names
        :returns: whatever the method returns
        '''
        if data is None:
            data = []
        for i in range(nretries):
            try:
                return method(statement, data)
            except connector.Error as err:
                if i < (nretries - 1) and err.errno in (CR_SERVER_LOST, CR_SERVER_LOST_EXTENDED, ER_LOCK_WAIT_TIMEOUT):
                    sleep(2 ** i)  # retry
                else:
                    raise  # raise exception (exits the for loop)

    def retry2(self, method, *args, nretries=8, **kwargs):
        '''Wrapper to retry MySQL queries up to nretries times with exponential back-off
        eg if nretries = 8 (default) retry (on error) after 1, 2, 4, 8, 16, 32, 64 and 128 seconds
        Call this: retry(query, statement, data)
        :param method: database table column names
        :param statement: database table column names
        :param data: database table column names
        :param nretries: database table column names
        :returns: whatever the method returns
        '''
        for i in range(nretries):
            try:
                return method(*args, **kwargs)
            except connector.Error as err:
                if i < (nretries - 1) and err.errno in (CR_SERVER_LOST, CR_SERVER_LOST_EXTENDED, ER_LOCK_WAIT_TIMEOUT):
                    sleep(2 ** i)  # retry
                else:
                    raise  # raise exception (exits the for loop)

