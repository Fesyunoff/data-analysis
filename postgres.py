#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import psycopg2

def get_client(verbose=False):
    conn_info = {
        'port': '5432',
        'host': '127.0.0.1',
        'database': 'postgres',
        'password': 'pass',
        'user': 'user'}
    if verbose:
        print(conn_info)
    client = psycopg2.connect(**conn_info)
    return client

def execute_query(query, conn, verbose=False):
    if verbose:
        print(query)
    conn = get_client()
    cur = conn.cursor()
    try:
        cur.execute(query)
    except psycopg2.Error as e:
       print(e.pgerror) 
    cur.close()
    conn.commit()

class Events:

    def __init__(self, schema, input_table, conn):
        self._schema_name = schema
        self._table_name = input_table
        self._conn = conn
        execute_query(self.create_table_SQL(), self._conn, True)

    def create_table_SQL(self):
        return """
    CREATE SCHEMA IF NOT EXISTS %(schema_name)s;

    CREATE TABLE IF NOT EXISTS %(schema_name)s.%(table_name)s(
          id SERIAL PRIMARY KEY,
          user_id VARCHAR NOT NULL, 
          event_timestamp TIMESTAMP NOT NULL,
          amount FLOAT8
         );""" % {
        'schema_name': self._schema_name,
        'table_name': self._table_name,
    }

    def insert_amount_SQL(self, user, ts, amount):
        return """

    INSERT INTO %(schema_name)s.%(table_name)s(
    user_id, event_timestamp, amount)
    VALUES (
    %(user_id)d, to_timestamp(%(ts)f), %(amount)f
         );""" % {
        'schema_name': self._schema_name,
        'table_name': self._table_name,
        'user_id': user,
        'ts': ts,
        'amount': amount
    }
    

    def insert_event_SQL(self, user, ts):
        return """

    INSERT INTO %(schema_name)s.%(table_name)s(
    user_id, event_timestamp)
    VALUES (
    %(user_id)d, to_timestamp(%(ts)f)
         );""" % {
        'schema_name': self._schema_name,
        'table_name': self._table_name,
        'user_id': user,
        'ts': ts
    }
    

class Historicity:

    def __init__(self, schema, input_table, conn):
        self._schema_name = schema
        self._table_name = input_table
        self._conn = conn
        execute_query(self.create_table_SQL(), self._conn, True)

    def create_table_SQL(self):
        return """
    CREATE SCHEMA IF NOT EXISTS %(schema_name)s;

    CREATE TABLE IF NOT EXISTS %(schema_name)s.%(table_name)s(
        record_id INTEGER NOT NULL,
        value_date DATE NOT NULL,
        value INTEGER NOT NULL,
        CONSTRAINT record_date PRIMARY KEY(record_id, value_date)
         );""" % {
        'schema_name': self._schema_name,
        'table_name': self._table_name,
    }

    def insert_value_SQL(self, insert_id, ts, value):
        return """

    INSERT INTO %(schema_name)s.%(table_name)s(
    record_id, value_date, value)
    VALUES (
    %(insert_id)d, date(to_timestamp(%(ts)f)), %(value)d
         );""" % {
        'schema_name': self._schema_name,
        'table_name': self._table_name,
        'insert_id': insert_id,
        'ts': ts,
        'value': value
    }
    
