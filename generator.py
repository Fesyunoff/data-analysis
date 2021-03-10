#!/usr/bin/env python
# -*- coding: utf-8 -*-

import random
import sys
import psycopg2
import postgres as db


def main():
    
    schema = 'analytics'
    verbose = False

    if len(sys.argv) > 1:
        entity = sys.argv[1]
        if len(sys.argv) > 2 and sys.argv[2] == 'v':
            verbose = True 
    else:
        raise Exception("Set params 'e' or 'h' to create table and 'v' to print all msgs")


    if entity == 'e':
        table = 'events'
        conn = db.get_client(verbose)
        a = db.Events(schema, table, conn)
        for i in range(1000):
            user = random.randint(100, 200)
            ts = random.randint(1612137600, 1613260800)
            amount = random.randint(1, 100000) / 100
            is_amount = random.randint(0, 1)
            if is_amount == 1:
                db.execute_query(a.insert_amount_SQL(user, ts, amount), conn, verbose)
            else:
                db.execute_query(a.insert_event_SQL(user, ts), conn, verbose)

    if entity == 'h':
        table = 'historisity'
        conn = db.get_client(verbose)
        a = db.Historicity(schema, table, conn)
        for j in range(25000):
            entry_size = random.randint(30, 50)
            for i in range(entry_size):
                ts = random.randint(1609459200, 1614556800)
                value = random.randint(100, 999)
                db.execute_query(a.insert_value_SQL(j, ts, value), conn, verbose)
            if verbose:
                print(j)
    print('DONE')

if __name__ == "__main__":
    main()  


