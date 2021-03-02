#!/usr/bin/env python
# -*- coding: utf-8 -*-

import random
import psycopg2
import postgres as db


def main():
    schema = 'analytics'
    table = 'events'
    conn = db.get_client(True)
    a = db.Storage(schema, table, conn)
    for i in range(1000):
        user = random.randint(100, 200)
        ts = random.randint(1612137600, 1613260800)
        amount = random.randint(1, 100000) / 100
        is_amount = random.randint(0, 1)
        if is_amount == 1:
            db.execute_query(a.insert_amount_SQL(user, ts, amount), conn, True)
        else:
            db.execute_query(a.insert_event_SQL(user, ts), conn, True)

if __name__ == "__main__":
    main()  


