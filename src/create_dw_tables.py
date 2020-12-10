#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec  3 16:35:09 2020

@author: UpcaseM

Create tables in DW
"""

import os
import glob
import faker
import random
import numpy as np
import pandas as pd
from scipy.stats import norm
import psycopg2
import psycopg2.extras as pe
import sql_queries


def drop_tables(conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    cur = conn.cursor()
    for query in sql_queries.drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(conn):
    """
    Creates each table using the queries in `create_table_queries` list.
    """
    cur = conn.cursor()
    for query in sql_queries.create_table_queries:
        cur.execute(query)
        conn.commit()
    print('Tables are created!')


def import_log_files(data_folder, conn):
    """
    Import data into staging_events.
    """
    lst_files = glob.glob(data_folder + r'/*.csv')
    c = conn.cursor()
    for i, file in enumerate(lst_files):
        with open(file, 'r') as f:
            next(f)  # Skip the header row.
            c.copy_from(f,
                        'staging_events',
                        sep=',',
                        columns=['event_time',
                                 'event_type',
                                 'product_id',
                                 'category_id',
                                 'category_code',
                                 'brand',
                                 'price',
                                 'user_id',
                                 'user_session'])
        print(f'File {i+1} is imported!')
    conn.commit()
    print('Log files are imported!')


def import_data(conn):
    """
    Import data into events, products, orders, time and users.
    """
    cur = conn.cursor()
    for query in sql_queries.insert_table_queries:
        cur.execute(query)
        conn.commit()

    # Insert user data into users table
    sql = """
        SELECT
            DISTINCT
            USER_ID
        FROM EVENTS
    """
    df_users = pd.read_sql(sql, conn)
    num_users = df_users.shape[0]

    fake = faker.Faker('zh_CN')
    faker.Faker.seed(123)
    # Create df_users
    df_users['user_name'] = [fake.user_name() for _ in range(num_users)]
    df_users['name'] = [fake.name() for _ in range(num_users)]
    df_users['gender'] = [generate_gender() for _ in range(num_users)]
    df_users['mail'] = [fake.email() for _ in range(num_users)]
    df_users['province'] = [fake.province() for _ in range(num_users)]
    # The age of the users consists of two parts. 80% is created by norm
    # and the remaining is created by random numbers between 20 and 60.
    users_norm = int(num_users*.8)
    df_users['age'] = np.append(np.round(norm.rvs(30, 4, users_norm)),
                                      [random.randint(20, 60)
                                       for _ in range(num_users - users_norm)])
    pe.execute_batch(cur, sql_queries.users_table_insert, df_users.values)
    conn.commit()
    print('Data is imported!')


def generate_gender():
    """
    Generate random gender. Assume 80% of the customers are female.
    """
    if random.random() <= 0.2:
        return 'M'
    else:
        return 'F'


def main():
    '''
    - Establishes connection with the postgresql database.

    - Drops all the tables.

    - Creates all tables needed.

    - Finally, closes the connection.
    '''
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    conn = psycopg2.connect(user='test',
                            password='123456',
                            host='127.0.0.1',
                            port='5432',
                            database="user_profile_dw")

    drop_tables(conn)
    create_tables(conn)
    import_log_files(project_dir + r'/input/ecommerce_cosmetics', conn)
    import_data(conn)

    # Drop staging table
    cur = conn.cursor()
    cur.execute(sql_queries.staging_events_table_drop)
    conn.commit()

    conn.close()


if __name__ == "__main__":
    main()
    print('Completed!')
