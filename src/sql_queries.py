#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec  3 16:50:45 2020

@author: UpcaseM

All queries.
"""

# Parameters
num_samples = 1000

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
users_table_drop = "DROP TABLE IF EXISTS users;"
products_table_drop = "DROP TABLE IF EXISTS products;"
orders_table_drop = "DROP TABLE IF EXISTS orders;"
events_table_drop = "DROP TABLE IF EXISTS events;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
# staging events
staging_events_table_create = ("""

CREATE TABLE staging_events (
    event_id SERIAL PRIMARY KEY,
    event_time TIMESTAMP,
    event_type VARCHAR(20),
    product_id VARCHAR,
    category_id VARCHAR,
    category_code VARCHAR,
    brand VARCHAR,
    price FLOAT,
    user_id VARCHAR,
    user_session VARCHAR
);

""")

# users
user_table_create = ("""

CREATE TABLE users (
    user_id VARCHAR PRIMARY KEY,
    user_name VARCHAR NOT NULL,
    name VARCHAR,
    gender VARCHAR,
    mail VARCHAR,
    province VARCHAR,
    age INTEGER
);

""")

# products
products_table_create = ("""

CREATE TABLE products(
    product_id VARCHAR PRIMARY KEY,
    category_id VARCHAR,
    category_code VARCHAR,
    brand VARCHAR
);

""")

# orders
orders_table_create = ("""

CREATE TABLE orders(
    line_id SERIAL PRIMARY KEY,
    so_number VARCHAR NOT NULL,
    product_id VARCHAR NOT NULL,
    user_id VARCHAR NOT NULL,
    so_created_time TIMESTAMP,
    so_created_date DATE,
    price FLOAT,
    qty INTEGER
);

""")

# events
events_table_create = ("""

CREATE TABLE events(
    event_id SERIAL PRIMARY KEY,
    event_time TIMESTAMP,
    event_date DATE,
    event_type VARCHAR(20),
    product_id VARCHAR,
    price FLOAT,
    user_id VARCHAR,
    user_session VARCHAR
);

""")

# time
time_table_create = ("""

CREATE TABLE time(
    date DATE NOT NULL PRIMARY KEY,
    dayofmonth INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER
);

""")

# INSERT events
events_table_insert = ("""
SELECT setseed(0);
INSERT INTO events (
                    event_time,
                    event_date,
                    event_type,
                    product_id,
                    price,
                    user_id,
                    user_session)
SELECT
    event_time,
    DATE(event_time) as event_date,
    event_type,
    product_id,
    price,
    user_id,
    user_session
FROM staging_events S
WHERE EXISTS (
    SELECT 1
    FROM (
        SELECT DISTINCT
            PRODUCT_ID
        FROM staging_events
        WHERE random()<0.01
        LIMIT {}
    ) T1
    WHERE T1.PRODUCT_ID = S.PRODUCT_ID);
""".format(num_samples))

# INSERT users
users_table_insert = ("""

INSERT INTO users (user_id,
                   user_name,
                   name,
                   gender,
                   mail,
                   province,
                   age)
    VALUES(%s, %s, %s, %s, %s, %s, %s);
""")

# Insert products
products_table_insert = ("""
SELECT setseed(0);
INSERT INTO products
SELECT
    product_id,
    category_id,
    category_code,
    brand
FROM (
    SELECT
        s.product_id,
        category_id,
        category_code,
        brand,
        ROW_NUMBER() OVER(PARTITION BY product_id
                          ORDER BY brand, category_code) as row_num
    FROM staging_events S
    WHERE EXISTS (
        SELECT 1
        FROM (
            SELECT DISTINCT
                PRODUCT_ID
            FROM staging_events
            WHERE random()<0.01
            LIMIT {}
        ) T1
        WHERE T1.PRODUCT_ID = S.PRODUCT_ID
)) T2
WHERE ROW_NUM = 1;
""".format(num_samples))

# Insert orders
orders_table_insert = ("""

INSERT INTO orders(so_number,
                   product_id,
                   user_id,
                   so_created_time,
                   so_created_date,
                   price,
                   qty)
SELECT
    so_number,
    product_id,
    user_id,
    so_created_time,
    so_created_date,
    price,
    qty
FROM (
        SELECT
            user_session as so_number,
            product_id,
            user_id,
            price,
            count(*) as qty
        FROM events
        WHERE event_type = 'purchase'
        GROUP BY so_number, product_id, user_id, price
) tbo
INNER JOIN (
        SELECT
        *
        FROM
        (
            SELECT
                user_session,
                event_time as so_created_time,
                event_date as so_created_date,
                ROW_NUMBER() OVER(PARTITION BY user_session
                                  ORDER BY event_time desc) AS ROW_NUM
            FROM events
            WHERE event_type = 'purchase'
        ) T1
        WHERE ROW_NUM = 1
) tbt ON tbo.so_number = tbt.user_session;
""")

# Insert time
time_table_insert = ("""

INSERT INTO time
SELECT
    DISTINCT
    event_date,
    DATE_PART('day', event_date) AS day,
    DATE_PART('week', event_date) AS week,
    DATE_PART('month', event_date) AS month,
    DATE_PART('year', event_date) AS year,
    DATE_PART('dow', event_date) AS weekday
FROM events;
""")


# QUERY LISTS
create_table_queries = [staging_events_table_create,
                        user_table_create,
                        products_table_create,
                        orders_table_create,
                        events_table_create,
                        time_table_create]
drop_table_queries = [staging_events_table_drop,
                      users_table_drop,
                      products_table_drop,
                      orders_table_drop,
                      events_table_drop,
                      time_table_drop]
insert_table_queries = [events_table_insert,
                        products_table_insert,
                        orders_table_insert,
                        time_table_insert]
