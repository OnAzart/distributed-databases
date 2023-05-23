# Comparison of lost update solutions. By case, time.
from functools import partial

# lost_update – 14.0 sec
# inplace_update – 12.9 sec
# row_level_locking_update – 20.8 sec
# optimistic_concurrency_control_update – 92.2 sec

from psycopg2 import connect
from tools import *

config = {
    "host": "localhost",
    "dbname": "uku",
    "user": "nazar",
    "password": "<pass>",
    "port": "5432"
}

table_name = 'dist_db.user_counter'


def connect_to_postgres():
    return connect(**config)  # conn


# FIRST TASK. Updates are losing, because of rapid change of them. --- 27 seconds
def lost_update(conn, cursor):
    cursor.execute(f"SELECT counter FROM {table_name} WHERE user_id = 1")
    current_counter = cursor.fetchone()[0]
    counter = current_counter + 1
    cursor.execute(f"update {table_name} set counter = {counter} where user_id = 1")
    conn.commit()


# SECOND TASK. Inplacing inside of DB. --- 14 seconds
def inplace_update(conn, cursor):
    cursor.execute(f"update {table_name} set counter = counter + 1 where user_id = 1")
    conn.commit()


# THIRD TASK. Block row, which were read, for update. Lock inside current transaction.  --- 24 seconds
def row_level_locking_update(conn, cursor):
    cursor.execute(f"SELECT counter FROM {table_name} WHERE user_id = 1 FOR UPDATE")
    current_counter = cursor.fetchone()[0]
    counter = current_counter + 1
    cursor.execute(f"update {table_name} set counter = {counter} where user_id = 1")
    conn.commit()


# FOURTH TASK.A lot of idle updates (no result). Updating version and counter. --- 96 seconds
def optimistic_concurrency_control_update(conn, cursor):
    while True:
        cursor.execute(f"SELECT counter, version FROM {table_name} WHERE user_id = 1")
        current_counter, current_version = cursor.fetchone()
        counter = current_counter + 1
        version = current_version + 1
        cursor.execute(f"update {table_name} set counter = {counter}, version = {version} "
                       f"where user_id = 1 and version = {current_version}")
        conn.commit()
        count = cursor.rowcount
        # print(f'{counter}c - {version}v. {count}rc')
        if count > 0:
            break


def add_10_000_to_postgres(realisation_function):
    with connect_to_postgres() as conn:
        with conn.cursor() as cursor:
            for i in range(10_000):
                realisation_function(conn, cursor)
                conn.commit()


def main():
    # update_function = optimistic_concurrency_control_update
    update_functions = [lost_update, inplace_update, row_level_locking_update, optimistic_concurrency_control_update]
    # update_functions = [lost_update, inplace_update, row_level_locking_update, optimistic_concurrency_control_update]
    for update_function in update_functions:
        duration = round(
            run_with_duration_decorator(
                partial(launch_10_threads_for, partial(add_10_000_to_postgres, update_function))
            ), 1)
        print(f'{update_function.__name__} – {duration} sec')


if __name__ == '__main__':
    main()
