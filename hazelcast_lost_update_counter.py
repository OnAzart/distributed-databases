from functools import partial
from random import random
from time import sleep

from tools import *
import hazelcast

client = hazelcast.HazelcastClient()
uku_map = client.get_map('uku')
key = 'counter'
# uku_map.put(key, 0)
uku_map.set(key, 0)


def nonblocking_adding():
    # res: 16 sec
    # not expected value - 10033
    cur_counter = uku_map.get(key).result()
    print(cur_counter)
    uku_map.put(key, cur_counter+1)


def pessimistic_blocking_adding():
    # not as expected – 6
    uku_map.lock(key)
    try:
        cur_counter = uku_map.get(key).result()
        counter = cur_counter + 1
        print(counter)
        uku_map.put(key, counter)
        print(f'{threading.get_ident()}: start {cur_counter} , real {uku_map.get(key).result()}, expected {counter}\n')
    finally:
        uku_map.unlock(key)


def optimistic_blocking_adding():
    # res: 110 sec
    # as expected
    while True:
        cur_counter = uku_map.get(key).result()
        counter = cur_counter + 1
        if uku_map.replace_if_same(key, cur_counter, counter).result():
            print(f'{threading.get_ident()}: start {cur_counter} , real {uku_map.get(key).result()}, expected {counter}\n')
            break
        else:
            print(False)


def atomic_long_cp_adding():
    # res: 45 sec
    # as expected
    counter_al = client.cp_subsystem.get_atomic_long('fst')
    value = counter_al.get().result()
    new_value = counter_al.increment_and_get().result()
    print(value, new_value)


def add_10_000_to_hazelcast(realisation_function):
    for i in range(10000):
        realisation_function()


def main():
    update_functions = [atomic_long_cp_adding]
    for update_function in update_functions:
        duration = round(
            run_with_duration_decorator(
                partial(launch_10_threads_for, partial(add_10_000_to_hazelcast, update_function))
            ),
            1)
        print(f'{update_function.__name__} – {duration} sec')


if __name__ == '__main__':
    main()
