import threading
from time import time


def launch_10_threads_for(realisation_function):
    threads = []
    for i in range(10):
        threads.append(threading.Thread(target=realisation_function))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()


def run_with_duration_decorator(func_to_measure):
    # usually this function need to return result of a function as decorator
    start_time = time()
    result = func_to_measure()
    end_time = time()
    duration = end_time - start_time
    return duration

