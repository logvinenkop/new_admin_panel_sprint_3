import time
from functools import wraps

from logger import log


def backoff(
    exception_to_catch,
    message,
    start_sleep_time=0.1,
    factor=2,
    border_sleep_time=10,
):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка.
    Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """

    def wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            cur_sleep_time = start_sleep_time
            try_count = 1
            while True:
                try:
                    return func(*args, **kwargs)
                except exception_to_catch as e:
                    log.error(
                        message + " Next try in {st} seconds".format(st=cur_sleep_time)
                    )
                    time.sleep(cur_sleep_time)
                    if cur_sleep_time < border_sleep_time:
                        cur_sleep_time = start_sleep_time * factor**try_count
                    else:
                        cur_sleep_time = border_sleep_time
                    try_count += 1

        return inner

    return wrapper
