from functools import wraps
from typing import Union, Tuple

from bools.log import Logger
from bools.datetime import Datetime


def catch(_func=None, *, exception: Union[Tuple[type(Exception), ...], type(Exception)] = Exception,
          except_func=None, except_return=None, log='', print_traceback=True):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except exception:
                import traceback
                if except_func is not None:
                    except_func()
                if log:
                    Logger.error(log)
                if print_traceback:
                    print(traceback.print_exc())
                if except_return is not None:
                    return except_return

        return wrapper

    return decorator(_func) if _func else decorator


def timeit(_func=None, *, count=3, return_costs=False):
    def decorator(func):
        @wraps(func)
        def wrapper(*arg, **kwargs):
            current, seconds = Datetime.now().timestamp(), []
            for i in range(count):
                func(*arg, **kwargs)
                now = Datetime.now().timestamp()
                seconds.append(now - current)
                current = now
            Logger.info(f'平均执行时间: {sum(seconds) / count:.3f}s')
            if return_costs:
                return seconds

        return wrapper

    return decorator(_func) if _func else decorator


def parallel(func=None, count: int = None):
    setattr(_outer_func, _FUNC, func)

    def inner(data: list):
        from multiprocessing import Pool
        with Pool(count) as p:
            result = p.map(_outer_func, data)
        return result

    return inner


_FUNC = 'func'


def _outer_func(*args, **kwargs):
    func = getattr(_outer_func, _FUNC)
    return func(*args, **kwargs) if func.__code__.co_argcount else func()
