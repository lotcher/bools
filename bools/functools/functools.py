from functools import wraps
from bools.log import Logger
from bools.datetime import Datetime


def catch(_func=None, *, except_func=print, except_return=None, log=''):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception:
                import traceback
                except_func()
                Logger.error(log)
                print(traceback.print_exc())
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
