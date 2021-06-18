from functools import wraps
from bools.log import Logger


def catch(except_func=print, except_return=None, log=''):
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

    return decorator
