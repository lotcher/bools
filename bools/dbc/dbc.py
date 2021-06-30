import json
from json.decoder import JSONDecodeError
from functools import wraps


def http_json_res_parse(_func=None, *, is_return=True):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            res = func(*args, **kwargs)
            if res.status_code != 200:
                raise ConnectionError(f'操作执行失败，错误码：[{res.status_code}]\n{res.text}')
            else:
                if is_return:
                    try:
                        return json.loads(res.text)
                    except JSONDecodeError:
                        raise ValueError(f'返回对象不是JSON字符串\n{res.text}')

        return wrapper

    return decorator(_func) if _func else decorator
