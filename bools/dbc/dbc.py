import json
import re

import requests
from json.decoder import JSONDecodeError
from functools import wraps
from dataclasses import dataclass
from abc import abstractmethod, ABC


@dataclass
class DBC(ABC):
    host: str = '127.0.0.1'
    port: int = None
    user: str = ''
    password: str = ''
    patch_pandas: bool = False
    version: int = None

    _ping_prefix = None
    _ping_result = None

    def __post_init__(self):
        protocol, self.host = re.findall("^(https?://)?(.*?)$", self.host)[0]
        self.base_url = f'{protocol or "http://"}{f"{self.user}:{self.password}@" if self.user else ""}{self.host}:{self.port}'
        if self._ping_prefix is not None and not self.version:
            ping = requests.get(f'{self.base_url}{self._ping_prefix}', verify=False)
            if ping.status_code != 200:
                raise ConnectionError(f'无法连接到{self.__class__.__name__}服务器，请检查配置是否正确\n\t{ping.text}\n'
                                      f'若确定服务器地址无误，可手动指定version参数关闭服务器连接检查')
            self._ping_result = ping

        if self.patch_pandas:
            self._patch_pandas()
        if not self.version:
            self.version = self._version

    @abstractmethod
    def query(self, *args, **kwargs):
        pass

    @abstractmethod
    def write(self, *args, **kwargs):
        pass

    @abstractmethod
    def _patch_pandas(self):
        pass

    @property
    def _version(self):
        return -1

    @classmethod
    def is_na(cls, value):
        import pandas as pd
        result = pd.isna(value)
        if isinstance(result, bool):
            return result
        else:
            return all(result)

    @classmethod
    def not_na(cls, value):
        return not cls.is_na(value)


def http_json_res_parse(_func=None, *, is_return=True):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            res = func(*args, **kwargs)
            if res.status_code >= 300:
                raise ConnectionError(f'操作执行失败，错误码：[{res.status_code}]\n{res.text}')
            else:
                if is_return:
                    try:
                        return json.loads(res.text)
                    except JSONDecodeError:
                        raise ValueError(f'返回对象不是JSON字符串\n{res.text}')

        return wrapper

    return decorator(_func) if _func else decorator
