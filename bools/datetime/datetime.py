import re
from datetime import datetime
from dateutil import tz


def _split_formats(format_str):
    return re.split(r'(%.)', format_str)


class Datetime(datetime):
    _DEFAULT_TZ = tz.gettz('Asia/Shanghai')
    _DEFAULT_FORMAT = '%Y-%m-%d %H:%M:%S.%f %z'
    _FORMATS = _split_formats(_DEFAULT_FORMAT)
    _SEP_PATTERN = re.compile(r'[-:.Tt ]')

    def __new__(cls, *args, **kwargs):
        kwargs.update(tzinfo=cls._DEFAULT_TZ)
        # args 只保留前7位时间日期参数
        return super().__new__(cls, *args[:7], **kwargs)

    @classmethod
    def fromtimestamp(cls, ts, tz_info=None, precision='s') -> 'Datetime':
        precisions = {'s': 1, 'ms': 1e3, 'us': 1e6, 'ns': 1e9}
        if precision not in precisions:
            raise ValueError(f'精度不符合规范，必须是{precisions.keys()}中的一种，当前（{precision}）')
        else:
            ts = ts / precisions[precision]
        return Datetime.from_datetime(super().fromtimestamp(ts, tz_info))

    @classmethod
    def now(cls, tz_info=None) -> 'Datetime':
        return Datetime.from_datetime(super().now(tz_info))

    def to_str(self, start=0, end=6) -> str:
        if not 0 <= start < end <= 8:
            raise ValueError(f'输出的位数参数必须在[0,{len(self._FORMATS) // 2})之间')
        return self.strftime(''.join(self._FORMATS[start * 2 + 1:end * 2]))

    @property
    def str(self):
        return self.to_str()

    @classmethod
    def from_str(cls, datetime_str: str, seps=None):
        if seps is None:
            values = cls._SEP_PATTERN.split(datetime_str)
        else:
            values = re.split(f'[{"".join(seps)}]', datetime_str)
        return Datetime(*map(lambda x: int(x.strip()), values))

    @classmethod
    def from_datetime(cls, dt: datetime):
        return Datetime(*[
            getattr(dt, attr)
            for attr in ['year', 'month', 'day', 'hour', 'minute', 'second', 'microsecond']
        ])

    def __add__(self, other):
        dt = super().__add__(other)
        return Datetime.from_datetime(dt)


def set_default_tz(tz_id):
    Datetime._DEFAULT_TZ = tz.gettz(tz_id)


def set_default_format(format_str):
    Datetime._DEFAULT_FORMAT = format_str
    Datetime._FORMATS = _split_formats(format_str)
