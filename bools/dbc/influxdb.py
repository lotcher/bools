import requests

from .dbc import DBC, http_json_res_parse
from dataclasses import dataclass

_CREATE, _DROP = 'CREATE', 'DROP'
_DATABASE, _MEASUREMENT = 'database', 'measurement'


@dataclass
class InfluxDB(DBC):
    port: int = 8086
    database: str = None

    _ping_prefix = '/health'

    def __post_init__(self):
        super().__post_init__()
        self.query_url = f'{self.base_url}/query'
        self.write_url = f'{self.base_url}/write'
        self.version = int(self._ping_result.json()['version'][0])

    @http_json_res_parse
    def query(self, influxql: str, database: str = None, batch_size=10000, timeout=180):
        database = self._check_database(database)
        query_url = f'{self.query_url}?db={database}&pretty=false&chunked={batch_size}&q={influxql}'
        return requests.get(query_url, timeout=timeout)

    def write(self, points: list, database: str = None, precision='n', batch_size=10000, timeout=180):
        database = self._check_database(database)
        for batch in range(len(points) // batch_size + 1):
            items = points[batch * batch_size:batch * batch_size + batch_size]
            self._write(points=items, database=database, precision=precision, timeout=timeout)

    @http_json_res_parse(is_return=False)
    def _write(self, points: list, database, precision='n', timeout=180):
        write_url = f'{self.write_url}?db={database}&precision={precision}'
        return requests.post(write_url, data='\n'.join(points), timeout=timeout)

    def drop_measurement(self, measurement: str, database: str = None):
        self.action(f'{_DROP} {_MEASUREMENT} "{measurement}"', self._check_database(database))

    def drop_database(self, database: str = None):
        self.action(f'{_DROP} {_DATABASE} "{database}"', self._check_database(database))

    def create_database(self, database):
        self.action(f'{_CREATE} {_DATABASE} "{database}"')

    @http_json_res_parse(is_return=False)
    def action(self, influxql, database=''):
        return requests.post(f'{self.query_url}?db={database}&q={influxql}')

    def _patch_pandas(self):
        import pandas as pd
        import numpy as np
        from pandas.core.dtypes.dtypes import DatetimeTZDtype

        def read_influxdb(influxql: str, database: str = None, batch_size=10000, timeout=180, tz_id='Asia/Shanghai'):
            res = self.query(
                influxql=influxql, database=database, batch_size=batch_size, timeout=timeout
            )['results'][0]
            result = pd.DataFrame()

            for block in res.get('series', []):
                df = pd.DataFrame(block['values'], columns=block['columns'])
                df.index = pd.to_datetime(df.pop('time'), utc=True).apply(lambda x: x.tz_convert(tz_id))
                result = pd.concat([result, df], axis=1)
            return result

        def to_influxdb(
                inner_self: pd.DataFrame, tag_cols, time_col='index',
                measurement=None, measurement_col=None,
                database: str = None, batch_size=10000, timeout=180
        ):
            if inner_self.empty:
                return
            if time_col != 'index':
                inner_self.index = inner_self.pop(time_col)

            if measurement and measurement_col:
                raise ValueError('measurement和measurement_col参数不能同时指定')
            if not (measurement or measurement_col):
                raise ValueError('measurement和measurement_col参数必须指定其中的一个')
            if measurement:
                measurement_col = f'__$@{_MEASUREMENT}'
                inner_self[measurement_col] = measurement

            # hash顺序的字段写入更快
            tag_cols = set(tag_cols or [])
            field_cols = set(inner_self.columns) - tag_cols - {measurement_col}

            try:
                time_dtype = inner_self.index.dtype
                if time_dtype in [np.dtype(t) for t in ['int32', 'int64', 'float32', 'float64']]:
                    inner_self.index = inner_self.index.astype('int')
                    power = 19 - len(str(inner_self.index[0]))
                    if power < 0 or power > 9:
                        raise ValueError(f'时间列：[{time_col}]格式不支持，时间戳支持ns,us,ms,s。请确认是否指定有效时间列')

                    inner_self.index = inner_self.index * 10 ** power
                elif time_dtype == np.dtype('O'):
                    inner_self.index = pd.to_datetime(inner_self.index)
                    time_dtype = inner_self.index.dtype

                if time_dtype == np.dtype('<M8[ns]'):
                    # 无时区数据当作UTC+8处理
                    inner_self.index = inner_self.index.astype('int') - 8 * 3600 * int(1e9)
                elif isinstance(time_dtype, DatetimeTZDtype):
                    inner_self.index = inner_self.index.astype('int')
            except Exception:
                raise ValueError(f'时间列：[{time_col}]格式不支持，当前支持时间戳(ns,us,ms,s), 时间字符串及date列类型')

            points = inner_self.apply(
                lambda line: f'{line[measurement_col]}{"".join(f",{tag}={line[tag]}" for tag in tag_cols)}'
                             f' {",".join(f"{field}={line[field]}" for field in field_cols)} {line.name}'
                , axis=1
            ).tolist()
            self.write(points=points, database=database, batch_size=batch_size, timeout=timeout)

        pd.read_influxdb = read_influxdb
        pd.DataFrame.to_influxdb = to_influxdb

    def _check_database(self, database):
        if not (self.database or database):
            raise ValueError('请指定database（初始化传入或函数入参）')
        return database or self.database
