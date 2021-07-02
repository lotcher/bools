import requests
import json
import re
from dataclasses import dataclass
from typing import List

from .dbc import http_json_res_parse
from bools.functools import catch
from bools.log import Logger

TEMPLATE_NAME = 'bowaer'
_HEADERS = {
    'Content-type': 'application/json'
}


@dataclass
class ElasticSearch:
    host: str = '127.0.0.1'
    port: int = 9200
    user: str = ''
    password: str = ''
    patch_pandas: bool = False

    def __post_init__(self):
        self.host = self.host.lstrip('http://').strip()
        self.base_url = f'http://{f"{self.user}:{self.password}@" if self.user else ""}{self.host}:{self.port}'
        ping = requests.get(self.base_url)
        if ping.status_code != 200:
            raise ConnectionError(f'无法连接到ES服务器，请检查配置是否正确\n\t{ping.text}')
        if self.patch_pandas:
            self._patch_pandas()
        self.version = int(ping.json()['version']['number'][0])
        self.type = '_doc'
        self.type_url = f'{self.type}/' if self.version <= 6 else ''

    def write(self, index: str, data: List[dict], batch_size=10000):
        self._batch_write(index, ['{"index":{}}\n' + json.dumps(item) + '\n' for item in data], batch_size)

    @http_json_res_parse
    def query(self, index, query_body: dict, sort_by_score=False, create_scroll=False, timeout=60):
        if 'sort' not in query_body and not sort_by_score:
            # 不要求按照分数排序搜索会更快一些
            query_body['sort'] = ['_doc']
        url = f'{self.base_url}/{index}/_search{f"?scroll={timeout // 60}m" if create_scroll else ""}'
        return requests.get(url, headers=_HEADERS, data=json.dumps(query_body), timeout=timeout)

    def scroll_query(self, index, query_body: dict, batch_size=1000, timeout=180):
        if 'size' not in query_body:
            query_body['size'] = batch_size
        result = self.query(index, query_body, create_scroll=True, timeout=timeout)
        expect_count = result['hits']['total'] if self.version <= 6 else result['hits']['total']['value']
        scroll_url = f'{self.base_url}/_search/scroll'
        scroll_data = json.dumps({'scroll_id': result['_scroll_id'], 'scroll': f'{timeout // 60}m'})

        while True:
            res = requests.post(
                scroll_url, data=scroll_data,
                headers=_HEADERS, timeout=timeout
            ).json()
            if 'error' in res or not res['hits']['hits']:
                if len(result['hits']['hits']) != expect_count:
                    Logger.warning("查询结果条数与预期不相等，请尝试调大timeout参数或者检查网络")
                return result

            result['took'] += res['took']
            result['hits']['hits'] += (res['hits']['hits'])

    @http_json_res_parse
    def delete(self, index_pattern):
        return requests.delete(f'{self.base_url}/{index_pattern}')

    @http_json_res_parse(is_return=False)
    def _write(self, index, ndjson_data: str):
        return requests.post(
            # 如果url没有指定index，则调用方在action中指定
            url=f'{self.base_url}/{f"{index}/" if index else "/"}{self.type_url}_bulk',
            data=ndjson_data,
            headers=_HEADERS
        )

    def _batch_write(self, index, ndjsons: List[str], batch_size):
        self._check_template(index if index.endswith("*") else re.split(r'\W', index)[0] + '*')
        for batch in range(len(ndjsons) // batch_size + 1):
            items = ndjsons[batch * batch_size:batch * batch_size + batch_size]
            if items:  # ES bulk操作body不能为空
                self._write(index, ''.join(items))

    def _check_template(self, index_pattern):
        url = f'{self.base_url}/_template/{TEMPLATE_NAME}'
        current_templates = requests.get(url).json()
        patterns = current_templates[TEMPLATE_NAME]['index_patterns'] if current_templates else []
        if index_pattern not in patterns:
            current_templates = {
                "index_patterns": patterns + [index_pattern],
                "settings": {
                    "number_of_replicas": 0,
                    "number_of_shards": 3
                },
                "mappings": {
                    "dynamic_date_formats": [
                        "yyyy-MM-dd HH:mm:ss.SSSZ||epoch_millis",
                        "yyyy-MM-dd HH:mm:ss.SSSSSSZ||epoch_millis",
                        "yyyy-MM-dd HH:mm:ss.SSSSSSSSSZ||epoch_millis",
                        "yyyy-MM-dd HH:mm:ssZ||epoch_millis"
                    ],
                    "dynamic_templates": [
                        {
                            "strings_as_keywords": {
                                "match_mapping_type": "string",
                                "mapping": {
                                    "type": "keyword"
                                }
                            }
                        }
                    ]
                }
            }
            if self.version <= 6:
                current_templates['mappings'] = {self.type: current_templates['mappings']}
            self.put_templates(current_templates, TEMPLATE_NAME)

    @http_json_res_parse
    def put_templates(self, templates: dict, template_name):
        url = f'{self.base_url}/_template/{template_name}'
        return requests.put(url, headers=_HEADERS, data=json.dumps(templates))

    def _patch_pandas(self):
        import pandas as pd
        import numpy as np
        from pandas.core.dtypes.dtypes import DatetimeTZDtype

        def to_es(inner_self: pd.DataFrame, index=None, index_col=None, numeric_detection=False, batch_size=10000):
            if inner_self.empty:
                return

            if index and index_col:
                raise ValueError('index和index_col参数不能同时指定')
            if not (index or index_col):
                raise ValueError('index和index_col参数必须指定其中的一个')
            if index:
                index_col = '__$@index'
                inner_self[index_col] = index
            inner_self.index = inner_self.pop(index_col)

            for col, dtype in zip(inner_self.columns, inner_self.dtypes):
                if dtype == np.dtype('<M8[ns]'):
                    inner_self[col] = inner_self[col].astype('str') + '+0800'
                elif isinstance(dtype, DatetimeTZDtype):
                    # es无法识别"+08:00"的时区标识
                    inner_self[col] = inner_self[col].astype('str').apply(lambda x: ''.join(x.rsplit(':', 1)))
                elif numeric_detection and dtype == np.object_:
                    inner_self[col] = catch(except_return=inner_self[col], print_traceback=False)(
                        lambda: inner_self[col].astype('float')
                    )()
            ndjsons = [
                json.dumps({'index': {'_index': index}}) + '\n' + item.to_json() + '\n'
                for index, item in inner_self.iterrows()
            ]
            self._batch_write(inner_self.index[0], ndjsons, batch_size)

        def read_es(index, query_body: dict, batch_size=1000, timeout=180):
            return pd.DataFrame([
                hit['_source']
                for hit in self.scroll_query(index, query_body, batch_size, timeout)['hits']['hits']
            ])

        pd.DataFrame.to_es = to_es
        pd.read_es = read_es
