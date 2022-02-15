import requests
import json
import re
from dataclasses import dataclass
from typing import Iterator, Generator, Union
from itertools import islice

from .dbc import DBC, http_json_res_parse
from bools.functools import catch
from bools.log import Logger

TEMPLATE_NAME = 'bowaer'
_HEADERS = {
    'Content-type': 'application/json'
}


@dataclass
class ElasticSearch(DBC):
    port: int = 9200
    type: str = '_doc'

    _ping_prefix = ''

    def __post_init__(self):
        super().__post_init__()
        self.type_url = f'{self.type}/' if self.version <= 6 else ''

    @property
    def _version(self):
        return int(self._ping_result.json()['version']['number'][0])

    def write(self, index: str, data: Iterator[dict], batch_size=10000, timeout=180):
        return self._batch_write(
            index=index, ndjsons=('{"index":{}}\n' + json.dumps(item) + '\n' for item in data),
            batch_size=batch_size, timeout=timeout
        )

    @http_json_res_parse
    def query(self, index, query_body: dict, sort_by_score=False, create_scroll=False, timeout=60):
        if 'sort' not in query_body and not sort_by_score:
            # 不要求按照分数排序搜索会更快一些
            query_body['sort'] = ['_doc']
        url = f'{self.base_url}/{index}/_search{f"?scroll={timeout // 60}m" if create_scroll else ""}'
        return requests.get(url, headers=_HEADERS, data=json.dumps(query_body), timeout=timeout, verify=False)

    def scroll_query(self, index, query_body: dict, batch_size=1000, timeout=180, total_size=None, log=False):
        if 'size' not in query_body:
            query_body['size'] = batch_size
        result = self.query(index, query_body, create_scroll=True, timeout=timeout)
        expect_count = total_size or (
            result['hits']['total'] if self.version <= 6 else result['hits']['total']['value']
        )
        scroll_url = f'{self.base_url}/_search/scroll'
        scroll_data = json.dumps({'scroll_id': result['_scroll_id'], 'scroll': f'{timeout // 60}m'})
        hits, cost = [], 0
        while True:
            res = requests.post(
                scroll_url, data=scroll_data,
                headers=_HEADERS, timeout=timeout, verify=False
            ).json()
            if 'error' in res or not res['hits']['hits'] or len(hits) >= expect_count:
                if len(hits) < expect_count:
                    Logger.warning("查询结果条数少于预期，请尝试调大timeout参数或者检查网络")
                result['took'] = cost
                result['hits']['hits'] = hits[:expect_count]
                return result

            cost += res['took']
            hits += (res['hits']['hits'])
            if log:
                Logger.info(f"take {len(hits)}, es query cost {cost}ms")

    @http_json_res_parse
    def delete(self, index_pattern):
        return requests.delete(f'{self.base_url}/{index_pattern}', verify=False)

    @http_json_res_parse
    def create_or_cover(self, index: str, document: Union[str, dict], doc_id: str = None):
        if isinstance(document, dict):
            document = json.dumps(document)
        return requests.post(
            f'{self.base_url}/{index}/_doc/{doc_id if doc_id else ""}',
            headers=_HEADERS, data=document
        )

    @http_json_res_parse
    def _write(self, index, ndjson_data: str, timeout):
        return requests.post(
            # 如果url没有指定index，则调用方在action中指定
            url=f'{self.base_url}/{f"{index}/" if index else "/"}{self.type_url}_bulk',
            data=ndjson_data, headers=_HEADERS, timeout=timeout, verify=False
        )

    def _batch_write(self, index, ndjsons: Generator[str, None, None], batch_size, timeout):
        self._check_template(index if index.endswith("*") else re.split(r'\W', index)[0] + '*')
        while True:
            items = list(islice(ndjsons, batch_size))
            if not items:  # ES bulk操作body不能为空
                break
            write_result = self._write(index=index, ndjson_data=''.join(items), timeout=timeout)
            if write_result.get('errors') is True:
                Logger.error('\n'.join(
                    [
                        str(item.get('index', {}).get('error', ''))
                        for item in write_result.get('items', [{}])
                    ][:10]
                ))

    def _check_template(self, index_pattern):
        url = f'{self.base_url}/_template/{TEMPLATE_NAME}'
        current_templates = requests.get(url, verify=False).json()
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
                        "yyyy-MM-dd HH:mm:ssZ||epoch_millis",
                        "yyyy-MM-dd'T'HH:mm:ss'Z'||epoch_millis",
                        "yyyy-MM-dd'T'HH:mm:ss.SSSZ||epoch_millis",
                        "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'||epoch_millis"
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
        return requests.put(url, headers=_HEADERS, data=json.dumps(templates), verify=False)

    def _patch_pandas(self):
        import pandas as pd
        import numpy as np
        from pandas.core.dtypes.dtypes import DatetimeTZDtype

        def to_es(inner_self: pd.DataFrame, index=None, index_col=None, id_col=None,
                  numeric_detection=False, batch_size=10000, timeout=180, copy=True):
            if inner_self.empty:
                return

            _self = inner_self.copy() if copy else inner_self
            _self['__$@_id'] = _self[id_col] if id_col else None

            if index and index_col:
                raise ValueError('index和index_col参数不能同时指定')
            if not (index or index_col):
                raise ValueError('index和index_col参数必须指定其中的一个')
            if index:
                index_col = '__$@index'
                _self[index_col] = index
            _self.index = _self.pop(index_col)

            for col, dtype in zip(_self.columns, _self.dtypes):
                if dtype == np.dtype('<M8[ns]'):
                    _self[col] = _self[col].astype('str') + '+0800'
                elif isinstance(dtype, DatetimeTZDtype):
                    # es无法识别"+08:00"的时区标识
                    _self[col] = _self[col].astype('str').apply(lambda x: ''.join(x.rsplit(':', 1)))
                elif numeric_detection and dtype == np.object_:
                    _self[col] = catch(except_return=_self[col], print_traceback=False)(
                        lambda: _self[col].astype('float')
                    )()
            ndjsons = (
                f"{json.dumps({'index': {'_index': name_tuple[0], '_id': name_tuple[-1]}})}\n"
                f"{json.dumps({col: value for col, value in zip(_self.columns, name_tuple[1:-1]) if self.not_na(value)})}\n"
                for name_tuple in _self.itertuples()
            )
            return self._batch_write(
                index=_self.index[0], ndjsons=ndjsons, batch_size=batch_size, timeout=timeout
            )

        def read_es(index, query_body: dict, batch_size=1000, timeout=180, total_size=None, log=False):
            return pd.DataFrame([
                hit['_source']
                for hit in self.scroll_query(index, query_body, batch_size, timeout, total_size, log)['hits']['hits']
            ])

        pd.DataFrame.to_es = to_es
        pd.read_es = read_es
