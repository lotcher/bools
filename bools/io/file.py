from tqdm import tqdm
from typing import Callable, Iterable, Any

from bools.functools import catch


def read_lines(filename: str, line_transfer: Callable[[str], Any],
               result_transfer: Callable[[Iterable], Any], log=False) -> Any:
    lines = []
    with open(filename) as f:
        for i, line in enumerate(tqdm(f.readlines(), ncols=100)):
            catch(
                lambda: lines.append(line_transfer(line)), log=f'解析第[{i}]行({line.strip()})失败' if log else None,
                print_traceback=False,
            )()

        return result_transfer(lines)


def read_jsons(filename: str, result_transfer: Callable[[Iterable], Any], log=False) -> Any:
    import json
    return read_lines(filename, json.loads, result_transfer, log)
