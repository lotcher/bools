import pandas as pd

from functools import partial
from bools.io import read_lines, read_jsons


def mixin():
    pd.read_lines = partial(read_lines, result_transfer=pd.DataFrame)
    pd.read_jsons = partial(read_jsons, result_transfer=pd.DataFrame)
