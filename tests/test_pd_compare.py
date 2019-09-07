import pytest
import pandas as pd
from pandas.util.testing import makeMixedDataFrame
from AllSpark.core.pd_compare import Compare


def test_equal_dataframes_compare():
    df1 = makeMixedDataFrame()
    compare = Compare(left=df1, right=df1, key_columns='A')
    assert compare.diff is None
