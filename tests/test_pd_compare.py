import io
import pytest
import pandas as pd
import numpy as np
from pandas.util.testing import makeMixedDataFrame, assert_series_equal
from AllSpark.core.pd_compare import Compare
from AllSpark.constants import Constants


def test_equal_dataframes_compare():
    df1 = makeMixedDataFrame()
    output = Compare(left=df1, right=df1, key_columns='A').diff
    assert output is None


def test_compare_object_column():
    data = f"""
a,b,expected
Hi,Hi,
Yo,Yo,
Hey,Hey ,{Constants.DIFF_TRUE}
résumé,resume,{Constants.DIFF_TRUE}
résumé,résumé,
1,2,-1.0
1,,{Constants.DIFF_TRUE}
,,
💩,💩,
💩,🤔,{Constants.DIFF_TRUE}
 , ,
  , ,{Constants.DIFF_TRUE}
abc,ABC,{Constants.DIFF_TRUE}
something,,{Constants.DIFF_TRUE}
,something,{Constants.DIFF_TRUE}
    """
    df = pd.read_csv(io.StringIO(data))
    output = Compare.compare_column(df.a, df.b, atol=0, rtol=0)
    assert all(pd.isnull(Compare.compare_object_columns(df.expected, output, atol=0, rtol=0)))


def test_compare_numeric_column():
    a = pd.Series(
        [0, 1, 2, 3.00000000000001, -4, 1. + 0.j, 2. + 0.j, 0. + 0.j, 0. + 0.j,
         1. + 1.j, 3. + 0.j, np.nan])
    b = pd.Series(
        [np.nan, 1, 2, 5.00000200000001, -5, 2. + 0.j, 2. + 0.j, 0. + 0.j,
         0. + 0.j, 1. + 1.j, 3. + 0.j, np.nan])
    expected = pd.Series(
        [Constants.DIFF_TRUE, np.nan, np.nan, (-2.000002+0j), (1+0j), (-1+0j),
         np.nan, np.nan, np.nan, np.nan, np.nan, np.nan])
    df = pd.DataFrame()
    df['a'] = a
    df['b'] = b
    output = Compare.compare_numeric_columns(df.a, df.b, atol=0, rtol=0)
    assert all(pd.isnull(
        Compare.compare_object_columns(expected, output, atol=0, rtol=0)))


def test_compare_datetime_column():
    data = """
a,b,expected
2015-07-04 00:00:00,,
2015-07-05 00:00:00,2015-07-05 00:00:00,0 days 00:00:00.00
2015-07-06 00:00:00,2015-07-08 00:00:00,-2 days +00:00:00.00
2015-07-12 00:00:00,2015-07-11 00:00:00,1 days 00:00:00.00
2015-07-03 06:00:00,2015-07-03 06:32:00,-1 days +23:28:00.00
2015-07-03 07:00:00,2015-07-03 07:00:00,0 days 00:00:00.00
,,
    """
    df = pd.read_csv(io.StringIO(data), parse_dates=['a', 'b', 'expected'])
    output = Compare.compare_timeseries_column(df.a, df.b)
    assert pd.to_timedelta(df.expected).equals(output)
#
#
# def test_compare_object_column_with_tol():
#     pass
#
#
# def test_sort_logic():
#     pass
#
#
# def test_duplicate_rows():
#     pass
