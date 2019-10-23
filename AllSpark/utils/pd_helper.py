import sys

import pandas as pd
from urllib.parse import urlparse
from pathlib import Path

from AllSpark.constants import Constants, PandasDtypes
from AllSpark.config import config
from AllSpark.utils.path import str_is_path


def read_pandas(file_name: Path) -> pd.DataFrame:
    """Read DataFrame based on the file extension. This function is used when the file is in a standard format.
    Various file types are supported (.csv, .json, .jsonl, .data, .tsv, .xls, .xlsx, .xpt, .sas7bdat, .parquet)
    Args:
        file_name: the file to read
    Returns:
        DataFrame
    Notes:
        This function is based on pandas IO tools:
        https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html
        https://pandas.pydata.org/pandas-docs/stable/reference/io.html
        This function is not intended to be flexible or complete. The main use case is to be able to read files without
        user input, which is currently used in the editor integration. For more advanced use cases, the user should load
        the DataFrame in code.
    """
    extension = file_name.suffix.lower()
    if extension == ".json":
        df = pd.read_json(str(file_name))
    elif extension == ".jsonl":
        df = pd.read_json(str(file_name), lines=True)
    elif extension == ".dta":
        df = pd.read_stata(str(file_name))
    elif extension == ".tsv":
        df = pd.read_csv(str(file_name), sep="\t")
    elif extension in [".xls", ".xlsx"]:
        df = pd.read_excel(str(file_name))
    elif extension in [".hdf", ".h5"]:
        df = pd.read_hdf(str(file_name))
    elif extension in [".sas7bdat", ".xpt"]:
        df = pd.read_sas(str(file_name))
    elif extension == ".parquet":
        df = pd.read_parquet(str(file_name))
    elif extension in [".pkl", ".pickle"]:
        df = pd.read_pickle(str(file_name))
    else:
        if extension != ".csv":
            pass
#           warn_read(extension)
        df = pd.read_csv(str(file_name))
    return df


def clean_column_names(df):
    """Removes spaces and colons from pandas DataFrame column names
    Args:
        df: DataFrame
    Returns:
        DataFrame with spaces in column names replaced by underscores, colons removed.
    """
    df.columns = df.columns.str.replace(" ", "_")
    df.columns = df.columns.str.replace(":", "")
    return df


def rename_index(df):
    """If the DataFrame contains a column or index named `index`, this will produce errors. We rename the {index,column}
    to be `df_index`.
    Args:
        df: DataFrame to process.
    Returns:
        The DataFrame with {index,column} `index` replaced by `df_index`, unchanged if the DataFrame does not contain such a string.
    """
    df.rename(columns={"index": "df_index"}, inplace=True)

    if "index" in df.index.names:
        df.index.names = [x if x != "index" else "df_index" for x in df.index.names]
    return df


def get_counts(series: pd.Series) -> dict:
    """Counts the values in a series (with and without NaN, distinct).
    Args:
        series: Series for which we want to calculate the values.
    Returns:
        A dictionary with the count values (with and without NaN, distinct).
    """
    value_counts_with_nan = series.value_counts(dropna=False)
    value_counts_without_nan = (
        value_counts_with_nan.reset_index().dropna().set_index("index").iloc[:, 0]
    )

    distinct_count_with_nan = value_counts_with_nan.count()
    distinct_count_without_nan = value_counts_without_nan.count()

    # When the inferred type of the index is just "mixed" probably the types within the series are tuple, dict,
    # list and so on...
    if value_counts_without_nan.index.inferred_type == "mixed":
        raise TypeError("Not supported mixed type")

    return {
        "value_counts_with_nan": value_counts_with_nan,
        "value_counts_without_nan": value_counts_without_nan,
        "distinct_count_with_nan": distinct_count_with_nan,
        "distinct_count_without_nan": distinct_count_without_nan,
    }


def is_boolean(series: pd.Series, series_description: dict) -> bool:
    """Is the series boolean type?
    Args:
        series: Series
        series_description: Series description
    Returns:
        True is the series is boolean type in the broad sense (e.g. including yes/no, NaNs allowed).
    """
    keys = series_description["value_counts_without_nan"].keys()
    if pd.api.types.is_bool_dtype(keys):
        return True
    elif (
            series_description["distinct_count_without_nan"] <= 2
            and pd.api.types.is_numeric_dtype(series)
            and series[~series.isnull()].between(0, 1).all()
    ):
        return True
    elif series_description["distinct_count_without_nan"] <= 4:
        unique_values = set([str(value).lower() for value in keys.values])
        accepted_combinations = [
            ["y", "n"],
            ["yes", "no"],
            ["true", "false"],
            ["t", "f"],
        ]

        if len(unique_values) == 2 and any(
                [unique_values == set(bools) for bools in accepted_combinations]
        ):
            return True

    return False


def is_numeric(series: pd.Series, series_description: dict) -> bool:
    """Is the series numeric type?
    Args:
        series: Series
        series_description: Series description
    Returns:
        True is the series is numeric type (NaNs allowed).
    """
    return pd.api.types.is_numeric_dtype(series) and series_description[
        "distinct_count_without_nan"
    ] >= config["low_categorical_threshold"].get(int)


def is_url(series: pd.Series, series_description: dict) -> bool:
    """Is the series url type?
    Args:
        series: Series
        series_description: Series description
    Returns:
        True is the series is url type (NaNs allowed).
    """
    if series_description["distinct_count_without_nan"] > 0:
        try:
            result = series[~series.isnull()].astype(str).apply(urlparse)
            return result.apply(lambda x: all([x.scheme, x.netloc, x.path])).all()
        except ValueError:
            return False
    else:
        return False


def is_path(series, series_description) -> bool:
    if series_description["distinct_count_without_nan"] > 0:
        try:
            result = series[~series.isnull()].astype(str).apply(str_is_path)
            return result.all()
        except ValueError:
            return False
    else:
        return False


def is_date(series) -> bool:
    """Is the variable of type datetime? Throws a warning if the series looks like a datetime, but is not typed as
    datetime64.
    Args:
        series: Series
    Returns:
        True if the variable is of type datetime.
    """
    is_date_value = pd.api.types.is_datetime64_dtype(series)

    return is_date_value


def get_var_type(series: pd.Series) -> dict:
    """Get the variable type of a series.
    Args:
        series: Series for which we want to infer the variable type.
    Returns:
        The series updated with the variable type included.
    """

    try:
        series_description = get_counts(series)

        distinct_count_with_nan = series_description["distinct_count_with_nan"]
        distinct_count_without_nan = series_description["distinct_count_without_nan"]

        if distinct_count_with_nan <= 1:
            var_type = Variable.S_TYPE_CONST
        elif is_boolean(series, series_description):
            var_type = Variable.TYPE_BOOL
        elif is_numeric(series, series_description):
            var_type = Variable.TYPE_NUM
        elif is_date(series):
            var_type = Variable.TYPE_DATE
        elif is_url(series, series_description):
            var_type = Variable.TYPE_URL
        elif is_path(series, series_description) and sys.version_info[1] > 5:
            var_type = Variable.TYPE_PATH
        elif distinct_count_without_nan == len(series):
            var_type = Variable.S_TYPE_UNIQUE
        else:
            var_type = Variable.TYPE_CAT
    except TypeError:
        series_description = {}
        var_type = Variable.S_TYPE_UNSUPPORTED

    series_description.update({"type": var_type})

    return series_description


def reverse_row_order(df, reset_index=False):
    """Reverse the order of the dataframe, and reset the indices (optional)
    Parameters
    ----------
    df : pandas.core.frame.DataFrame
        Two-dimensional size-mutable,
        potentially heterogeneous tabular data
    reset_index : bool, optional
        Reset the index of the DataFrame to start at '0'
    Returns
    -------
    `pandas.core.frame.DataFrame`
        Reversed order of rows in DataFrame
    """
    return df.loc[::-1].reset_index(drop=True) if reset_index else df.loc[::-1]


def reverse_col_order(df):
    """Summary
    Parameters
    ----------
    df : pandas.core.frame.DataFrame
        Two-dimensional size-mutable,
        potentially heterogeneous tabular data
    Returns
    -------
    `pandas.core.frame.DataFrame`
        Reversed order of cols in DataFrame
    """
    return df.loc[:, ::-1]


def select_by_datatype(df, include_datatype=[], exclude_datatype=[]):
    """
    Parameters
    ----------
    df : pandas.core.frame.DataFrame
        Two-dimensional size-mutable,
        potentially heterogeneous tabular data
    include_datatype : list, optional
        A list containing data-type to include.
    exclude_datatype : list, optional
        A list containing data-type to exclude.
    Returns
    -------
    `pandas.core.frame.DataFrame`
        DataFrame containing included/excluded data-types
    """
    return (
        df.select_dtypes(include=include_datatype, exclude=exclude_datatype)
        if include_datatype or exclude_datatype
        else df
    )


def build_df_from_csvs(csv_files, axis, ignore_index=True):
    """Build a DataFrame from multiple files (row-wise)
    Parameters
    ----------
    csv_files : list
        List of csv files
    axis : int
        Concatenate csv files according to columns or rows.
    ignore_index : bool, optional
        Resets indices
    Returns
    -------
    `pandas.core.frame.DataFrame`
        DataFrame containing data from CSV files(s)
    """
    return pd.concat(
        (pd.read_csv(file) for file in csv_files),
        axis=axis,
        ignore_index=ignore_index,
    )


def continous_to_categorical_data(df, column_name, bins=[], labels=[]):
    """Summary
    Parameters
    ----------
    df : pandas.core.frame.DataFrame
        Two-dimensional size-mutable,
        potentially heterogeneous tabular data
    column_name : str
        Description
    bins : list
        Description
    labels : list
        Description
    Returns
    -------
    TYPE
        Description
    Example
    -------
    >> data['age']
        0 | 22.0
        1 | 38.0
        2 | 26.0
        3 | 35.0
        4 | 35.0
        5 |  NaN
        6 | 54.0
        7 |  2.0
        8 | 27.0
        9 | 14.0
        Name: Age, dtype: float64
    >> continuous_to_categorical_data(
    data, 'age', bins=[0, 18, 25, 99], labels==['child', 'young adult', 'adult'])
        0 | young adult
        1 | adult
        2 | adult
        3 | adult
        4 | adult
        5 | NaN
        6 | adult
        7 | child
        8 | adult
        9 | child
        Name: Age, dtype: category
        Categories (3, object): [child < young adult < adult]
    # This assigned each value to a bin with a label.
    # Ages 0 to 18 were assigned the label "child", ages 18 to 25 were assigned the
    # label "young adult", and ages 25 to 99 were assigned the label "adult".
    """
    return pd.cut(df[column_name], bins=bins, labels=labels)


def get_cat_codes_df(df):
    """
    Returns DataFrame containing only categorical columns
    converted into numeric code, used for clustering
    Parameters
    ----------
    df : pd.DataFrame
    Returns
    -------
    df_cat_codes : pd.DataFrame
    """
    df_cat_codes = pd.DataFrame()
    for column, dtype in df.dtypes.to_dict().items():
        if str(dtype) == 'category':
            df_cat_codes[column] = df[column].cat.codes
        elif str(dtype).startswith('int'):
            df_cat_codes[column] = df[column]
    return df_cat_codes

# print(ax.index._summary(),
# ax.dtypes,
# ax.count(),
# ax.memory_usage().sum())
