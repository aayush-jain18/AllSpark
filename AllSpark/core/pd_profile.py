import numpy as np
import pandas as pd


class DataFrameProfile:

    def __init__(self, df):
        self.columns = df.columns
        self.row_count = len(df)
        self.initial_dtypes = self.df.dtypes
        self.df = df
        self.memory_usage = self.df.memory_usage()

        
    @property
    def df(self):
        return self._df

    @df.setter
    def df(self, df):
        """Check left dataframe is a instance of pd.DataFrame and has the all
        the key columns"""
        self._df = df.copy()
        self._validate_dataframe()

    def _validate_dataframe(self):
        if len(self.columns) == 0 or self.row_count == 0:
            raise ValueError(f"Empty Dataframe cannot be profiled")

    def profile_dataframe(self):
        pass

    def multiprocess_1d(column, series) -> Tuple[str, dict]:
        """Wrapper to process series in parallel.
        Args:
            column: The name of the column.
            series: The series values.
        Returns:
            A tuple with column and the series description.
        """
        return column, describe_1d(series)

    def describe_1d(series: pd.Series) -> dict:
        """Describe a series (infer the variable type, then calculate type-specific values).
        Args:
            series: The Series to describe.
        Returns:
            A Series containing calculated series description values.
        """

        # Replace infinite values with NaNs to avoid issues with histograms later.
        series.replace(to_replace=[np.inf, np.NINF, np.PINF], value=np.nan, inplace=True)

        # Infer variable types
        series_description = base.get_var_type(series)

        # Run type specific analysis
        if series_description["type"] == Variable.S_TYPE_UNSUPPORTED:
            series_description.update(describe_unsupported(series, series_description))
        else:
            series_description.update(describe_supported(series, series_description))

            type_to_func = {
                Variable.S_TYPE_CONST: describe_constant_1d,
                Variable.TYPE_BOOL: describe_boolean_1d,
                Variable.TYPE_NUM: describe_numeric_1d,
                Variable.TYPE_DATE: describe_date_1d,
                Variable.S_TYPE_UNIQUE: describe_unique_1d,
                Variable.TYPE_CAT: describe_categorical_1d,
                Variable.TYPE_URL: describe_url_1d,
                Variable.TYPE_PATH: describe_path_1d,
            }

            if series_description["type"] in type_to_func:
                series_description.update(
                    type_to_func[series_description["type"]](series, series_description)
                )
            else:
                raise ValueError("Unexpected type")

        # Return the description obtained
        return series_description

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