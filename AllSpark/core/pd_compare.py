import logging
import multiprocessing
from multiprocessing.pool import ThreadPool

import numpy as np
import pandas as pd

from ..constants import Constants

logging.basicConfig(format=Constants.LOG_FORMAT, level=logging.INFO)


class Compare:
    """Comparison class to be used to compare whether two dataframes as equal.
    Both left and right should be dataframes containing all of the key_columns,
    with unique column names.
    Parameters
    ----------
    left : pandas ``DataFrame``
        First dataframe to check
    right : pandas ``DataFrame``
        Second dataframe to check
    key_columns :  str, list of str, or array-like, optional
        Column or index level name(s) in the caller to join on the index
        in `other`. If multiple values given, the `other` DataFrame must
        have a MultiIndex. Can pass an array as the join key if it is not
        already contained in the calling DataFrame. Like an Excel
        VLOOKUP operation.
    atol : float, optional
        Absolute tolerance between two values
    rtol : float, optional
        Relative tolerance between two values
    lsuffix : str, default '_left'
        Suffix to use from left frame's overlapping columns.
    rsuffix : str, default '_right'
        Suffix to use from right frame's overlapping columns.
    dsuffix : str, default '_right'
        Suffix to use for the compare result columns.
    if_duplicate_keys:
    if_duplicate_rows:
    Attributes
    ----------

    """
    def __init__(self, *,
                 left,
                 right,
                 on_index=False,
                 key_columns=None,
                 atol=0,
                 rtol=0,
                 lsuffix='_left',
                 rsuffix='_right',
                 dsuffix='_diff',
                 if_duplicate_keys='drop',
                 if_duplicate_rows=None,
                 ignore_extra_columns=False,
                 keep_only_diff_rows=True,
                 keep_only_diff_columns=True,
                 pool_size=1,
                 ):

        if key_columns is None and on_index is False:
            raise Exception("Provide key_columns, or set on_index = True")
        elif on_index:
            self.on_index = True
        elif isinstance(key_columns, str):
            self.key_columns = [key_columns]
            self.on_index = False
        elif isinstance(key_columns, list):
            self.key_columns = key_columns
            self.on_index = False
        elif isinstance(key_columns, tuple):
            self.key_columns = list(key_columns)
            self.on_index = False

        # TODO: Add method for duplicate keys
        if if_duplicate_keys in Constants.IF_DUPLICATE_KEY_METHODS:
            self._if_dupes = if_duplicate_keys
        else:
            raise ValueError(f"{if_duplicate_keys} not a valid strategy")

        self.left = left
        self.right = right
        self.lsuffix = lsuffix
        self.rsuffix = rsuffix
        self.dsuffix = dsuffix
        self.atol = atol
        self.rtol = rtol
        self.ignore_extra_columns = ignore_extra_columns
        self.keep_only_diff_rows = keep_only_diff_rows
        self.keep_only_diff_columns = keep_only_diff_columns
        if isinstance(pool_size, int) and pool_size > 0:
            self.pool_size = pool_size
        else:
            self.pool_size = multiprocessing.cpu_count()
        self._dups = self.check_duplicate_keys()
        self.diff = pd.DataFrame()
        self.merge_df = pd.DataFrame()
        # self.metadata = pd.DataFrame(columns=Constants.METADATA_COLUMNS)
        self.metadata = pd.Series()
        self.column_mapping = dict()
        self._compare()

    @property
    def left(self):
        return self._left

    @left.setter
    def left(self, left):
        """Check left dataframe is a instance of pd.DataFrame and has the all
        the key columns"""
        self._left = left
        self._validate_dataframe("left")

    @property
    def right(self):
        return self._right

    @right.setter
    def right(self, right):
        """Check right dataframe is a instance of pd.DataFrame and has the all
        the key columns"""
        self._right = right
        self._validate_dataframe("right")

    def _validate_dataframe(self, index):
        """
        """
        dataframe = getattr(self, index)
        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError(f"{index} must be a pandas DataFrame")

        if dataframe.empty:
            raise ValueError(f"{index} can not be empty")

        # check if the key columns exist in dataframe
        if not set(self.key_columns).issubset(set(dataframe.columns)):
            raise ValueError(f"{index} must have all columns from key columns")

    # TODO: check when enabling on_index
    def check_duplicate_keys(self):
        if self.key_columns:
            dups = any(self.left[self.key_columns].duplicated() | self.right[self.key_columns].duplicated())
            return dups
        elif self.on_index:
            dups = any(self.left.index.duplicated() |
                       self.right.index.duplicated())
            return dups

    def handle_duplicate_keys(self, df):
        if self._if_dupes == 'drop':
            return df.loc[~df.index.duplicated(keep='first')]
        elif self._if_dupes == 'sort':
            df['sort_key'] = np.arange(len(df))
            self.key_columns.append('sort_key')
            return df.set_index('sort_key', append=True)
        elif self._if_dupes == 'aggregate':
            pass

    @property
    def left_unq_columns(self):
        return set(self.left.columns) - set(self.right.columns)

    @property
    def right_unq_columns(self):
        return set(self.right.columns) - set(self.left.columns)

    # TODO: Adjust this for on_index comparison
    @property
    def common_columns(self):
        return (set(self.left.columns) & set(self.right.columns)) - set(self.key_columns)

    def _merge_dataframe(self):
        """
        Merge DataFrame or named Series objects with a database-style join.
        The join is done on columns or indexes. If joining columns on
        columns, the DataFrame indexes *will be ignored*.
        """
        if self.key_columns:
            logging.info("Merging dataframes for comparison, on basis of "
                         f"key columns :{self.key_columns}")
            df = pd.merge(self.left, self.right, how='outer', sort=True,
                          on=self.key_columns, indicator='rows_present',
                          suffixes=(self.lsuffix, self.rsuffix)).set_index(self.key_columns)
            df = df[sorted(df.columns)]
            return df
        elif self.on_index:
            logging.info("Merging dataframes for comparison, on basis of index")
            df = pd.merge(self.left, self.right, how='outer', sort=True,
                          left_index=True, right_index=True,
                          indicator=True, suffixes=(self.lsuffix, self.rsuffix))
            df = df[sorted(df.columns)]
            return df

    @staticmethod
    def get_diff_row(diff):
        return diff.notnull()

    @staticmethod
    def count_notna(col):
        """
        Return the count of not null values in a pd.Series or a column in
        pd.DataFrame.

        Parameters
        ----------
        col : pd.Series or pd.DataFrame[column]

        Returns
        -------
        int
        """
        return sum(col.notna())

    @staticmethod
    def compare_timeseries_column(col1, col2):
        """
        Compare and return absolute differences in times between two datetime
        type pd.Series or pd.DataFrame[column].

        Parameters
        ----------
        col1, col2: pd.Series or pd.DataFrame[column]

        Returns
        -------
        timedelta series
        """
        return col1 - col2

    @staticmethod
    def compare_object_columns(col1, col2, atol, rtol):
        """
        Compare and returns diff between two pd.Series/pd.DataFrame[column]
        of dtype object.

        Parameters
        ----------
        col1, col2: pd.Series or pd.DataFrame[column]

        Returns
        -------
        diff series
        """
        diff = np.full(col2.size, np.nan, dtype='O')
        for index, row in enumerate(zip(col1, col2)):
            if row[0] != row[1]:
                # TODO: if the compare type strict do not look for numeric differences in object type columns
                try:
                    # Hack as float nan doesn't match to itself as well as np.nan
                    val1 = float(row[0])
                    val2 = float(row[1])
                    if ~(np.isnan(val1) and np.isnan(val2)):
                        if ~np.isclose(val1, val2, atol=atol, rtol=rtol):
                            float_diff = (val1 - val2)
                            float_diff = Constants.DIFF_TRUE if np.isnan(float_diff) else float_diff
                        else:
                            float_diff = np.nan
                        diff[index] = float_diff
                except ValueError:
                    diff[index] = Constants.DIFF_TRUE
        return pd.Series(diff, index=col1.index)

    @staticmethod
    def compare_numeric_columns(col1, col2, atol=0, rtol=0):
        """
        Returns a boolean array where absolute difference between two arrays
        element-wise compared within a tolerance.
        The tolerance values are positive, typically very small numbers.  The
        relative difference (`rtol` * abs(`b`)) and the absolute difference
        `atol` are added together to compare against the absolute difference
        between `a` and `b`.
        .. warning:: The default `atol` is not appropriate for comparing numbers
                     that are much smaller than one.
        Parameters
        ----------
        col1, col2: pd.Series or pd.DataFrame[column]
        atol : float
            The absolute tolerance parameter.
        rtol : float
            The relative tolerance parameter.

        Returns
        -------
        numeric difference
        """
        diff = np.full(col1.size, np.nan, dtype='O')
        if_num_diff = pd.Series(np.isclose(col1, col2, rtol=rtol, atol=atol,
                                           equal_nan=True))
        if not all(if_num_diff):
            for index, x_equals_y in if_num_diff.iteritems():
                if x_equals_y:
                    diff[index] = np.nan
                elif np.isnan(col1[index]) | np.isnan(col2[index]):
                    diff[index] = Constants.DIFF_TRUE
                else:
                    diff[index] = col1[index] - col2[index]
        return pd.Series(diff, index=col1.index)

    @staticmethod
    def compare_column(col1, col2, atol, rtol):
        col_diff = None
        if_diff_in_col = not col1.equals(col2)
        if if_diff_in_col:
            if np.issubdtype(col1.dtype, np.number) and \
                    np.issubdtype(col2.dtype, np.number):
                col_diff = Compare.compare_numeric_columns(col1, col2, atol, rtol)
            elif col1.dtype.kind == 'M' and col2.dtype.kind == 'M':
                col_diff = Compare.compare_timeseries_column(col1, col2)
            elif (col1.dtype.kind == 'O') or (col1.dtype.kind == 'O'):
                col_diff = Compare.compare_object_columns(col1, col2, atol, rtol)
        return col_diff

    @staticmethod
    def prepare_metadata(df):
        return df.count().drop('rows_present')

    def drop_non_diff_columns(self):
        drop_col = []
        for column in self.column_mapping:
            if self.count_notna(self.diff[self.column_mapping[column]['diff']]) == 0:
                drop_col.extend(list(self.column_mapping[column].values()))
        if drop_col:
            self.diff.drop(drop_col, axis='columns', inplace=True)

    def multiprocess_compare(self, df):
        """Wrapper to process series in parallel.
        Args:
            df:
        Returns:'
            A tuple with column and the series description.
        """
        diff_df = pd.DataFrame()
        diff_rows = pd.DataFrame()
        for column in self.common_columns:
            left_col = column + self.lsuffix
            right_col = column + self.rsuffix
            diff_col = column + self.dsuffix
            self.column_mapping[column] = {'left': left_col, 'right': right_col, 'diff': diff_col}
            col1 = df.loc[:, left_col]
            col2 = df.loc[:, right_col]
            diff = self.compare_column(col1, col2, self.atol, self.rtol)
            diff_df[left_col] = col1
            diff_df[right_col] = col2
            if diff is not None:
                diff_df[diff_col] = diff
                diff_rows[column] = self.get_diff_row(diff)
            else:
                diff_df[diff_col] = np.nan
        diff_df['rows_present'] = df.loc[:, 'rows_present']
        if self.left_unq_columns:
            diff_df[self.rsuffix + '_missing_columns'] = ', '.join(self.left_unq_columns)
        if self.right_unq_columns:
            diff_df[self.lsuffix + '_missing_columns'] = ', '.join(self.right_unq_columns)

        # Remove the rows where there is no diffs
        if self.keep_only_diff_rows:
            rows_with_diff = diff_rows[diff_rows.apply(any, axis=1)].index
            diff_df = diff_df.loc[rows_with_diff]
        return diff_df

    def _compare(self):
        if self.left.equals(self.right):
            logging.info("left dataset equals right dataset, skipping compare")
            self.diff = None
            return None
        if self._dups:
            self.left = self.handle_duplicate_keys(self.left)
            self.right = self.handle_duplicate_keys(self.right)

        # merge the left and right dataframes for comparison
        self.merge_df = self._merge_dataframe()

        # Multiprocessing diff calculation on partitions of merged dataframe
        if self.pool_size == 1:
            self.diff = self.multiprocess_compare(self.merge_df)
        else:
            with ThreadPool(self.pool_size) as executor:
                merge_df_partitions = np.array_split(self.merge_df, self.pool_size)
                results = executor.map(self.multiprocess_compare, merge_df_partitions)
            for diff_frame in results:
                self.diff = self.diff.append(diff_frame)
        self.drop_non_diff_columns()
        self.metadata = self.prepare_metadata(self.diff)

    def save_output(self, engine):
        self.diff.to_sql('diff', con=engine)
        self.metadata.to_sql('diff_metadata', con=engine)

    @staticmethod
    def color_diff_cells(value):
        """
        Colors elements in a dateframe
        green if positive and red if
        negative. Does not color NaN
        values.
        """

        if value == Constants.DIFF_TRUE:
            color = 'grey'
        elif value < 0:
            color = 'red'
        elif value > 0:
            color = 'green'
        else:
            color = 'black'
        return 'color: %s' % color

    @staticmethod
    def hover(hover_color="#AED6F1"):
        return dict(selector="tr:hover",
                    props=[("background-color", "%s" % hover_color)])

    def style_differ(self):
        # Set CSS properties for th elements in dataframe
        th_props = [
            ('text-align', 'center'),
            ('font-weight', 'bold'),
            ('background-color', '#f7f7f9')
        ]

        # Set CSS properties for td elements in dataframe
        # td_props = [
        #     ('font-size', '11px')
        # ]

        # Set table styles
        styles = [
            Compare.hover(),
            dict(selector="th", props=th_props),
        #    dict(selector="td", props=td_props)
        ]
        return (self.diff.style
                .applymap(Compare.color_diff_cells, subset=self.metadata['diff_column'].to_list())
                .set_caption('This is a custom caption.')
                .set_table_styles(styles))

