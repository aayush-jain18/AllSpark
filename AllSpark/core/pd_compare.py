import os
import logging

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

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
                 compare='loose',
                 if_duplicate_keys='drop',
                 if_duplicate_rows=None,
                 ignore_extra_columns=False,):

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
        self._dups = self.check_duplicate_keys()
        self.diff = pd.DataFrame()
        self.comd_df = pd.DataFrame()
        self.mtdt_df = pd.DataFrame(columns=Constants.METADATA_COLUMNS)
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
            self._missing_rows = df[df['rows_present'].isin(['left_only',
                                                             'right_only'])].copy()
            # Drop missing rows and extra column
            df.drop(self._missing_rows.index, inplace=True)
            return df
        elif self.on_index:
            logging.info("Merging dataframes for comparison, on basis of index")
            df = pd.merge(self.left, self.right, how='outer', sort=True,
                          left_index=True, right_index=True,
                          indicator=True, suffixes=(self.lsuffix, self.rsuffix))
            self._missing_rows = df[df['rows_present'].isin(['left_only',
                                                             'right_only'])].copy()
            df.drop(self._missing_rows.index, inplace=True)
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
    def compare_object_columns(col1, col2):
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
            either_nan = ((row[0] == np.nan) | (row[1] == np.nan))
            if row[0] != row[1]:
                if either_nan:
                    diff[index] = Constants.DIFF_TRUE
                else:
                    try:
                        float_diff = float(row[0]) - float(row[1])
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
            for index, same in if_num_diff.iteritems():
                if same:
                    diff[index] = np.nan
                elif (col1[index] == np.nan) | (col2[index] == np.nan):
                    diff[index] = Constants.DIFF_TRUE
                else:
                    diff[index] = col1[index] - col2[index]
        return pd.Series(diff, index=col1.index)

    def _extra_columns(self, side, columns):
        self.diff[side + '_missing_columns'] = ', '.join(columns)
        for column in columns:
            if side == self.lsuffix:
                self.mtdt_df = self.mtdt_df.append(
                    {'column': column, 'column_present': self.lsuffix,
                     'left_column': column}, ignore_index=True)
            elif side == self.rsuffix:
                self.mtdt_df = self.mtdt_df.append(
                    {'column': column, 'column_present': self.rsuffix,
                     'right_column': column}, ignore_index=True)

    def compare_column(self, col1, col2):
        col_diff = None
        if_diff_in_col = not col1.equals(col2)
        if if_diff_in_col:
            if np.issubdtype(col1.dtype, np.number) and \
                    np.issubdtype(col2.dtype, np.number):
                col_diff = self.compare_numeric_columns(col1, col2)
            elif col1.dtype.kind == 'M' and col2.dtype.kind == 'M':
                col_diff = self.compare_timeseries_column(col1, col2)
            elif (col1.dtype.kind == 'O') or (col1.dtype.kind == 'O'):
                col_diff = self.compare_object_columns(col1, col2)
        return col_diff

    # TODO: Add entries for column left_count, right_count, diff_count in
    #  metadata dataframe(mtdt_df)
    def _compare(self):
        self.diff_rows = pd.DataFrame()
        if self.left.equals(self.right):
            logging.info("left dataset equals right dataset, skipping compare")
            self.diff = None
            return None
        if self._dups:
            self.left = self.handle_duplicate_keys(self.left)
            self.right = self.handle_duplicate_keys(self.right)

        # merge the left and right dataframes for comparison
        self.comd_df = self._merge_dataframe()

        for column in self.common_columns():
            left_col = column + self.lsuffix
            right_col = column + self.rsuffix
            diff_col = column + self.dsuffix
            col1 = self.comd_df.loc[:, left_col]
            col2 = self.comd_df.loc[:, right_col]
            diff = self.compare_column(col1, col2)
            if diff is not None:
                self.diff[left_col] = col1
                self.diff[right_col] = col2
                self.diff[diff_col] = diff
                self.mtdt_df = self.mtdt_df.append({'column': column,
                                                    'column_present': 'both',
                                                    'left_column': left_col,
                                                    'right_column': right_col,
                                                    'diff_column': diff_col},
                                                   ignore_index=True)
                self.diff_rows[column] = self.get_diff_row(diff)
            else:
                logging.info("%s column equals %s column", left_col, right_col)
        self.diff_rows.index = self.comd_df.index

        # replace nan in unique rows diff columns with boolean True, np.nan
        # to represent no diffs
        self.diff['rows_present'] = self.comd_df.loc[:, 'rows_present']
        missing_rows_diff = self._missing_rows.loc[:, self.diff.columns]
        for column in self.mtdt_df['diff_column']:
            missing_rows_diff[column] = Constants.DIFF_TRUE
        self.diff = self.diff.append(missing_rows_diff)

        if (self.left_unq_columns or self.right_unq_columns) and \
                not self.ignore_extra_columns:
            if self.left_unq_columns:
                self._extra_columns(self.lsuffix, self.left_unq_columns)
            if self.right_unq_columns:
                self._extra_columns(self.rsuffix, self.right_unq_columns)

        # Remove the rows where there is no diffs
        rows_without_diff = ~self.diff_rows.apply(any, axis=1)
        self.diff.drop(self.diff_rows[rows_without_diff].index, inplace=True)
        self.mtdt_df.set_index('column', inplace=True)
        for column in self.mtdt_df.index:
            if self.mtdt_df.loc[column, 'column_present'] == 'both':
                self.mtdt_df.loc[column, 'diff_count'] = self.count_notna(
                    self.diff[column + self.dsuffix])
                self.mtdt_df.loc[column, 'left_count'] = self.count_notna(
                    self.comd_df[column + self.lsuffix])
                self.mtdt_df.loc[column, 'right_count'] = self.count_notna(
                    self.comd_df[column + self.rsuffix])
            elif self.mtdt_df.loc[column, 'column_present'] == self.lsuffix:
                self.mtdt_df.loc[column, 'right_count'] = 0
                self.mtdt_df.loc[column, 'left_count'] = self.count_notna(
                    self.comd_df[column])
                self.mtdt_df.loc[column, 'diff_count'] = self.count_notna(
                    self.comd_df[column])
            elif self.mtdt_df.loc[column, 'column_present'] == self.rsuffix:
                self.mtdt_df.loc[column, 'left_count'] = 0
                self.mtdt_df.loc[column, 'right_count'] = self.count_notna(
                    self.comd_df[column])
                self.mtdt_df.loc[column, 'diff_count'] = self.count_notna(
                    self.comd_df[column])

    def save_output(self, engine):
        self.diff.to_sql('diff', con=engine)
        self.mtdt_df.to_sql('diff_metadata', con=engine)

