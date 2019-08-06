import pandas as pd
import numpy as np

from ..constants import Constants


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
    ignore_spaces : bool, optional
        Flag to strip whitespace (including newlines) from string columns
    ignore_case : bool, optional
        Flag to ignore the case of string columns
    if_duplicate_key:
    if_duplicate_rows:
    Attributes
    ----------

    """
    def __init__(self, *,
                 left,
                 right,
                 key_columns=False,
                 output_path=None,
                 atol=0,
                 rtol=0,
                 lsuffix='_left',
                 rsuffix='_right',
                 dsuffix='_diff',
                 ignore_case=False,
                 ignore_spaces=False,
                 compare='loose',
                 if_duplicate_key='drop',
                 if_duplicate_rows=None):

        if key_columns is None:
            raise Exception("Provide key_columns")
        elif isinstance(key_columns, str):
            self.key_columns = [key_columns.lower()]
            self.on_index = False
        else:
            self.key_columns = [col.lower() for col in key_columns]
            self.on_index = False

        # if os.path.exists(output_path):
        #     self.output_file = output_path + '.db'

        # TODO: Add method for duplicate keys
        # if if_dupes in Constants.duplicate_strategy:
        #     self._if_dupes = if_dupes
        # else:
        #     raise ValueError(f"{if_dupes} not a valid strategy")

        self.left = left
        self.right = right
        self.lsuffix = lsuffix
        self.rsuffix = rsuffix
        self.dsuffix = dsuffix
        self.atol = atol
        self.rtol = rtol
        self._dupes = False
        self.diff_df = pd.DataFrame()
        self._diff_rows = pd.DataFrame()
        self.metadata_df = pd.DataFrame(columns=Constants.METADATA_COLUMNS)
        self._compare(ignore_spaces, ignore_case)
        if compare.lower() in ['strict', 'loose']:
            self.compare = compare.lower()
        else:
            raise ValueError(f'Unknown compare option: "{compare}"')

    @property
    def left(self):
        return self._left

    @left.setter
    def left(self, left):
        """Check that it is a dataframe and has the join columns"""
        self._left = left
        self._validate_dataframe("left")

    @property
    def right(self):
        return self._right

    @right.setter
    def right(self, right):
        """Check that it is a dataframe and has the join columns"""
        self._right = right
        self._validate_dataframe("right")

    def _validate_dataframe(self, dataframe):
        """
        """
        pass

    @property
    def unq_columns(self):
        """
        """
        left_unq_columns = set(self.left.columns) - set(self.right.columns)
        right_unq_columns = set(self.right.columns) - set(self.left.columns)
        return {self.rsuffix: left_unq_columns, self.rsuffix: right_unq_columns}

    def common_columns(self):
        """
        """
        return (set(self.left.columns) & set(self.right.columns)) - set(self.key_columns)

    def if_duplicate_keys(self):
        """
        """
        if self.key_columns:
            duplicate_keys = any(self.left[[self.key_columns]].duplicated() |
                                 self.right[self.key_columns].duplicated())
        elif self.on_index:
            duplicate_keys = any(self.left.index.duplicated() |
                                 self.right.index.duplicated())
        return duplicate_keys

    @staticmethod
    def get_diff_row(diff):
        return pd.Series(diff.isnull() | diff | diff != 0)

    @staticmethod
    def compare_timeseries_column(col1, col2):
        """

        Parameters
        ----------
        col1
        col2

        Returns
        -------

        """
        return col1 - col2

    @staticmethod
    def compare_object_columns(col1, col2):
        diff = pd.Series()
        diff = [False] * len(col2)
        for index, row in enumerate(zip(col1, col2)):
            if row[0] != row[1]:
                try:
                    if_float_diff_possible = float(row[0]) - float(row[1])
                    diff[index] = float(if_float_diff_possible)
                except ValueError:
                    diff[index] = True
        return diff

    @staticmethod
    def compare_numeric_columns(col1, col2, atol=0, rtol=0):
        diff = pd.Series()
        if_num_diff = pd.Series(np.isclose(col1, col2, rtol=rtol, atol=atol,
                                           equal_nan=True))
        if not all(if_num_diff):
            if (atol == 0) and (rtol == 0):
                diff = col1 - col2
            else:
                diff = [col1[index] - col2[index] if not boolean else 0
                        for index, boolean in enumerate(if_num_diff)]
        return diff

    def compare_column(self, col1, col2):
        col_diff = None
        if_diff_in_col = not all(col1 == col2)
        if if_diff_in_col:
            if np.issubdtype(col1.dtype, np.number) and \
                    np.issubdtype(col2.dtype, np.number):
                col_diff = self.compare_numeric_columns(col1, col2)
            elif col1.dtype.kind == 'M' and col2.dtype.kind == 'M':
                col_diff = self.compare_timeseries_column(col1, col2)
            elif (col1.dtype.kind == 'O') or (col1.dtype.kind == 'O'):
                col_diff = self.compare_object_columns(col1, col2)
        return col_diff

    def _merge_dataframe(self):
        """
        Merge DataFrame or named Series objects with a database-style join.
        The join is done on columns or indexes. If joining columns on
        columns, the DataFrame indexes *will be ignored*.
        """
        self._merged_df = self.left.merge(self.right, how='outer', sort=True,
                                          on=self.key_columns,
                                          suffixes=(self.lsuffix, self.rsuffix),
                                          indicator=True).set_index(self.key_columns)

    def _compare(self, ignore_spaces, ignore_case):
        if self.left.equals(self.right):
            return None

        # merge the two dataframes on basis of keys or index
        self._merge_dataframe()
        self._missing_rows = self._merged_df[self._merged_df['_merge'].isin(['left_only', 'right_only'])].copy()

        # Drop missing rows
        self._merged_df.drop(self._merged_df[self._merged_df['_merge'].isin(['left_only', 'right_only'])].index, inplace=True)
        for column in self.common_columns():
            left_col = column + self.lsuffix
            right_col = column + self.rsuffix
            diff_col = column + self.dsuffix
            col1 = self._merged_df.loc[:, left_col]
            col2 = self._merged_df.loc[:, right_col]
            diff = self.compare_column(col1, col2)
            if diff is not None:
                self.diff_df[left_col] = col1
                self.diff_df[right_col] = col2
                self.diff_df[diff_col] = diff
                self.metadata_df = self.metadata_df.append({'column': column, 'left_column': left_col, 'right_column': right_col, 'diff_column': diff_col}, ignore_index=True)
            else:
                pass

        # TODO: let's take care of extra columns in either dataframe
        for side, columns in self.unq_columns.items():
            if side == self.lsuffix:
                self.diff_df[:, self.lsuffix + '_missing_columns'] = [columns] * len(self.diff_df)
                for column in columns:
                    self.metadata_df = self.metadata_df.append(
                        {'column': column, 'left_column': column}, ignore_index=True)
            elif side == self.rsuffix:
                self.diff_df[self.rsuffix + '_missing_columns'] = [columns] * len(self.diff_df)
                for column in columns:
                    self.metadata_df = self.metadata_df.append(
                        {'column': column, 'left_column': column}, ignore_index=True)
        self.metadata_df.set_index('column', inplace=True)
        # TODO: Remove the rows where there is no diffs
        # for columns in
        # diff_rows = self.get_diff_row(col_diff)
        # self._diff_rows[diff_col] = diff_rows
        self.diff_df['row present'] = self._merged_df.loc[:, '_merge'].replace('both', '')
