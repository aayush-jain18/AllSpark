from enum import unique, Enum


class Constants:
    LOG_FORMAT = '%(asctime)-15s - %(levelname)s - %(funcName)s: %(message)s'
    IF_DUPLICATE_KEY_METHODS = ('drop', 'sort', 'aggregate')
    IF_DUPLICATE_ROWS = ('drop', None)
    METADATA_COLUMNS = ['column', 'column_present', 'left_column', 'left_count',
                        'right_column', 'right_count', 'diff_column',
                        'diff_count']
    DIFF_TRUE = 'DIFF.TRUE'


@unique
class PandasDtypes(Enum):
    """The possible types of variables in the Profiling Report."""

    TYPE_CAT = "CAT"
    """A categorical variable"""

    TYPE_BOOL = "BOOL"
    """A boolean variable"""

    TYPE_NUM = "NUM"
    """A numeric variable"""

    TYPE_DATE = "DATE"
    """A date variable"""

    TYPE_URL = "URL"
    """A URL variable"""

    TYPE_PATH = "PATH"
    """Absolute files"""

    S_TYPE_CONST = "CONST"
    """A constant variable"""

    S_TYPE_UNIQUE = "UNIQUE"
    """An unique variable"""
