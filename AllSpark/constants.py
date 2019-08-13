class Constants:
    LOG_FORMAT = '%(asctime)-15s - %(levelname)s - %(funcName)s: %(message)s'
    IF_DUPLICATE_KEY_METHODS = ('drop', 'sort', 'aggregate')
    IF_DUPLICATE_ROWS = ('drop', None)
    METADATA_COLUMNS = ['column', 'column_present', 'left_column', 'left_count',
                        'right_column', 'right_count', 'diff_column',
                        'diff_count']
    DIFF_TRUE = 'AllSpark.Compare.True'


