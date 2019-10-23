import oyaml as yaml


def load_yaml_file(file):
    """
    Read yaml file into a Dictionary.
    Parameters
    ----------
    file : yaml file to load
    Returns
    -------
    dict
    """

    with open(file, 'r') as stream:
        try:
            return yaml.load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            raise


config = load_yaml_file('allspark_config.yaml')
