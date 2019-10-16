import logging

import pandas as pd
import click
from sqlalchemy import create_engine

from AllSpark.core.pd_compare import Compare
from AllSpark.constants import Constants

logging.basicConfig(format=Constants.LOG_FORMAT, level=logging.INFO)


@click.group(chain=True)
def allspark():
    pass


@allspark.command('pandas-compare')
@click.option('-l', '--left_file',
              help='',
              type=click.Path(exists=True, file_okay=True, dir_okay=True,
                              readable=True), required=True)
@click.option('-r', '--right_file',
              help='',
              type=click.Path(exists=True, file_okay=True, dir_okay=True,
                              readable=True), required=True)
@click.option('-k', '--key-column',
              help='',
              multiple=True)
@click.option('-o', '--output',
              help='',
              type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.option('--ltype',
              help='',
              type=click.Choice(['csv', 'tsv', 'excel', 'json', 'html']))
@click.option('--rtype',
              help='',
              type=click.Choice(['csv', 'tsv', 'excel', 'json', 'html']))
@click.option('--atol',
              help='',
              type=float, default=0)
@click.option('--rtol',
              help='',
              type=float, default=0)
@click.option('-db', '--database', help='', type.click.Choice(['sqlite', 'postgres', 'mysql', 'oracle']))
def pandas_compare(left_file, right_file, key_column, output, ltype, rtype,
                   atol, rtol, db):
    """Compare two pandas dataframe and stores the diff report in Database"""
    left = pd.read_csv(left_file)
    right = pd.read_csv(right_file)
    diff = Compare(left=left, right=right, key_columns=key_column, atol=atol, rtol=rtol)
    diff.save_report()

@allspark.command('spark-compare')
@click.option('-l', '--left_file', help='',
              type=click.Path(exists=True, file_okay=True, dir_okay=True, readable=True),
              )
@click.option('-r', '--right_file',
              type=click.Path(exists=True, file_okay=True, dir_okay=True,
                              readable=True))
@click.option('-k', '--key-column', multiple=True)
@click.option('-o', '--output',
              type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.option('--ltype',
              type=click.Choice(['csv', 'tsv', 'excel', 'json', 'html']))
@click.option('--rtype',
              type=click.Choice(['csv', 'tsv', 'excel', 'json', 'html']))
@click.option('--atol', type=float)
@click.option('--rtol', type=float)
def spark_compare(left_file, right_file, key_column, output, ltype, rtype, atol, rtol):
    raise NotImplementedError
