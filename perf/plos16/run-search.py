from __future__ import (
    absolute_import,
    division,
    print_function)
# from builtins import *

import click
from sklearn.grid_search import ParameterGrid
from os import path
import subprocess
import shlex

BASE_DIR=path.abspath(path.join(path.dirname(__file__),'../..'))

DEF_SPARK_OPTIONS = {
 '--master':'yarn-client', 
 '--num-executors':32, 
 '--executor-memory':'4G', 
 '--driver-memory':'4G'
}


@click.group()
def cmd():
    pass

def run_variant_spark(cmd, args, spark_args=DEF_SPARK_OPTIONS):
    spark_runner = "--local --"
    cmd_line = "%s/variant-spark %s  %s %s" % (BASE_DIR, spark_runner, cmd ,  " ".join(map(" ".join,args.items())))
    print(cmd_line)
    print(BASE_DIR)
    subprocess.call(shlex.split(cmd_line))
    

def run_gen_data(nvars, nsamples):
   run_variant_spark('gen-features', {
                '--gen-n-samples':nsamples,
                '--gen-n-variables': nvars, 
                '--spark-par':'256',
                '--seed':'13',
                '--output-file':"data_s%s_v%s.parquet" %(nsamples, nvars) 
                })


@cmd.command()
@click.option('--nvars', '-v', multiple=True, required = True)
@click.option('--nsamples', '-s', multiple=True, required = True)
def gen_data(**kwargs):
    search_grid = ParameterGrid(kwargs)
    for args in search_grid:
        run_gen_data(**args)

@cmd.command()
@click.option('--input', '-i', multiple=True)
def hello(input):
    search_grid = ParameterGrid({'samples': [1000, 5000, 10000], 'variables':[ 100000, 200000000]})
    print(list(search_grid))
    print(input)


if __name__ == '__main__':
    cmd(obj={})