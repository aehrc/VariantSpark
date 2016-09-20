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
 '--num-executors':'64', 
 '--executor-memory':'4G', 
 '--driver-memory':'4G'
}


BIG_SPARK_OPTIONS = {
 '--master':'yarn-client', 
 '--num-executors':'128', 
 '--executor-memory':'6G', 
 '--driver-memory':'6G'
}

@click.group()
@click.option('--local', required = False, is_flag=True)
@click.pass_context
def cmd(ctx, **kwargs):
    ctx.obj = kwargs
    pass

@click.pass_context
def run_variant_spark(ctx, cmd, args, spark_args=DEF_SPARK_OPTIONS, output = None):
    print("Context: %s" % ctx.obj)
    spark_runner =  "--local --" if ctx.obj['local'] else "--spark %s --" % " ".join(map(" ".join,spark_args.items()))
    vs_cmd = path.join(BASE_DIR, 'variant-spark')
    cmd_line = "%s %s  %s %s" % (vs_cmd, spark_runner, cmd ,  " ".join(map(" ".join,args.items())))
    print("Running: %s" % cmd_line)
    print("Output to: %s" % output)
    with open(output, "w") as outfile:
        exit_code = subprocess.call(shlex.split(cmd_line), stdout = outfile, stderr = subprocess.STDOUT)
    print("Exit code: %s" % exit_code)
    
def run_gen_data(output_dir, nvars, nsamples):
    run_variant_spark('gen-features', {
                '--gen-n-samples':nsamples,
                '--gen-n-variables': nvars, 
                '--spark-par':'256',
                '--seed':'13',
                '--output-file':path.join(output_dir, "data_s%s_v%s.parquet" %(nsamples, nvars))
                }, output = path.join(output_dir, "data_s%s_v%s.out" %(nsamples, nvars)))


@cmd.command()
@click.option('--nvars', '-v', multiple=True, required = True)
@click.option('--nsamples', '-s', multiple=True, required = True)
@click.option('--output-dir', required = True)
def gen_data(output_dir, **kwargs):
    search_grid = ParameterGrid(kwargs)
    for args in search_grid:
        run_gen_data(output_dir = output_dir, **args)

def run_gen_labels(data_dir, nvars, nsamples):
    run_variant_spark('gen-labels', {
                '--feature-column':'resp',
                '--feature-continous-column':'resp_cont',
                '--feature-file': path.join(data_dir, "labels_s%s_v%s.csv" %(nsamples, nvars)),
                '-ge v_10:0.85 -ge v_100:0.9 -ge v_1000:1.1 -ge v_10000:1.0 -ge v_100000:1.15':'',
                '--spark-par':'256',
                '--seed':'13',
                '--input-file':path.join(data_dir, "data_s%s_v%s.parquet" %(nsamples, nvars))
                }, output = path.join(data_dir, "labels_s%s_v%s.out" %(nsamples, nvars)))


@cmd.command()
@click.option('--nvars', '-v', multiple=True, required = True)
@click.option('--nsamples', '-s', multiple=True, required = True)
@click.option('--data-dir', required = True)
def gen_labels(data_dir, **kwargs):
    search_grid = ParameterGrid(kwargs)
    for args in search_grid:
        run_gen_labels(data_dir = data_dir, **args)


def run_importance(data_dir, nvars, nsamples, mtry, times, ntree='100'):
    run_variant_spark('importance', {
                '--feature-column':'resp',
                '--feature-file': path.join(data_dir, "labels_s%s_v%s.csv" %(nsamples, nvars)),
                '--rf-mtry-fraction':mtry,
                '--rf-n-trees':ntree,
                '--rf-batch-size':'50',
                '--rf-oob':'',
                '--spark-par':'256',
                '--seed':'13',
                '-v':'',
                '--input-type':'parquet',
                '--input-file':path.join(data_dir, "data_s%s_v%s.parquet" %(nsamples, nvars))
                }, output = path.join(data_dir, "importance_s%s_v%s_m%s_t%s.%s.out" %(nsamples, nvars, mtry, ntree, times)), 
            spark_args = BIG_SPARK_OPTIONS)



class BasedIntParamType(click.ParamType):
    name = 'integer'

    def convert(self, value, param, ctx):
        try:
            return range(0, int(value))
        except ValueError:
            self.fail('%s is not a valid integer' % value, param, ctx)

BASED_INT = BasedIntParamType()


@cmd.command()
@click.option('--nvars', '-v', multiple=True, required = True)
@click.option('--nsamples', '-s', multiple=True, required = True)
@click.option('--mtry', '-m', multiple=True, required = True)
@click.option('--times', required = False, default='1', type=BASED_INT)
@click.option('--data-dir', required = True)
def importance(data_dir, **kwargs):
    search_grid = ParameterGrid(kwargs)
    for args in search_grid:
        run_importance(data_dir = data_dir, **args)


if __name__ == '__main__':
    cmd(obj={})
