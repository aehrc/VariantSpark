#!/usr/bin/env python

import sys
import argparse
import os

EMR_PARSER = argparse.ArgumentParser(description='EMR Options')
EMR_PARSER.add_argument('--cluster-id', required = True)
EMR_PARSER.add_argument('--step-name', default = 'variant-spark')
EMR_PARSER.add_argument('--action-on-failure', default = 'CONTINUE')


#aws emr add-steps --cluster-id j-4Z2Z6XAC6D32 --steps Type=Spark,Name="Spark Program",ActionOnFailure=CONTINUE,Args=[--class,au.csiro.variantspark.cli.VariantSparkApp,/mnt/variant-spark-0.0.2/lib/variant-spark_2.11-0.0.2-SNAPSHOT-all.jar,importance,-if,s3://au.csiro.pbdava.test/variant-spark/data/chr22_1000.vcf,-ff,s3://au.csiro.pbdava.test/variant-spark/data/chr22-labels.csv,-fc,22_16051249,-v,-rn,500,-rbs,20]

EMR_TEMPL = "aws emr add-steps --cluster-id %(cluster_id)s --steps Type=Spark,Name='%(step_name)s',ActionOnFailure=%(action_on_failure)s,Args=[%(arg_list)s]"
VS_EMR_ARGS = ['--class','au.csiro.variantspark.cli.VariantSparkApp','/mnt/variant-spark-0.0.2/lib/variant-spark_2.11-0.0.2-SNAPSHOT-all.jar']
def emr_runner(runner_args):
    def run(args):
        print("Running EMR: %s, %s, %s" % (emr_options, other_args, args))
        emr_cmd = EMR_TEMPL % dict(vars(emr_options), arg_list = ",".join(VS_EMR_ARGS +  other_args + args))
        print("EMR: %s" % emr_cmd)
        os.system(emr_cmd)
        
    emr_options, other_args = EMR_PARSER.parse_known_args(runner_args)
    return run
    

def spark_runner():
    def run(args):
        print("Running: %s" % args)
    return run

def get_runner(runner_args):
    if runner_args:
        if '--emr' == runner_args[0]:
            return emr_runner(runner_args[1:-1])
    return spark_runner()

def split_params(args):
    index = 0
    if args and args[0].startswith('--'):
        index = args.index('--') + 1
    return (args[0:index], args[index:])

def run(args):
    print(args)
    runner_args, prog_args = split_params(args)
    print(runner_args, prog_args)
    runner = get_runner(runner_args)
    runner(prog_args)
    
def cli():
    run(sys.argv[1:])

if __name__ == '__main__':
    cli()
