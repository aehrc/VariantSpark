#!/usr/bin/env python

import sys
import yaml
import click
import pystache
import json

@click.group()
#this needs to be moved to vnl.sumbmit.main somehow but for now I just to not have any idea how do it
@click.option('--noop', help='Name to greet', is_flag=True)
@click.option('--verbose', help='Name to greet', is_flag=True)
def cli(noop, verbose):
    pass

@cli.command(name='create-cluster')
@click.option('--var',  multiple=True)
@click.option('--profile',  multiple=True)
@click.option('--id-file',  required = False)

def create_cluster(var, profile, id_file):
    
    def split_var_string(s):
        index = s.find('=')
        print(index)
        return (s[0:index],s[index+1:])
    
    def to_aws_option(k,v):
        if "tags" == k:
            return "--%s %s" % (k," ".join("'%s=%s'" % kv for kv in v.items()))            
        elif bool == type(v):
            return ("--%s" if v else "--no-%s") % k
        elif list == type(v) or dict == type(v):
            return "--%s '%s'" % (k,json.dumps(v))
        else:
            return "--%s %s" % (k,json.dumps(v))
        
    """ Command placeholder/template 
    """
    # split by the first == to keys  and values
    vars = dict( split_var_string(s) for s in var)
    print("vars: %s" % vars)    
    with open('profiles/cluster.yaml', 'r') as f:
        template  = f.read()
    config = yaml.load(pystache.render(template, vars))
    print(config)
    options = config['options']
    
    aws_options = [to_aws_option(*kv) for kv in options.items()]
    print(aws_options)
    print(" ".join(aws_options))


if __name__ == '__main__':
    cli()