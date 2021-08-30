#!/usr/bin/env python
import click
import json
from itertools import chain
import pandas as pd


def traverse_tree(node, acc, fun):
    acc.append(fun(node))
    if 'left' in node.keys():
        traverse_tree(node['left'], acc, fun)
    if 'right' in node.keys():
        traverse_tree(node['right'], acc, fun)
    return acc

def split_vars_from_tree(tree):
    return filter(lambda v: v is not None,
                  traverse_tree(tree['rootNode'], [], lambda n: n['splitVar'] if 'splitVar' in n else None))

def split_var_from_model(rf_model):
    return chain.from_iterable(map(split_vars_from_tree,  rf_model['trees']))

@click.command()
@click.option('-i', '--input-model', required = True, type=click.Path(exists=True),
              help='Input .json file wile model saved from VariantSpark')
@click.option('-o', '--output-counts', required = True, type = click.Path(exists=False),
              help='Output .tsv file with split variable counts')
def extract_split_var_counts(input_model, output_counts):
    """
        Extract counts of split variables from a json representation
        of variant spark random forest model
    """
    click.echo("Reading model from: `%s` ..." % input_model)
    with open(input_model, 'r') as model_f:
        rf_model = json.load(model_f)
    click.echo("Loaded %s trees." % len(rf_model['trees']))
    click.echo("Computing variable counts ...")
    split_variables = pd.Series(list(split_var_from_model(rf_model)))
    split_variable_counts = split_variables.value_counts()
    split_variable_counts_df  = pd.DataFrame.from_dict(dict(var_name = split_variable_counts.index,
                                                            count = split_variable_counts))
    click.echo(split_variable_counts)
    click.echo("Writing split variable counts to: `%s`" % output_counts)
    split_variable_counts_df.to_csv(output_counts, index=False, sep='\t')

if __name__ == "__main__":
    extract_split_var_counts()