#!/usr/bin/env python

import click
import pandas as pd
import networkx as nx
from itertools import combinations, chain

def ped_to_graph(ped_df):
    tree = nx.DiGraph()
    for r in ped_df.to_records():
        _,_,offspring, father, mother   = tuple(r)[0:5]
        if father!='0':
            tree.add_edge(father,offspring, rel='father')
        if mother!='0':
            tree.add_edge(mother,offspring, rel='mother') 
    return tree

def relatetness(g, n1, n2):
    n1_ancestors = nx.ancestors(g, n1)
    n1_ancestors.add(n1)
    n2_ancestors = nx.ancestors(g, n2)
    n2_ancestors.add(n2)    
    
    n1_rank = -1
    n2_rank = -1
    n_ancestors = []
    common_ancestors = n1_ancestors & n2_ancestors
    if common_ancestors:
        n1_ranks = sorted([(a, nx.shortest_path_length(g, a, n1)) for a in common_ancestors ], key=lambda t:t[1])
        n2_ranks = sorted([(a, nx.shortest_path_length(g, a, n2)) for a in common_ancestors ], key=lambda t:t[1])
        n1_rank = n1_ranks[0][1]
        n2_rank = n2_ranks[0][1]
        n1_ancestors = [ r[0] for r in n1_ranks if r[1] == n1_rank]
        n2_ancestors = [ r[0] for r in n2_ranks if r[1] == n2_rank]
        # we asssume up to two shares ancestors 
        assert set(n1_ancestors) == set(n2_ancestors), "failed: %s == %s" %(n1_ancestors, n2_ancestors)
        n_ancestors = n1_ancestors
    return [[n_ancestors[0], n1_rank, n2_rank] if len(n_ancestors) > 0 else [],
            [n_ancestors[1], n1_rank, n2_rank] if len(n_ancestors) > 1 else []]


@click.command()
@click.argument('input_ped', required=True)
@click.argument('output_anc', required=True)
def ped_to_ancestry(input_ped, output_anc):
    tree = ped_to_graph(pd.read_table(input_ped))
    df_ancestry = pd.DataFrame([ list(chain(c, *relatetness(tree, *c))) for c in combinations(tree.nodes(), 2)],
            columns=['ID1', 'ID2', 'ANC1', 'DIST_ANC1_1', 'DIST_ANC1_2', 'ANC2', 'DIST_ANC2_1', 'DIST_ANC2_2'])    
    df_ancestry.to_csv(output_anc, index=None)


if __name__ == '__main__':
    ped_to_ancestry()