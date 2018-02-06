#!/usr/bin/env python


import click
import pandas as pd
from itertools import chain
from random import randint
from collections import namedtuple

Individual = namedtuple('Individual', ['id', 'gender'])
MALE = 1
FEMALE = 2

def random_gender():
    return randint(1,2)

def relatives(ped, depth, no_children):
    male_founder_ids  = list(ped[ped['Gender'] == MALE]['Individual ID'].sample(depth))
    female_founder_ids  = list(ped[ped['Gender'] == FEMALE]['Individual ID'].sample(depth))
    def offspring(male_founder_ids, female_founder_ids):
        mate = Individual(male_founder_ids.pop(), MALE)
        for i in range(1, depth):
            father  = mate if mate.gender == MALE else Individual(male_founder_ids.pop(), MALE)
            mother  = mate if mate.gender == FEMALE else Individual(female_founder_ids.pop(), FEMALE)
            
            children = [ Individual("OF%05d_%02d" %(i,k),random_gender()) for k in range(0, no_children)]
            yield [(child.id, father.id, mother.id, child.gender ) for child in children]
            mate = children[-1]
    return pd.DataFrame(list(chain(*list(offspring(male_founder_ids, female_founder_ids)))), 
                       columns = ['Individual ID', 'Paternal ID', 'Maternal ID', 'Gender'])

@click.command()
@click.argument('input_ped', required=True)
@click.argument('output_ped', required=True)
@click.option('--no-gen', help='Number of generations (def = 15)', 
              default=12, required=False, type=int)
@click.option('--no-children', help='Number of children for each generation (def = 2)', 
              default=2, required=False, type=int)
def build_family(input_ped, output_ped, no_gen, no_children):
    input = pd.read_table(input_ped)
    output = relatives(input, no_gen, no_children)
    output.to_csv(output_ped, sep='\t', index=False)

if __name__ == '__main__':
    build_family()