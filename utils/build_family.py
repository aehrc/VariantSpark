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

def  opposite_sex(gender):
    return FEMALE if gender == MALE else MALE

def relatives(ped, depth, no_replicas, no_children = 2):
    male_founders = ped[ped['Gender'] == MALE]['Individual ID']
    female_founders = ped[ped['Gender'] == FEMALE]['Individual ID']
    founders = [
            list(male_founders.sample(len(male_founders))),
            list(female_founders.sample(len(female_founders)))
    ]
    def get_founder(gender):
        return Individual(founders[gender-1].pop(), gender)
    def offspring():
        nestors = [ get_founder(random_gender()) for i in range(0, no_replicas)]
        yield [(nestor.id, '0', '0', nestor.gender) for nestor in nestors]
        for i in range(1, depth):
            print("Level: %s" % i)
            print("Nestors: %s" % nestors)
            spouses = [ get_founder(opposite_sex(n.gender)) for n in nestors]
            print("Spouses: %s" %spouses)
            yield [(spouse.id, '0', '0', spouse.gender) for spouse in spouses]
            luckies = []
            for (l, (nestor, spouse)) in enumerate(zip(nestors, spouses)):
                father  = nestor if nestor.gender == MALE else spouse
                mother  = nestor if nestor.gender == FEMALE else spouse
                children = [ Individual("OF%02d_%04d_%02d" %(i,l,k),random_gender()) for k in range(0, no_children)]
                yield [(child.id, father.id, mother.id, child.gender ) for child in children]
                luckies.append(children[-1])
            nestors = luckies
    return pd.DataFrame(list(chain(*list(offspring()))), 
                       columns = ['Individual ID', 'Paternal ID', 'Maternal ID', 'Gender'])

@click.command()
@click.argument('input_ped', required=True)
@click.argument('output_ped', required=True)
@click.option('--no-gen', help='Number of generations (def = 15)', 
              default=12, required=False, type=int)
@click.option('--no-replicas', help='Number of replicas of the basic tree (def = 1)', 
              default=1, required=False, type=int)
def build_family(input_ped, output_ped, no_gen, no_replicas):
    input = pd.read_table(input_ped)
    output = relatives(input, no_gen, no_replicas)
    output.to_csv(output_ped, sep='\t', index=False)

if __name__ == '__main__':
    build_family()