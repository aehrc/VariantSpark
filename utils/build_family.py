#!/usr/bin/env python

import click
import pandas as pd
from itertools import chain
from random import randint
from collections import namedtuple

Individual = namedtuple('Individual', ['id', 'gender', 'family'])
MALE = 1
FEMALE = 2

DEF_PHENOTYPE = 0

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
    def get_founder(gender, family_id = None):
        founder_id = founders[gender-1].pop()
        return Individual(founder_id, gender, family_id or founder_id)
    def offspring():
        nestors = [ get_founder(random_gender()) for i in range(0, no_replicas)]
        yield [(nestor.family, nestor.id, '0', '0', nestor.gender, DEF_PHENOTYPE) for nestor in nestors]
        for i in range(1, depth):
            print("Level: %s" % i)
            print("Nestors: %s" % nestors)
            spouses = [ get_founder(opposite_sex(n.gender), n.family) for n in nestors]
            print("Spouses: %s" %spouses)
            yield [(spouse.family, spouse.id, '0', '0', spouse.gender, DEF_PHENOTYPE) for spouse in spouses]
            luckies = []
            for (l, (nestor, spouse)) in enumerate(zip(nestors, spouses)):
                father  = nestor if nestor.gender == MALE else spouse
                mother  = nestor if nestor.gender == FEMALE else spouse
                children = [ Individual("OF%02d_%04d_%02d" %(i,l,k),random_gender(), father.family) for k in range(0, no_children)]
                yield [(child.family, child.id, father.id, mother.id, child.gender, DEF_PHENOTYPE) for child in children]
                luckies.append(children[-1])
            nestors = luckies
    return pd.DataFrame(list(chain(*list(offspring()))), 
                       columns = ['Family Id', 'Individual ID', 'Paternal ID', 'Maternal ID', 'Gender', 'Phenotype'])

@click.command()
@click.argument('input_ped', required=True)
@click.argument('output_ped', required=True)
@click.option('--no-gen', help='Number of generations (def = 15)', 
              default=12, required=False, type=int)
@click.option('--no-replicas', help='Number of replicas of the basic tree (def = 1)', 
              default=1, required=False, type=int)
@click.option('--header/--no-header', help='Write the header to the output ped', 
              default=True, required=False, is_flag=True)
def build_family(input_ped, output_ped, no_gen, no_replicas, header):
    input = pd.read_table(input_ped)
    output = relatives(input, no_gen, no_replicas)
    output.to_csv(output_ped, sep='\t', index=False, header = header)

if __name__ == '__main__':
    build_family()