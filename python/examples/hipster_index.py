'''
Created on 24 Jan 2018

@author: szu004
'''
import os
from varspark import VariantsContext
from pyspark.sql import SparkSession

PROJECT_DIR=os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))

def main():
    print(VariantsContext.spark_conf())
    spark = SparkSession.builder\
        .config(conf=VariantsContext.spark_conf()) \
        .appName("HipsterIndex") \
        .getOrCreate()
    vs = VariantsContext(spark)
    features = vs.import_vcf(os.path.join(PROJECT_DIR, 'data/chr22_1000.vcf'))
    labels = vs.load_label(os.path.join(PROJECT_DIR,'data/chr22-labels.csv'), '22_16050408')
    model  = features.importance_analysis(labels, mtry_fraction = 0.1, seed = 13, n_trees = 200)
    print("Oob = %s" % model.oob_error())
    for entry in model.important_variables(10):
        print(entry)
        
if __name__ == '__main__':
    main()

