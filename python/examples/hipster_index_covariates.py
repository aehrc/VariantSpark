#!/usr/bin/env python

"""
Created 4 Nov 2025

@author: edw222
"""
import os
import varspark as vs
from pyspark.sql import SparkSession

PROJECT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
)


def main():
    spark = vs.configure_spark(
        SparkSession.builder.config("spark.jars", vs.find_jar())
    ).getOrCreate()
    vc = vs.VarsparkContext(spark)

    genotypes = vc.import_vcf(
        os.path.join(PROJECT_DIR, "data/hipsterIndex/hipster.vcf.bgz")
    )
    labels = vc.load_label(
        os.path.join(PROJECT_DIR, "data/hipsterIndex/hipster_labels_covariates.txt"),
        "label",
    )
    covtypes = {
        "age": "CONTINUOUS",
        "PC0": "CONTINUOUS",
        "PC1": "CONTINUOUS",
        "PC2": "CONTINUOUS",
    }
    covariates = vc.import_covariates(
        os.path.join(PROJECT_DIR, "data/hipsterIndex/hipster_labels_covariates.txt"),
        covtypes,
    )
    data = vc.union_features_and_covariates(genotypes, covariates)

    rf_model = vs.RandomForestModel(
        vc, seed=13, mtry_fraction=0.05, min_node_size=5, max_depth=10
    )
    rf_model.fit_trees(data, labels, n_trees=100, batch_size=50)

    print("OOB error: %s" % rf_model.oob_error())
    ia = rf_model.importance_analysis()
    print(ia.important_variables(limit=5).head())

    rf_model.export_to_json(
        os.path.join(PROJECT_DIR, "target/chr22_1000_GRCh38-model.json"), True
    )


if __name__ == "__main__":
    main()
