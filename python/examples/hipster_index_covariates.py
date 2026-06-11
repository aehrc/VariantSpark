#!/usr/bin/env python

"""
Created 4 Nov 2025

@author: edw222
"""

import os
import varspark as vs
from pyspark.sql import SparkSession
from varspark.variable_type import (
    Continuous,
    Nominal,
    Ordinal,
)

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
    labels = vc.load_response(
        os.path.join(PROJECT_DIR, "data/hipsterIndex/hipster_labels_covariates.txt"),
        "label",
    )
    covtypes = {
        "age": Continuous,
        "PC0": Continuous,
        "PC1": Continuous,
        "PC2": Continuous,
        "sex": Ordinal(2),
        # Note: sex is a nominal variable, but nominal and ordinal are equivalent at 2 levels
        # Ordinal is used here to demonstrate the API; use Nominal in practice
        "lifestyle": Nominal(4),
    }
    covariates = vc.import_covariates(
        os.path.join(PROJECT_DIR, "data/hipsterIndex/hipster_labels_covariates.txt"),
        covtypes,
    )
    data = vc.union_feature_sources(genotypes, covariates)

    rf_model = vs.RandomForestClassifier(
        vc, seed=13, mtry_fraction=0.05, min_node_size=5, max_depth=10
    )
    rf_model.fit_trees(data, labels, n_trees=100, batch_size=50)

    print("OOB error: %s" % rf_model.oob_error())
    ia = rf_model.importance_analysis()
    print(ia.important_variables(limit=5).head())

    rf_model.export_to_json(
        os.path.join(PROJECT_DIR, "target/hipster-index-model.json"), True
    )


if __name__ == "__main__":
    main()
