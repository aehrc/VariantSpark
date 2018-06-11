.. _sec-pyapi:

=======================================
Python API
=======================================


This is the API documentation for ``VariantSpark``, and provides detailed information
on the Python programming interface.

The code below illustrates the basic use of variant-spark:

::

    from varspark import VariantsContext
    from pyspark.sql import SparkSession

    spark = SparkSession.builder\
        .appName("HipsterIndex") \
        .getOrCreate()
        
    vs = VariantsContext(spark)
    features = vs.import_vcf(VCF_FILE)
    labels = vs.load_label(LABEL_FILE, LABEL_NAME)
    model  = features.importance_analysis(labels, mtry_fraction = 0.1, seed = 13, n_trees = 200)
    print("Oob = %s" % model.oob_error())


Contents:

.. toctree::
   :maxdepth: 1
   
   varspark
   varspark.core 
   varspark.hail
   varspark.utils

