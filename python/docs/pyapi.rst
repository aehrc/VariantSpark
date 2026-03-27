.. _sec-pyapi:

=======================================
Python API
=======================================


This is the API documentation for ``VariantSpark``, and provides detailed information
on the Python programming interface.

The code below illustrates the basic use of variant-spark:

::

    import varspark as vs
    from pyspark.sql import SparkSession

    spark = vs.configure_spark(
        SparkSession.builder.appName("HipsterIndex")
    ).getOrCreate()

    vc = vs.VarsparkContext(spark)
    features = vc.import_vcf(VCF_FILE)
    labels = vc.load_response(LABEL_FILE, LABEL_NAME)

    rf = vs.RandomForestClassifier(vc, mtry_fraction=0.1, seed=13)
    rf.fit_trees(features, labels, n_trees=200)
    print("Oob = %s" % rf.oob_error())
    ia = rf.importance_analysis()
    print(ia.important_variables())


Contents:

.. toctree::
   :maxdepth: 1
   
   varspark
   varspark.core 

