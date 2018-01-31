# variant-spark
### Python API for VariantSpark

variant-spark is a scalable toolkit for genome-wide association studies optimized for GWAS like datasets
build on top of Apache Spark.

## Installation

To install **variant-spark** use:

    pip install variant-spark

## Usage

The code below illustates the basic use of variant-spark:

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
    

### Submitting jobs to spark

**variant-spark** is a wrapper on a scala library and the it's jar needs to be passed to `spark-submit`

The location of the jar can be obtained by running:

    varspark-jar
    
To submit a variant-spark file you can either use:

    spark-submit --jars ` varspark-jar` ... 
    
or the a simple wrapper that adds the requied options for you:

    varspark-submit ...
  
### Example: Compute variable importance on a small dataset

Using `varspark-submit`:

    varspark-submit examples/hipster_index.py
    
Using `spark-submit`:

    spark-submit --jars `varspark-jar` examples/hipster_index.py

## Dev Install

Install **variant-spark** for development using:

    git https://github.com/aehrc/VariantSpark.git
    cd VariantSpark/python
    pip install -e .

Running the tests after a dev install above:

    pip install variant-spark[test]
    python -m unittest varspark.test.test_core
    
## Resources

TBP