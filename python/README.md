# Python API for VariantSpark

VariantSpark is a scalable, custom machine learning API for genome-wide association studies (i.e. optimized for GWAS-sized datasets).
VariantSpark is built on top of Apache Spark core. It implements a supervised, wide random forest machine learning algorthim. 

### 1. Install VariantSpark

Prerequisites: 
- Python 2.7 with pip (current version of VariantSpark is not compatible with Python 3)
- PySpark installed in the system either as a python package or from the distribution package. VariantSpark has been tested with Spark 2.1.x
 
Installation:
 
    pip install variant-spark  
    
NOTES and TIPS:  
 - You may need to run this command as 'sudo', use a python virtual environment system such as conda, or use '--user' option.

### 2. Verify VariantSpark

    variant-spark --help

### 3. Submit a Job to VariantSpark

VariantSpark Python API is a wrapper written around a custom ML libary written on  Apache Spark core library (original library is written in Scala). This Scala jar file needs to be passed to `spark-submit` to run VariantSpark using the Pyhton API.

3a. Find the location of the VariantSpark jar file by running this command:

    varspark-jar
    
3b. Use one of two commands to submit a job to VariantSpark.  

    varspark-submit ...
    varspark-submit examples/hipster_index.py  
    --OR--
    spark-submit --jars `varspark-jar` ... 
    spark-submit --jars `varspark-jar` examples/hipster_index.py

**NOTES and TIPS:**   
  - Upload the sample files (from the \examples directory) to your cluster to run these examples. 
  - Edit the path to these files in the JOB definition file, depending on your upload location.   
  - Locate the JOB definition file - **`hipster_index.py`** (in the `/VariantSpark/python/examples` directory)  
  - Locate the DATA files - **`chr22_1000.vcf`** and **`chr22-labels.csv`**  (in the `/VariantSpark/data/` directory)
  - You may need to use the complete path when running this command

### Code Example

The code below shows using the Python API for VariantSpark with an example analysis using the HipsterIndex synthetic phenotype:

    from varspark import VariantsContext
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("HipsterIndex") \
        .getOrCreate()
        
    vs = VariantsContext(spark)
    features = vs.import_vcf(VCF_FILE)
    labels = vs.load_label(LABEL_FILE, LABEL_NAME)
    model  = features \
        .importance_analysis(labels, mtry_fraction = 0.1, seed = 13, n_trees = 200)
    print("Out of bag error = %s" % model.oob_error())
    
**NOTES and TIPS:**
   - You may wish to set other values for the input parameters to the importanceAnalysis function
   - These include the following: mtry_fraction, seed and n_trees

#### Other Information

For more information about how the VariantSpark wide random forest algorithm works, see the main README.md page of this repository.

##### Development Install

Install VariantSpark for development using this command:

    git https://github.com/aehrc/VariantSpark.git
    cd VariantSpark/python
    pip install -e .

Run the tests after a dev install above, using this command:

    pip install variant-spark[test]
    python -m unittest varspark.test.test_core
    
