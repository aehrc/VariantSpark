.. VariantS documentation master file, created by
   sphinx-quickstart on Fri Nov 17 15:37:37 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

=======================================
Welcome to variant-spark documentation!
=======================================


**variant-spark** is a scalable toolkit for genome-wide association studies optimized for GWAS like datasets. 

Machine learning methods and, in particular, random forests (RFs) are a promising alternative to standard single SNP analysis in genome-wide association studies (GWAS). 
RFs provide variable importance measures to rank SNPs according to their predictive power.
Although there are number of existing random forest implementations available, some even parallel or distributed such as: Random Jungle, ranger or SparkML,
most of them are not optimized to deal with GWAS datasets, which usually come with thousands of samples and millions of variables.

**variant-spark** currently provides the basic functionality of building random forest model and estimating variable importance with mean decrease gini method and can operate on VCF and CSV files. 
Future extensions will include support of other importance measures, variable selection methods and data formats. 

**variant-spark** utilizes a novel approach of building random forest from data in transposed representation, which allows it to efficiently deal with even extremely wide GWAS datasets. 
Moreover, since the most common genomics variant calls VCF and uses the transposed representation, variant-spark can work directly with the VCF data, without the costly pre-processing required by other tools.

**variant-spark** is built on top of Apache Spark – a modern distributed framework for big data processing, which gives variant-spark the ability to to scale horizontally on both bespoke cluster and public clouds.

The potential users include:

- Medical researchers seeking to perform GWAS-like analysis on large cohort data of genome wide sequencing data or imputed SNP array data.
- Medical researchers or clinicians seeking to perform clustering on genomic profiles to stratify large-cohort genomic data
- General researchers with classification or clustering needs of datasets with millions of features.


Contents:
---------

.. toctree::
   :maxdepth: 2
   
   getting_started
   cmd_ref
   pyapi
   development
   
   
Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

