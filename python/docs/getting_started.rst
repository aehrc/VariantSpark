.. _sec-getting_started:

=======================================
Getting Started
=======================================

**VariantSpark** is currently supported only on Unix-like system (Linux, MacOS). 

You’ll need:

- The Java 8 JDK.
- Spark 2.2.1. **VariantSpark** is compatible with Spark 2.1+
- Python 2.7 and Jupyter Notebooks. We recommend the free Anaconda distribution.

It’s easy to run locally on one machine - you need to have Apache Spark installed correctly  with:

- ``spark-submit`` and ``pyspark`` on you system ``PATH``

Get **VariantSpark** distribution from the downloads page of project web site. 
Untar the distribution after you download it (you may need to change the name of the file to match the current version). 

::

    tar -xzf variant-spark_2.11-0.2.0.tar.gz


Next, edit and copy the below bash commands to set up the VariantSpark environment variables. 
You may want to add these to the appropriate dot-file (we recommend ``~/.profile``) so that you don’t need to rerun these commands in each new session.

Here, fill in the path to the un-tared VariantSpark distribution.

::

    export VARSPARK_HOME=???
    export PATH=$PATH:$VS_HOME/bin/

Now you should be all setup to run **VariantSpark** command line tool. 

::

    variant-spark -h

The `-h` option displays the help on available commands. To find out more about the command line tool please visit :ref:`sec-cmd_ref`.


Running examples 
----------------

**VariantSpark** comes with several sample programs and datasets. Command-line and Python Jupyter examples are ``examples`` directory.
There is a few small data sets in the ``data`` directory suitable for running on a single machine. 


For example
::

./examples/local_run-importance-ch22.sh 

runs variable importance command on a small sample of the chromosome 22 vcf file (from 1000 Genomes Project). 
``TODO: add a bit more description about what's happening here'
::

    variant-spark --spark --master local[*] -- importance -if /Users/szu004/dev/variant-spark/target/dist/variant-spark_2.11-0.2.0-SNAPSHOT/data/chr22_1000.vcf -ff /Users/szu004/dev/variant-spark/target/dist/variant-spark_2.11-0.2.0-SNAPSHOT/data/chr22-labels.csv -fc 22_16051249 -v -rn 500 -rbs 20 -ro


::

    18/07/02 13:50:37 INFO ImportanceCmd: Running with params: au.csiro.variantspark.cli.ImportanceCmd@5136207f[inputFile=/Users/szu004/dev/variant-spark/target/dist/variant-spark_2.11-0.2.0-SNAPSHOT/data/chr22_1000.vcf,inputType=vcf,varOrdinalLevels=3,inputVcfBiallelic=false,inputVcfSeparator=_,featuresFile=/Users/szu004/dev/variant-spark/target/dist/variant-spark_2.11-0.2.0-SNAPSHOT/data/chr22-labels.csv,featureColumn=22_16051249,outputFile=<null>,nVariables=20,includeData=false,modelFile=<null>,nTrees=500,rfMTry=-1,rfMTryFraction=NaN,rfEstimateOob=true,rfRandomizeEqual=false,rfSubsampleFraction=NaN,rfSampleNoReplacement=false,rfBatchSize=20,randomSeed=-2211036639763447309,sparkPar=0,beVerbose=true,beSilent=false,conf=org.apache.spark.SparkConf@5411dd90,spark=org.apache.spark.sql.SparkSession@50194e8d,sc=org.apache.spark.SparkContext@3cee53dc,sqlContext=<null>]
    Finding  20  most important features using random forest
    Loading header from VCF file: /Users/szu004/dev/variant-spark/target/dist/variant-spark_2.11-0.2.0-SNAPSHOT/data/chr22_1000.vcf
    VCF Version: VCF4_1
    VCF Header: [VCFHeader:
        FORMAT=<ID=GT,Number=1,Type=String,Description="Phased Genotype">
    ...
        INFO=<ID=SNPSOURCE,Number=.,Type=String,Description="indicates if a snp was called when analysing the low coverage or exome alignment data">
        reference=GRCh37
    ]
    Loaded rows: [HG00096,HG00097,HG00099,...,NA20819,NA20826,NA20828] total: 1092
    Loading labels from: /Users/szu004/dev/variant-spark/target/dist/variant-spark_2.11-0.2.0-SNAPSHOT/data/chr22-labels.csv, column: 22_16051249
    Loaded labels: [0,1,1,...,1,0,0] total: 1092
    Loading features from: /Users/szu004/dev/variant-spark/target/dist/variant-spark_2.11-0.2.0-SNAPSHOT/data/chr22_1000.vcf
    Loaded variables: [22_16050408,22_16050612,22_16050678,22_16050984,22_16051107,22_16051249,...] total: 1988, took: 2.058
    Assumed ordinal variable with 3 levels
    Data preview:
    22_16050408:[0,1,1,0,0,1,0,0,0,0,...,0,0,0,0,0,0,0,0,0] total: 1092
    ...
    22_16051249:[0,1,1,0,0,1,0,0,1,0,...,0,0,0,0,1,1,1,0,0] total: 1092
    22_16051347:[0,1,1,1,1,1,0,0,1,2,...,0,1,0,0,1,1,1,1,1] total: 1092
    Training random forest with trees: 500 (batch size:  20)
    Random seed is: -2211036639763447309
    RF Params: au.csiro.variantspark.algo.RandomForestParams@7a388990[oob=true,nTryFraction=0.022428065200812832,bootstrap=true,subsample=1.0,randomizeEquality=true,seed=-2211036639763447309]
    RF Params mTry: 44
    Finished trees: 20, current oobError: 0.01282051282051282, totalTime: 2.981 s, avg timePerTree: 0.14905 s
    Last build trees: 20, time: 2981 ms, timePerTree: 149 ms
    Finished trees: 40, current oobError: 0.016483516483516484, totalTime: 5.903 s, avg timePerTree: 0.147575 s
    ...
    Finished trees: 500, current oobError: 0.01282051282051282, totalTime: 41.817 s, avg timePerTree: 0.083634 s
    Last build trees: 20, time: 1305 ms, timePerTree: 65 ms
    Random forest oob accuracy: 0.01282051282051282, took: 41.959 s
    variable,importance
    22_16051453,0.0015948334818872735
    22_16053862,0.001523900023853203
    22_16051249,0.0015133325093640974
    22_16051347,2.8904502028141455E-4
    22_16051497,2.3341780231924257E-4
    22_16053791,1.7816058149774274E-4
    ...



Installing from PyPI
--------------------

TBP:


Where to Go from Here
----------------------

- If you’d like to build VariantSpark from source, visit :ref:`sec-development`.





