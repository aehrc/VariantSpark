.. _sec-cmd_ref:

=======================================
Command Line Reference
=======================================

**variant-spark** requires an existing spark 2.4+ installation (either a local one or a cluster one).

To run variant-spark use:
::

    ./variant-spark [(--spark|--local) <spark-options>* --] [<command>] <command-options>*

In order to obtain the list of the available commands use:
::

    ./variant-spark -h
    
In order to obtain help for a specific command (for example `importance`) use:
::

    ./variant-spark importance -h

You can use ``--spark`` marker before the command to pass `spark-submit` options to variant-spark. The list of spark options needs to be terminated with ``--``, e.g:

    ./variant-spark --spark --master yarn-client --num-executors 32 -- importance .... 
    
Please, note that ``--spark`` needs to be the first argument of **variant-spark**

You can also run variant-spark in the ``--local`` mode. In this mode variant-spark will ignore any Hadoop or Spark configuration files and run in the local mode for both Hadoop and Spark.
In particular in this mode all file paths are interpreted as local file system paths. Also any parameters passed after `--local` and before `--` are ignored. 
For example:
::

    ./variant-spark --local -- importance  -if data/chr22_1000.vcf -ff data/chr22-labels.csv -fc 22_16051249 -v -rn 500 -rbs 20 -ro

Note: 

The difference between running in ``--local`` mode and in ``--spark`` with ``local`` master is that in the latter case Spark uses the hadoop filesystem configuration and the input files need to be copied to this filesystem (e.g. HDFS) 
Also the output will be written to the location determined by the hadoop filesystem settings. In particular paths without schema e.g. 'output.csv' will be resolved with the hadoop default filesystem (usually HDFS)
To change this behavior you can set the default filesystem in the command line using `spark.hadoop.fs.default.name` option. For example to use local filesystem as the default use:
::

    variant-spark --spark ... --conf "spark.hadoop.fs.default.name=file:///" ... -- importance  ... -of output.csv

You can also use the full URI with the schema to address any filesystem for both input and output files e.g.:
::

    variant-spark --spark ... --conf "spark.hadoop.fs.default.name=file:///" ... -- importance  -if hdfs:///user/data/input.csv ... -of output.csv



