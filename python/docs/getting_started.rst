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
- ``SPARK_HOME`` pointing to your Spark installation


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



Installing from PyPI
--------------------

TBP:


Where to Go from Here
----------------------

- If you’d like to build VariantSpark from source, visit :ref:`sec-development`.





