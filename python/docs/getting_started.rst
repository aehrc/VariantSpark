.. _sec-getting_started:

=======================================
Getting Started
=======================================

**VariantSpark** is currently supported only on Unix-like system (Linux, MacOS). 

You’ll need:

- The `Java 8 JDK <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`_.
- `Apache Spark 2.4 <http://spark.apache.org/downloads.html>`_. **VariantSpark** is compatible with Spark 2.4+
- Python 3.6+ and Jupyter Notebooks. We recommend the free `Anaconda distribution <https://www.continuum.io/downloads>`_.

It’s easy to run locally on one machine - you need to have Apache Spark installed correctly  with:

- ``spark-submit`` and ``pyspark`` on you system ``PATH``


Installing from a distribution
------------------------------

Get **VariantSpark** distribution from the downloads page of project web site. 
Un-tar the distribution after you download it (you may need to change the name of the file to match the current version). 
::

    tar -xzf variant-spark_2.11-0.3.0.tar.gz


Next, edit and copy the below bash commands to set up the **VariantSpark** environment variables. 
You may want to add these to the appropriate dot-file (we recommend ``~/.profile``) 
so that you don’t need to rerun these commands in each new session.

Here, fill in the path to the un-tared **VariantSpark** distribution.
::

    export VARSPARK_HOME=???
    export PATH=$PATH:$VARSPARK_HOME/bin

Now you should be all setup to run **VariantSpark** command line tool. 
::

    variant-spark -h

The `-h` option displays the help on available commands. To find out more about the command line tool please visit :ref:`sec-cmd_ref`.

**VariantSpark** comes with several sample programs and datasets. Command-line and Python Jupyter examples are ``examples`` directory.
There is a few small data sets in the ``data`` directory suitable for running on a single machine. 

Installing from PyPI
--------------------

It's recommended that Python users install **VariantSpark** from PyPI with: 
::

     pip install variant-spark  

This assumes that a compatible version of Apache Spark is already installed in your Python environment. If not,
you can install it from the distribution using the information from the beginning of this section, 
or with:
::

    pip install variant-spark[spark]


The code and data samples are installed into the ``<PYTHON_PREFIX>/share/variant-spark`` directory, 
where ``<PYTHON_PREFIX>`` is he value of Python's ``sys.prefix``.

You can find what your ``<PYTHON_PREFIX>`` is on your system by typing:
::

    python -c $'import sys\nprint(sys.prefix)'

It's recommend that you make a copy the examples in your home/working directory before using them.

Running examples 
----------------

The rest of this section assumes that you are in the ``examples`` directory.

One of the main applications of **VariantSpark** is discovery of genomic variants correlated with a response 
variable (e.g. case vs control) using random forest gini importance.

The ``chr22_1000.vcf`` is a very small sample of the chromosome 22 VCF file
from the `1000 Genomes Project <http://www.internationalgenome.org/>`_.

``chr22-labels.csv`` is a CSV file with sample response variables (labels). 
In fact the labels directly represent the number of alternative alleles for each sample at a specific genomic position. 
E.g.: column ``22_16050408`` has labels derived from variants in chromosome 22 position 16050408.
We would expect then that position  *22:16050408* in the VCF file is strongly correlated with the label ``22_16050408``.


Running importance analysis with command line tool
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We can run importance analysis on these data with the following command:
::

    variant-spark importance -if ../data/chr22_1000.vcf -ff ../data/chr22-labels.csv -fc 22_16050408 -v -rn 500 -rbs 20 -ro -sr 13

alternatively you can just the sample script:
::

    ./local_run-importance-ch22.sh 

This will build a random forest with 500 trees and report OOB (out of bag) error as well as the top 20 important variables. 
The final output should be similar to this:
::

    Random forest oob accuracy: 0.015567765567765568, took: 28.249 s
    variable,importance
    22_16050408,8.634503639847714E-4
    22_16051107,7.843083422549387E-4
    ...

As expected variable ``22_16050408`` representing the variant at position *22:16050408* comes out as the most important. 

To find out about the options for the *importance* command type ``variant-spark importance -h`` or visit :ref:`sec-cmd_ref`.


Running importance analysis with PythonAPI
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The same analysis can be also performed using **VariantSpark** PythonAP with a Jupyter notebook.

You can start the *VariantSpark* enabled Jupyter notebook server with:
::

    jvariant-spark
    
Open an run the ``run_importance_chr22.ipynb`` for the PythonAPI example.
You can check the expected results :ref:`here </examples/run_importance_chr22.ipynb>`.

To find out about more about the Python API visit :ref:`sec-pyapi`.


Where to Go from Here
----------------------

- If you’d like to build VariantSpark from source, visit :ref:`sec-development`.
