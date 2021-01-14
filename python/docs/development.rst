.. _sec-development:

=======================================
Development
=======================================

**variant-spark** requires:

- java jdk 1.8+ 
- maven 3+

In order to build the binaries use:

::

    mvn clean install


For python **variant-spark** requires python 3.6+ with pip.

The other packages required for development are listed in dev/dev-requirements.txt and can be installed with:

::

    pip install -r dev/dev-requirements.txt

or with:

::
 
    ./dev/py-setup.sh

The complete built including all check can be run with:

::

    ./dev/build.sh


For info on how to contribute see: https://github.com/aehrc/VariantSpark/blob/master/CONTRIBUTING.md


