from __future__ import print_function
from setuptools import setup, find_packages
import sys
import os
import re
import time

if sys.version_info < (2, 7):
    print("Python versions prior to 2.7 are not supported.", file=sys.stderr)
    exit(-1)

HERE = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(HERE, os.pardir))
TEMP_PATH = "target"

in_src = os.path.isfile(os.path.join(ROOT_DIR, "pom.xml"))
        
VERSION = '0.2.0a0'    
    
# Provide guidance about how to use setup.py
incorrect_invocation_message = """
If you are installing varspark from variant-spark source, you must first build variant-spark  and
run sdist.
    To build  with maven you can run:
      ./build/mvn -DskipTests clean package
    Building the source dist is done in the Python directory:
      cd python
      python setup.py sdist
      pip install dist/*.tar.gz"""

JARS_PATH = os.path.join(ROOT_DIR, "target")
JARS_TARGET = os.path.join(TEMP_PATH, "jars")

if (in_src):
    # Construct links for setup
    try:
        os.mkdir(TEMP_PATH)
    except:
        print("Temp path for symlink to parent already exists {0}".format(TEMP_PATH),
              file=sys.stderr)
        exit(-1)


try:
        
    if (in_src):
        # Construct the symlink farm - this is necessary since we can't refer to the path above the
        # package root and we need to copy the jars and scripts which are up above the python root.
        os.symlink(JARS_PATH, JARS_TARGET)

    setup(
        name='variant-spark',
        version= VERSION,    
        packages=find_packages(exclude=["*.test"]) + ['varspark.jars'], 
        install_requires=['typedecorator'],
#        test_suite = 'varspark.test',
#        test_requires = [
#            'pyspark>=2.1.0'
#        ],
        extras_require = {
            'test': [ 
                'pyspark==2.1.2', 
            ],
        },
        include_package_data=True,
        package_dir={
            'varspark.jars': 'target/jars',
        },
        package_data={
            'varspark.jars': ['*-all.jar'],
        },
        entry_points='''
        [console_scripts]
        varspark-jar=varspark.cli:varspark_jar
        varspark-submit=varspark.cli:varspark_submit
        ''',
        # metadata for upload to PyPI
        description='VariantSpark Python API',
        long_description=("variant-spark is a scalable toolkit for "
                        "genome-wide association studies optimized for GWAS like datasets"),
        author='Piotr Szul et. al',
        author_email='piotr.szul@csiro.au',
        license='CSIRO Non-Commercial Source Code Licence Agreement v1.0',
        keywords='gwas vcf random forest association studies variant',
        classifiers=[
            'Development Status :: 2 - Pre-Alpha',
    
            'Intended Audience :: Science/Research',
            'Topic :: Scientific/Engineering :: Bio-Informatics',
    
            "Programming Language :: Python :: 2.7"
        ],
        url='https://bioinformatics.csiro.au/variantspark',   # project home page, if any
        project_urls={
            'Bug Tracker': 'https://github.com/aehrc/VariantSpark/issues',
            'Documentation': 'http://variantspark.readthedocs.io/en/latest',
            'Source Code': 'https://code.example.com/HelloWorld/',
        },
    )
finally:
    # We only cleanup the symlink farm if we were in Spark, otherwise we are installing rather than
    # packaging.
    if (in_src):
        # Depending on cleaning up the symlink farm or copied version
        os.remove(os.path.join(TEMP_PATH, "jars"))
        os.rmdir(TEMP_PATH)
