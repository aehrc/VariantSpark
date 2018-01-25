from __future__ import print_function
from setuptools import setup, find_packages
import sys
import os

if sys.version_info < (2, 7):
    print("Python versions prior to 2.7 are not supported.", file=sys.stderr)
    exit(-1)
 

#VERSION = __version__
# A temporary path so we can access above the Python project root and fetch scripts and jars we need
TEMP_PATH = "target"
ROOT_DIR = os.path.abspath("../")

# Provide guidance about how to use setup.py
incorrect_invocation_message = """
If you are installing variants from variant-spark source, you must first build variant-spark  and
run sdist.
    To build  with maven you can run:
      ./build/mvn -DskipTests clean package
    Building the source dist is done in the Python directory:
      cd python
      python setup.py sdist
      pip install dist/*.tar.gz"""

JARS_PATH = os.path.join(ROOT_DIR, "target")
JARS_TARGET = os.path.join(TEMP_PATH, "jars")

in_src = os.path.isfile("../pom.xml") 

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
        name='variants',
        version='0.1.0',
        description='VariantSpark Python API',
        packages=find_packages(exclude=["*.test"]) + ['variants.jars'], 
        install_requires=['typedecorator'],
#        test_suite = 'variants.test',
#        test_requires = [
#            'pyspark>=2.1.0'
#        ],
        include_package_data=True,
        package_dir={
            'variants.jars': 'target/jars',
        },
        package_data={
            'variants.jars': ['*-all.jar'],
        },
        entry_points='''
        [console_scripts]
        variants-jar=variants.cli:cli
        ''',
    )
finally:
    # We only cleanup the symlink farm if we were in Spark, otherwise we are installing rather than
    # packaging.
    if (in_src):
        # Depending on cleaning up the symlink farm or copied version
        os.remove(os.path.join(TEMP_PATH, "jars"))
        os.rmdir(TEMP_PATH)
