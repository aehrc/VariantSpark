from __future__ import print_function
from setuptools import setup, find_packages
import sys
import os
import glob

if sys.version_info < (2, 7):
    print("Python versions prior to 2.7 are not supported.", file=sys.stderr)
    exit(-1)

HERE = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(HERE, os.pardir))
TEMP_PATH = "target"

in_src = os.path.isfile(os.path.join(ROOT_DIR, "pom.xml"))
        
VERSION = '0.2.0b1'
    
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



def src_path(src_dir):
    return os.path.join(ROOT_DIR, src_dir)

def dst_path(dst_dir):
    return os.path.join(TEMP_PATH, dst_dir)

EXT_DIRS = [
    ('target', 'jars'),
    ('bin', 'bin'),
    ('examples','examples'),    
    ('data','data')    
]

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
        for (src_dir,dst_dir) in EXT_DIRS:
            os.symlink(src_path(src_dir), dst_path(dst_dir))

    setup(
        name='variant-spark',
        version= VERSION,    
        packages=find_packages(exclude=["*.test"]) + ['varspark.jars'], 
        install_requires=[
            'typedecorator'
        ],
#        test_suite = 'varspark.test',
#        test_requires = [
#            'pyspark>=2.1.0'
#        ],
        extras_require = {                          
            'deps': [ 
                'pandas>=0.22.0', 
            ],
            'spark': [ 
                'pyspark==2.2.1', 
            ],
            'test': [ 
                'pyspark==2.2.1', 
            ],
            'hail': [
                'decorator==4.1.2',
                'hail==devel',
            ]
        },
        dependency_links = [
            'https://s3-ap-southeast-2.amazonaws.com/variant-spark/deps/hail-0.1/latest/hail-devel.tar.gz#egg=hail-devel'
        ],
        include_package_data=True,
        package_dir={
            'varspark.jars': 'target/jars',
        },
        package_data={
            'varspark.jars': ['*-all.jar'],
        },
        data_files = [
            ('share/variant-spark/examples', glob.glob(os.path.join(dst_path('examples'),'*'))),
            ('share/variant-spark/data', [ p for p in glob.glob(os.path.join(dst_path('data'),'*')) if not os.path.isdir(p)]),
        ],
        scripts = [
            'target/bin/variant-spark',                   
            'target/bin/jvariant-spark',
            'target/bin/find-varspark-jar',
        ],
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
            'Source Code': 'https://github.com/aehrc/VariantSpark',
        },
    )
finally:
    # We only cleanup the symlink farm if we were in Spark, otherwise we are installing rather than
    # packaging.
    if (in_src):
        # Depending on cleaning up the symlink farm or copied version
        for (src_dir,dst_dir) in EXT_DIRS:
            os.remove(dst_path(dst_dir))
        os.rmdir(TEMP_PATH)
