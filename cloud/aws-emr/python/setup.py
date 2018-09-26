from setuptools import setup

setup(
    name='vs-emr',
    version='0.2.0a2',
    packages=['vs_emr'],
    install_requires=[
        'Click', 'PyYAML', 'pystache', 'jsonmerge', 'awscli>=1.11'
    ],
    include_package_data=True,
    entry_points='''
        [console_scripts]
        vs-emr=vs_emr.cli:cli
    ''',
        # metadata for upload to PyPI
    description='VariantSpark AWS EMR Tool',
    long_description=("A command line utility to facilitate running of VariantSpark on AWS EMR "),
    author='Piotr Szul et. al',
    author_email='piotr.szul@csiro.au',
    license='CSIRO Non-Commercial Source Code Licence Agreement v1.0',
    keywords='gwas vcf random-forest association-studies variant-spark',
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
