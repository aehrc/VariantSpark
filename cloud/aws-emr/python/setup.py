from setuptools import setup

setup(
    name='vs_emr',
    version='0.2',
    packages=['vs_emr'],
    install_requires=[
        'Click', 'PyYAML', 'pystache', 'jsonmerge', 'awscli>=1.11'
    ],
    include_package_data=True,
    entry_points='''
        [console_scripts]
        vs-emr=vs_emr.cli:cli
    ''',
)
