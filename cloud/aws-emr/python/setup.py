from setuptools import setup

setup(
    name='vs_emr',
    version='0.1',
    py_modules=['vs_emr'],
    install_requires=[
        'Click',
    ],
    package_data = {'':['*.yaml']},
    entry_points='''
        [console_scripts]
        vs-emr=vs_emr:cli
    ''',
)