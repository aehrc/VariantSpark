import pkg_resources
import hail as hl
import varspark as vs
import os
import sys
from pyspark import SparkConf, SparkContext

def init(**kwargs):
  
    jars = []
    vs_jar_path=vs.find_jar()
    assert os.path.exists(vs_jar_path), f'{vs} does not exist'
    sys.stderr.write(f'using variant-spark jar at {vs_jar_path}\n')
    jars.append(vs_jar_path)
 
    if pkg_resources.resource_exists(hl.__name__, "hail-all-spark.jar"):
        hail_jar_path = pkg_resources.resource_filename(hl.__name__, "hail-all-spark.jar")
        assert os.path.exists(hail_jar_path), f'{hail_jar_path} does not exist'
        sys.stderr.write(f'using hail jar at {hail_jar_path}\n')
        jars.append(hail_jar_path)
        
    conf = SparkConf()
    conf.set('spark.jars', ",".join(jars))
    conf.set('spark.driver.extraClassPath', hail_jar_path)
    conf.set('spark.executor.extraClassPath', './hail-all-spark.jar')
    SparkContext._ensure_initialized(conf=conf)
                
    hl.init(**kwargs)