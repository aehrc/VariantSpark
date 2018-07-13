import os
try:
    from varspark.core import VarsparkContext, VariantsContext
except Exception:
    if not os.environ.get('VS_FIND_JAR'):
        raise

from varspark.etc import find_jar
