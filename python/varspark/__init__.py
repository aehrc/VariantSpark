import os

try:
    from varspark.core import VarsparkContext, VariantsContext, configure_spark
    from varspark.rfmodel import (
        RandomForestModel,
        RandomForestClassifier,
        RandomForestRegressor,
        RFModelContext,
    )
except Exception:
    if not os.environ.get("VS_FIND_JAR"):
        raise

from varspark.etc import find_jar
from varspark.variable_type import (
    VariableType,
    Continuous,
    Nominal,
    Ordinal,
)
