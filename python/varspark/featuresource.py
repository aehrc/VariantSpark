import struct
import pandas as pd
from typedecorator import params, Nullable


class FeatureSource(object):
    def __init__(self, _jvm, _vs_api, _jsql, sql, _jfs):
        self._jfs = _jfs
        self._jvm = _jvm
        self._vs_api = _vs_api
        self._jsql = _jsql
        self.sql = sql

    @params(sample_list_str=str)
    def extract_samples(sample_list_str):
        """Convert the sample list string to a list of sample names.

        :param (str) sample_list_str: The string representation of the sample list.

        :return List[str]: A list of sample names.
        """
        cleaned_str = sample_list_str.replace("List(", "").replace(")", "")
        return [s.strip().strip('"') for s in cleaned_str.split(",")]

    @params(feature=object)
    def unpack_feature_data(feature):
        """Unpack feature data from byte array into a tuple of integers.

        :param feature: The feature object containing the data.

        return tuple: A tuple containing unpacked integers.
        """
        byte_string = feature.data().valueAsByteArray()
        format_string = f"{len(byte_string)}B"
        return struct.unpack(format_string, byte_string)

    @params(features_ref=object)
    def collect_feature_data(features_ref):
        """Collect and organize feature data into a dictionary.

        :param features_ref: The list of feature objects.

        :return dict: A dictionary with feature labels as keys and unpacked data as values.
        """
        return {
            feature.label(): FeatureSource.unpack_feature_data(feature)
            for feature in features_ref
        }

    @params(self=object, scala=Nullable(bool))
    def to_df(self, scala=False):
        """Converts a Feature Source RDD to a pandas dataframe.

        :param (bool) scala: Indicates whether to use the scala version of DataFrame conversion

        :return features (DataFrame): dataframe with values for respective samples (rows)
        """
        if scala:
            jdf = self._jfs.toDF(self._jsql)
            jdf.count()
            jdf.createOrReplaceTempView("df")
            features = self.sql.table("df").toPandas()
            features.set_index("variant_id", inplace=True)
            return features
        else:
            features_ref = self._jfs.features().collect()
            samples = FeatureSource.extract_samples(str(self._jfs.sampleNames()))
            feature_data = FeatureSource.collect_feature_data(features_ref)
            return pd.DataFrame(feature_data, index=samples)
