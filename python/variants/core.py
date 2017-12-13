import sys

from pyspark.sql import SQLContext


class VariantsContext(object):
    """The main entry point for VariantSpark functionality.

    :param ss: SparkSession
    :type sc: :class:`.pyspark.SparkSession`

    """
    def __init__(self, ss=None):
        self.sc = ss.sparkContext
        self.sql = SQLContext.getOrCreate(self.sc)
        self._jsql = self.sql._jsqlContext
        self._jvm = self.sc._jvm
        self._vs_api = getattr(self._jvm, 'au.csiro.variantspark.api')
        jss = ss._jsparkSession
        self._jvsc = self._vs_api.VSContext.apply(jss)

        sys.stderr.write('Running on Apache Spark version {}\n'.format(self.sc.version))
        if self.sc._jsc.sc().uiWebUrl().isDefined():
            sys.stderr.write('SparkUI available at {}\n'.format(self.sc._jsc.sc().uiWebUrl().get()))
        sys.stderr.write(
            'Welcome to\n'            
' _    __           _             __  _____                  __    \n'
'| |  / /___ ______(_)___ _____  / /_/ ___/____  ____ ______/ /__  \n'
'| | / / __ `/ ___/ / __ `/ __ \/ __/\__ \/ __ \/ __ `/ ___/ //_/  \n'
'| |/ / /_/ / /  / / /_/ / / / / /_ ___/ / /_/ / /_/ / /  / ,<     \n'
'|___/\__,_/_/  /_/\__,_/_/ /_/\__//____/ .___/\__,_/_/  /_/|_|    \n'
'                                      /_/                         \n')


    def import_vcf(self, vcf_file_path):
        """ Shut down the VariantsContext.
        """
        return VCFFeatureSource(self._jvm, self._vs_api, self._jsql, self._jvsc.importVCF(vcf_file_path))

    def load_label(self, label_file_path, col_name):
        """ Loads the label source file

        :param label_file_path: The file path for the label source file
        :param the name of the column containing labels
        """
        return self._jvsc.loadLabel(label_file_path, col_name)

    def stop(self):
        """ Shut down the VariantsContext.
        """

        self.sc.stop()
        self.sc = None

class VCFFeatureSource(object):

    def __init__(self, _jvm, _vs_api, _jsql, _jvcffs):
        self._jvcffs = _jvcffs
        self._jvm = _jvm
        self._vs_api = _vs_api
        self._jsql = _jsql

    def importance_analysis(self, labelSource, n_trees=1000, mtry_fraction=None, oob=True, seed=None,
                            batch_size=100, varOrdinalLevels=3):
        """Builds random forest classifier.

        :param labelSource: The ingested label source
        :param int n_trees: The number of trees to build in the forest.
        :param float mtry_fraction: The fraction of variables to try at each split.
        :param bool oob: Should OOB error be calculated.
        :param long seed: Random seed to use.
        :param int batch_size: The number of trees to build in one batch.
        :param int var ordinal level

        :return: Importance analysis model.
        :rtype: :py:class:`ImportanceAnalysis`
        """
        rfParams = self._jvm.au.csiro.variantspark.algo.RandomForestParams(bool(oob), float(mtry_fraction), True, float('nan'), True, long(seed))
        ia = self._vs_api.ImportanceAnalysis(self._jsql ,self._jvcffs, labelSource, rfParams, n_trees, batch_size, varOrdinalLevels)
        return ImportanceAnalysis(ia)

class ImportanceAnalysis(object):
    """ Model for random forest based importance analysis
    """
    def __init__(self, _jia):
        self._jia = _jia

    def important_variables(self, limit=10):
        """ build index for names
        """

        return self._jia.importantVariables(limit)
