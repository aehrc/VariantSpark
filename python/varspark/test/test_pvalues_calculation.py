import os
import unittest

import hail as hl
import pytest

import varspark.hail as vshl
from varspark.test import PROJECT_DIR


@pytest.mark.pvalues
class PValuesCalculationTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        """
        This constructor creates the initial dataframe with the variance importances to assess
        the different functions of the p-value calculation.
        """

        vshl.init()

        vds = hl.import_vcf(os.path.join(PROJECT_DIR, 'data/chr22_1000.vcf'))
        labels = hl.import_table(os.path.join(PROJECT_DIR, 'data/chr22-labels-hail.csv'),
                                 impute=True, delimiter=",").key_by('sample')

        vds = vds.annotate_cols(label=labels[vds.s])
        rf_model = vshl.random_forest_model(y=vds.label['x22_16050408'], x=vds.GT.n_alt_alleles(),
                                            seed=13, mtry_fraction=0.05, min_node_size=5,
                                            max_depth=10)
        rf_model.fit_trees(100, 50)

        self.lfdrvs = rf_model.get_lfdr()

    def test_number_of_significant_variants(self):
        """
        Assess weather the p-values calculation returns the same number of significant variants
        as the original R script.
        :return:
        """
        df, fdr = self.lfdrvs.compute_fdr(countThreshold=2, local_fdr_cutoff=0.05)

        self.assertEqual(df.is_significant.sum(), 17)


if __name__ == '__main__':
    unittest.main()
