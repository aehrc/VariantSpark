
import os
from io import StringIO
import unittest
import pytest

import hail as hl
import pandas as pd

import varspark.hail as vshl
from pyspark.sql.functions import desc
from varspark.test import PROJECT_DIR

@pytest.mark.covariates
class RFModelHailTest(unittest.TestCase):

    def test_covariates(self):
        """
        Checks that the top 5 features are the same as the expected ones.
        :return:
        """
        vshl.init()

        data = hl.import_vcf(os.path.join(PROJECT_DIR, 'data/chr22_1000.vcf'))
        labels = hl.import_table(os.path.join(PROJECT_DIR, 'data/chr22-labels.csv'),
                                 delimiter=',',
                                 types={'22_16050408': 'float64'}).key_by('sample')

        #loading the covariates and transposing them
        covariates = pd.read_csv(os.path.join(PROJECT_DIR, 'data/chr22_1000_pheno-wide.csv')).T.reset_index()
        covariates.at[0,'index'] = 'samples'
        covariates.columns = covariates.iloc[0]
        covariates = covariates.iloc[1: , :]
        covariates = hl.Table.from_pandas(covariates).key_by('samples')
        mt = data.annotate_cols(covariates=covariates[data.s], labels=labels.select('22_16050408')[data.s])

        with vshl.random_forest_model(y=mt.cov.label, x=mt.GT.n_alt_alleles(),
                                      covariates=mt.covariates, #TODO: Age and BMI
                                      seed=13, mtry_fraction=0.05,
                                      min_node_size=5, max_depth=10) as rf_model:
            rf_model.fit_trees(100, 50)

            impTable = rf_model.variable_importance()
            predicted_df = impTable.to_pandas()

        predicted_df['variable'] =  predicted_df['locus.contig'].astype(str) + '_' + \
                                    predicted_df['locus.position'].astype(str) + '_' + \
                                    predicted_df['alleles'].apply(lambda x: x[0]) + '_' + \
                                    predicted_df['alleles'].apply(lambda x: x[1])

        expected_df = pd.read_csv(StringIO("""variable,importance
        22_16050408_T_C,18.693030423906
        age,17.367359847009364
        22_16051480_T_C,15.313500916790746
        22_16053435_G_T,14.716169509232733
        22_16051882_C_T,13.895489401155135
        22_16050678_C_T,13.791699595501232
        22_16051107_C_A,12.706363192083234
        22_16053197_G_T,10.81675799199181
        22_16052656_T_C,10.170068805911956
        22_16052838_T_A,9.570174715544987
        22_16053727_T_G,8.941658479000619
        22_16053509_A_G,8.350809808267378
        22_16050612_C_G,7.231670806837649
        22_16054283_C_T,6.10815025638534
        22_16053797_T_C,6.052694195793001
        bmi,5.4745415656421335
        22_16053881_A_C,2.218640340875331
        22_16052250_A_G,1.8718504692508477
        22_16053001_A_T,0.7804287757204303
        22_16052080_G_A,0.5453104311825786"""))

        top_five_ranking = True
        for a, b in zip(predicted_df['variable'].head(5).tolist(), expected_df['variable'].head(5).tolist()):
            top_five_ranking = top_five_ranking and a == b

        hl.stop()

        # Asserting
        self.assertTrue(top_five_ranking)


if __name__ == '__main__':
    unittest.main(verbosity=2)