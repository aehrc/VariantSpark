import unittest
from unidip import UniDip

from python.varspark.pvalues_calculation import *
import hail as hl
import python.varspark.hail as vshl
vshl.init()

class PValuesCalculationTest(unittest.TestCase):
    def test_number_of_significant_variants(self):
        vds = hl.import_vcf('../data/chr22_1000.vcf')
        labels = hl.import_table('../data/chr22-labels-hail.csv', impute = True, delimiter=",").key_by('sample')

        vds = vds.annotate_cols(label = labels[vds.s])
        rf_model = vshl.random_forest_model(y=vds.label['x22_16050408'], x=vds.GT.n_alt_alleles(),
                                            seed = 13, mtry_fraction = 0.05, min_node_size = 5,
                                            max_depth = 10)
        rf_model.fit_trees(100, 50)
        impTable = rf_model.variable_importance()
        df = impTable.order_by(hl.desc(impTable.importance)).to_spark(flatten=False).toPandas()
        df['log_importance'] = df.importance.apply(np.log)

        for SplitCountThreshold in range(1,8):
            dat = np.msort(df[df['splitCount']>SplitCountThreshold]['log_importance'])
            intervals = UniDip(dat, ntrials=1000).run()
            if len(intervals) <= 1:
                break

        df = df[df.importance > 0]
        df['composed_index'] = df.apply(lambda row: str(row['locus'][0]) + '_'
                                                    + str(row['locus'][1]) + '_'
                                                    + str('_'.join(row['alleles'])), axis=1)
        df = df[['composed_index','log_importance']].set_index('composed_index').squeeze()

        temp = run_it_importances(df)

        #The 14 is computed using the R script using same data
        self.assertEqual(len(temp['ppp']), 14)


if __name__ == '__main__':
    unittest.main()
