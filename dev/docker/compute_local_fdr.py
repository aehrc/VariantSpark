import hail as hl
import varspark.hail as vshl
from matplotlib import pyplot as plt
vshl.init()

vds = hl.import_vcf('./data/chr22_1000.vcf')
labels = hl.import_table('./data/chr22-labels-hail.csv', impute = True, delimiter=",").key_by('sample')

vds = vds.annotate_cols(label = labels[vds.s])
vds.cols().show(3)

rf_model = vshl.random_forest_model(y=vds.label['x22_16050408'],
                x=vds.GT.n_alt_alleles(), seed = 13, mtry_fraction = 0.05, min_node_size = 5, max_depth = 10)
rf_model.fit_trees(300, 50)

print("OOB error: %s" % rf_model.oob_error())
impTable = rf_model.variable_importance()
impTable.order_by(hl.desc(impTable.importance)).show(10)

fdrCalc = rf_model.get_lfdr()

fig, ax1 = plt.subplots(figsize=(10, 5), layout='constrained')
fdrCalc.plot_log_densities(ax1, cutoff_list=[1, 2, 3, 4, 5, 10, 15, 20], find_automatic_best=True)
plt.show()

fig, ax2 = plt.subplots(figsize=(10, 5), layout='constrained')
fdrCalc.plot_log_hist(ax2, split_count=2)
plt.show()

pvalsDF, fdr = fdrCalc.compute_fdr(countThreshold = 2, local_fdr_cutoff = 0.05)
pvalsDF, fdr

fig, ax3 = plt.subplots(figsize=(10, 5), layout='constrained')
fdrCalc.plot(ax3)
plt.show()

hl.stop()
