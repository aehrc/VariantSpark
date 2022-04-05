# Threshold Values for the Gini Variable Importance

Random Forests are machine learning methods commonly used to model data. They are highly scalable 
and robust to overfitting while modelling non-linearities. Using an empirical bayes approach, we 
were able to quantify the importance of the variants in our models; thus, improving the 
interpretability of such algorithms.

## Requirements

python==3.8.12\
numpy==1.21.2 \
pandas==1.4.1 \
patsy==0.5.2 \
scipy==1.7.3\
statsmodels==0.13.2

## Usage

As an input it is expected a Pandas series data frame where the column is the logarithm of
the importances. The method returns a dictionary with the FDR values as array, the estimates for
the fitted function (array length three), and the p-values for the statistically significant
variants (array).

The code can be used stand-alone requiring only the script file. If that is your wish the file 
can be found at: 
https://github.com/aehrc/VariantSpark/blob/918c80be28818b8872ce346cbb2092da5c4d2ced/python/varspark/pvalues_calculation.py

A hands-on jupyter notebook with a step by step implementation to go from importances to p-values can be found at: 
https://github.com/aehrc/VariantSpark/blob/918c80be28818b8872ce346cbb2092da5c4d2ced/examples/computing_p-value_example.ipynb

However, this method is also integrated with VariantSpark. This enables the p-value calculation 
with a single function call. Training a model, calculating the p-values, and getting them can be done 
in few lines of code using VariantSpark as shown in the following snippet: 


        vds = hl.import_vcf(os.path.join(PROJECT_DIR, 'data/chr22_1000.vcf'))
        labels = hl.import_table(os.path.join(PROJECT_DIR, 'data/chr22-labels-hail.csv'),
                                 impute=True, delimiter=",").key_by('sample')

        vds = vds.annotate_cols(label=labels[vds.s])
        rf_model = vshl.random_forest_model(y=vds.label['x22_16050408'], x=vds.GT.n_alt_alleles(),
                                            seed=13, mtry_fraction=0.05, min_node_size=5,
                                            max_depth=10)
        rf_model.fit_trees(100, 50)

        significant_variants = rf_model.get_significant_variances()

Notes: If you wish to use the VariantSpark implementation please consider reading more about the 
tool [here](https://github.com/aehrc/VariantSpark/blob/master/README.md) and [here](https://github.com/aehrc/VariantSpark/blob/master/python/README.md).

## Citation

If you use this method please consider citing us:

    Dunne, R. ... (2022). Threshold Values for the Gini Variable Importance: A Empirical Bayes 
    Approach. arXiv preprint arXiv:XXXX.XXXXX.

