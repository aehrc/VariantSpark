package varspark.hail
=====================

This package contains variant spark integration with Hail.

::
    
    import hail as hl
    import varspark.hail as vshl
    vshl.init()
    vds = hl.import_vcf(...)
    ...

    rf_model = vshl.random_forest_model(y=vds.label,
                x=vds.GT.n_alt_alleles(), seed = 13, mtry_fraction = 0.05,
                min_node_size = 5, max_depth = 10)
    rf_model.fit_trees(100, 50)
    impTable = rf_model.variable_importance()
    rf_model.release()


.. automodule:: varspark.hail
    :members:
    :undoc-members:

module varspark.hail.rf
-----------------------

.. automodule:: varspark.hail.rf
    :members:
    :undoc-members:

module varspark.hail.plot
-------------------------

.. automodule:: varspark.hail.plot
    :members:
    :undoc-members:
