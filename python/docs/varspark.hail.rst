package varspark.hail
=====================

This package contains variant spark integration with Hail.

::
    
    from hail import *
    import varspark.hail
    hc = HailContext(sc)    
    vds = hc.import_vcf(...)
    ...
    via = vds.importance_analysis("sa.pheno.label", n_trees = 1000)



module varspark.hail.extend
---------------------------

.. automodule:: varspark.hail.extend
    :members:
    :undoc-members:

module varspark.hail.rf
-----------------------

.. automodule:: varspark.hail.rf
    :members:
    :undoc-members:
