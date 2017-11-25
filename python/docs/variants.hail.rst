package variants.hail
=====================

This package contains variant spark integration with Hail.

::
    
    from hail import *
    import variants.hail
    hc = HailContext(sc)    
    vds = hc.import_vcf(...)
    ...
    via = vds.importance_analysis("sa.pheno.label", n_trees = 1000)



module variants.hail.extend
---------------------------

.. automodule:: variants.hail.extend
    :members:
    :undoc-members:

module variants.hail.rf
-----------------------

.. automodule:: variants.hail.rf
    :members:
    :undoc-members: