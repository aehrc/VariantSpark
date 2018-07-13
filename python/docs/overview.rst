.. _sec-overview:

=======================================
Overview
=======================================


TBP: Info on random forest, importance and how it's applicable in GWAS.
Comand line tool and various apis.

Importance analysis
-------------------

``TBP``


``Some content that may go here:``


Prepare VCF file for VariantSpark importance analysis:
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ 
 
VariantSpark encodes 0/0, 0/X and X/Y genotypes into 0, 1 and 2 respectively where X and Y refer to any of alternate allele in the ALT field (>=1). 
This could be a problem when dealing with multiallelic variants. You may decide to breakdown multi-allelic sites into multiple bi-allelic sites 
prior to the importance analysis (both BCFtools and Hail provide this functionality). This allows you to correctly consider the effect of each allele (on the same site) separately.
In case you need to keep all allele in the same variable and at the same time distinguish between different allele, you can manually 
transform the VCF file in a CSV file and use your own encoding. Note that having more distinct values for a variable will slow down the RandomForest training process.
Also, note that VariantSpark does not consider phasing in the VCF file. Thus both 0|1 and 1|0 are encoded to 1 assumed to be the same.

Another consideration is about joining VCF file together (i.e. join the case and control VCF file). Hail join function ignores multi-allelic 
sites with different allele lists. BCFtools join multi-allelic sites but cause errors. 
We strongly recommend to breakdown multi-alleic site before joining VCF file and if needed merge them to the same site after joining (both BCFtools and Hail provide this functionality).
 
Some notes for using VariantSpark importance analysis:
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ 
 
 
The value of mtry significantly affects the RandomForest training process. The default value of mtry is the square root of the number of variables.
Although it is a good value, in general, it does not appropriate for all sorts of data. You may consider varying the value of mtry and re-run the analysis
to find the optimal value that suits your dataset. Note that with lower mtry processing each node of a tree would be faster but trees will get deeper. 
Thus, the relation between mtry and execution time is not linear.



There are some papers about how to estimate number of trees.
 
The most applicable approach is to run with 10000 and 20000 trees (with default mtry) if there were significant changes in the top 100 or top 1000 important variable the model is not stable and they should try with 40000 and so on.
 
I recommend to run with mtry = 0.1 * num_variants (with 10000 trees) as this may change the top hits.
 
I also recommend to run with low mtry (i.e. 100) and as many trees as possible (500000) as this may change the top hits too (with low mtry tree building is much faster)

