P-Value/FDR calculation utilities
---------------------------------


Requires:
- python 3.7+
- Click 7.0
- pandas 1.3+

To extract split variable counts from json model use:

    ./model2counts.py -i <input-json-model-file> -o <output-tsv-count-file>
    
E.g:
    
    ./model2counts.py -i  data/chr22_22_16050408-model.json -o /tmp/counts.tsv

