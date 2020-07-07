# JSON RandomForest Model Analyser

[JsonRfAnalyser.py](JsonRfAnalyser.py) provides a set of functions to look into the RandomForest model (in JSON format) produced by VariantSpark importance command. It can perform six different operations:

- **forest**: List all vaiables (Variants) used in the nodes of forest (duplicates are removed) in a single line in the output, seprated by comma.
- **tree**: List variables used in each tree in a line of output file (duplicates are removed).
- **branch**: List variables used in each branch (from root to leaf) in a line of output file (duplicates are **NOT** removed). The first column is the index of the tree where the branch taken form.
- **limit**: Similar to **branch** but merge branches from the bottom to top so that the total number of node (not necessary unique variables used to split nodes) is above threshold.
- **comb/combx**: Process the output of the above four commands. Compute total number of **n**-Variable combinations (tests to be performed) when performing exhustive **n**-Variable epistasis analysis on variables of each line of the input file. **n**=2 count number of combination in exustive pairwise epistasis search. Combx excludes the first column of the input file and should be used to process output of branch and limit command where the first column is the tree id. the last argument is **n**.
- roots: List variables used to split root node of the trees.

usecase: 

```sh
$ python JsonRfAnalyser.py [forest|tree|branch|limit|roots] JSON_MODEL_FILE.json output.csv Threshold
$ python JsonRfAnalyser.py [comb|combx] input.csv n
```

[runTest.sh](runTest.sh) show case the use of [JsonRfAnalyser.py](JsonRfAnalyser.py) for processing an example RandomForest model ([RF.json.gz](RF.json.gz)). The output log of this script is stored in [runTest.output.log](runTest.output.log)

[RF.json.gz](RF.json.gz) is an example RandomForest model produced by VariantSpark. This model is trained on a subset of 1000 genomes dataset and a synthetic dataset (the **Hipster** column in [Hipster2.csv](Hipster2.csv)). You should decompress this file before using it see ([runTest.sh](runTest.sh)).

The following command shows how many times each variable is selected in a root node where **v_roots.csv** is the output of **roots** command.
```sh
sort v_roots.csv | uniq -c | awk '{print($1"\t"$2)}' | sort -g
```

The following command list all upstream variables for a given variable. Upstream variables are variables which are appears on top of the given variable in the branch. **v_branch.csv** is the output of the **branch** command. The second column of the output is the tree id where the branch belongs too. The first column is the number of upstream variable in the branch. The last column of each line is the given variable. The screenshot below shows the output highlighted with colors.

```sh
VARI="7_17284577_T_C"
grep $VARI  v_branch.csv | tr , \\t | awk -v VARI="$VARI" '{n=0; b=""; for(i=1; i<=NF; i++) {if($i==VARI) break; n++; b=b"\t"$i}; b=b"\t"$i; n=n-1; print(n"\t"b)}' | uniq |  sort -g | column -t | grep $VARI
```

![](ss.png?raw=true)