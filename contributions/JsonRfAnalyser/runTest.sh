#!/bin/bash
set -x

gzip -dc RF.json.gz > RF.json

python JsonRfAnalyser.py forest RF.json v_forest.csv
python JsonRfAnalyser.py tree   RF.json v_tree.csv
python JsonRfAnalyser.py branch RF.json v_branch.csv
python JsonRfAnalyser.py limit  RF.json v_limit.csv 10
python JsonRfAnalyser.py roots RF.json v_roots.csv

tar czvf output.tar.gz v_*

python JsonRfAnalyser.py comb  v_forest.csv 1
python JsonRfAnalyser.py comb  v_tree.csv   1
python JsonRfAnalyser.py combx v_branch.csv 1
python JsonRfAnalyser.py combx v_limit.csv  1

python JsonRfAnalyser.py comb  v_forest.csv 2
python JsonRfAnalyser.py comb  v_tree.csv   2
python JsonRfAnalyser.py combx v_branch.csv 2
python JsonRfAnalyser.py combx v_limit.csv  2

python JsonRfAnalyser.py comb  v_forest.csv 3
python JsonRfAnalyser.py comb  v_tree.csv   3
python JsonRfAnalyser.py combx v_branch.csv 3
python JsonRfAnalyser.py combx v_limit.csv  3

python JsonRfAnalyser.py comb  v_forest.csv 4
python JsonRfAnalyser.py comb  v_tree.csv   4
python JsonRfAnalyser.py combx v_branch.csv 4
python JsonRfAnalyser.py combx v_limit.csv  4

sort v_roots.csv | uniq -c | awk '{print($1"\t"$2)}' | sort -g | tail

VARI="7_17284577_T_C"
grep $VARI  v_branch.csv | tr , \\t | awk -v VARI="$VARI" '{n=0; b=""; for(i=1; i<=NF; i++) {if($i==VARI) break; n++; b=b"\t"$i}; b=b"\t"$i; n=n-1; print(n"\t"b)}' | uniq |  sort -g | column -t | grep $VARI