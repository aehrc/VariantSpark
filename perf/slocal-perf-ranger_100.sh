#!/bin/bash
/Users/szu004/dev/variant-spark/variant-spark --spark --master local[4] --driver-java-options "-Dsparkle.prof=true"  -- importance -if /Users/szu004/dev/variant-spark/data/ranger-wide_1000_10000.csv.bz2 -it csv -ff /Users/szu004/dev/variant-spark/data/ranger-labels_10000.csv -fc resp5 -v -t 20 -rt 100 -sp 4

