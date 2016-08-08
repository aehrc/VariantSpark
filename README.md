Variant Spark
==============

variant-spark is a tool ... bleh bhel 

### Building

variant-spark required java jdk 1.8+ and maven 3+

In order to build the binaries use:

	mvn clean install
	
### Running

variant-spark requires an existing spark 1.6+ installation (either a local one or a cluster one).

To run variant-spark use:

	./variant-spark [(--spark|--local) <spark-options>* --] [<command>] <command-options>*

In order to obtain the list of the available commands use:

	./variant-spark -h
	
In order to obtain help for a specific command (for example `importance`) use:

	./variant-spark importance -h

You can use `--spark` maker before the command to pass any `spark-submit` options to variant spark. The list of spark options needs to be terminated with `--`, e.g:

	./variant-spark --spark --master yarn-client --num-executors 32 -- importance .... 
	
Please, note that `--spark` needs to be the first argument of `variant-spark`

You can also run variant-spark in the `--local` mode. In this mode variant-spark will ignore any Hadoop and Spark configuration files and run in the local mode for both Hadooop and Spark. In particular in this mode all file paths are interpreted as local filesystem paths. Also any parameters passed after `--local` and before `--` are ignored. For example:

	./variant-spark --local -- importance data/small.vcf -ff data/small-labels.csv -fc 22_16051249 -v -t 5

Note: The difference between running in `--local` mode and in `--spark` with `local` master is that in the latter case spark will  use the hadoop filesystem configuration and the input files need to be copied to this filesystem (e.g. HDFS) 


### Running examples




	


	