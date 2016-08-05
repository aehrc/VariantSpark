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

	./variant-spark [--spark <spark-options>* --] [<command>] <command-options>*

In order to obtain the list of the available commands use:

	./variant-spark -h
	
In order to obtain help for a specific command (for example `importance`) use:

	./variant-spark importance -h

You can use `--spark` maker before the command to pass any `spark-submit` options to variant spark. The list of spark options needs to be terminated with `--`, e.g:

	./variant-spark --spark --master yarn-client --num-executors 32 -- importance .... 
	
Please, note that `--spark` needs to be the first argument of `variant-spark`


### Running examples

TBP


	


	