Variant Spark
==============

variant-spark is a tool ... bleh bhel 

### Building

variant-spark requires java jdk 1.8+ and maven 3+

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

You can use `--spark` marker before the command to pass `spark-submit` options to variant-spark. The list of spark options needs to be terminated with `--`, e.g:

	./variant-spark --spark --master yarn-client --num-executors 32 -- importance .... 
	
Please, note that `--spark` needs to be the first argument of `variant-spark`

You can also run variant-spark in the `--local` mode. In this mode variant-spark will ignore any Hadoop or Spark configuration files and run in the local mode for both Hadooop and Spark. In particular in this mode all file paths are interpreted as local file system paths. Also any parameters passed after `--local` and before `--` are ignored. For example:

	./variant-spark --local -- importance data/small.vcf -ff data/small-labels.csv -fc 22_16051249 -v -t 5

Note: The difference between running in `--local` mode and in `--spark` with `local` master is that in the latter case Spark uses the hadoop filesystem configuration and the input files need to be copied to this filesystem (e.g. HDFS) 


### Running examples

variant-spark comes with a few example scripts in the `scripts` directory that demonstrate how to run its commands on sample data .

There is a few small data sets in the `data` directory suitable for running in the `--local` mode. The scripts that operate on this data start with the `local-` prefix. For example

	./scripts/local-run-importance-ch22.sh 
	
runs variable importance command on a small sample of the chromosome 22 vcf file (from 1000 Genomes Project)


The full size examples require a cluster environment (the scripts are configured to work with Spark on YARN).

The data required for the examples can be obtained from: [https://bitbucket.csiro.au/projects/PBDAV/repos/variant-spark-data](https://bitbucket.csiro.au/projects/PBDAV/repos/variant-spark-data)

This repository uses the git Large File Support extension, which needs to be installed first (see: [https://git-lfs.github.com/](https://git-lfs.github.com/))

Clone the `variant-spark-data` repository and then to install the test data into your hadoop filesystem use:

	./install-data
	
By default the sample data will installed into the `variant-spark-data\input` sub directory of your HDFS home directory.

You can choose a different location by setting the `VS_DATA_DIR` environment variable.

After the test data has been successfully copied to HDFS you can run examples scripts, e.g.:

	./scripts/run-importance-ch22.sh

Note: if you installed the data to a non default location the `VS_DATA_DIR` needs to be set accordingly when running the examples






	


	