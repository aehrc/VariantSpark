Running VariantSpark on AWS EMR
================================

### Distributions 

variant-spark distributions are available on `s3` Sydney Region (ap-southeast-2)

- develpment builds in `s3://variant-spark/s3://variant-spark/unstable/<version>`
- relase builds in: `s3://variant-spark/s3://variant-spark/stable/<version>`

where `<version>` is:

- `<maj-ver>.<min-ver>/<maj-ver>.<min-ver>-<hash>` for unstable builds
- `<maj-ver>.<min-ver>/<maj-ver>.<min-ver>.<rev>` for stable builds

For example the distribution for release 0.0.2 is available at:
     `s3://variant-spark/s3://variant-spark/stable/0.0/0.0.2`

### The basic of running on EMR

To run variant-spark you need emr-5.7+ with Spark 2.1+ installed.
To install variant spark in EMR cluster add the use the following bootstrap step:

    s3://variant-spark/s3://variant-spark/stable/<version>/bootstrap/install-variant-spark.sh --release-url s3://variant-spark/s3://variant-spark/stable/<version>
    
  
To add an emr step for variant spark use:

    Type: Spark application  
    Deploy mode: Client
    Spark-submit options: --class au.csiro.variantspark.cli.VariantSparkApp
    Jar location: /mnt/variant-spark/variant-spark_2.11-all.jar
    Arguments: <varint-spark arguments, e.g: importance -if s3://au.csiro.pbdava.test/variant-spark/data/chr22_1000.vcf ...>

To add custom emr step for variant spark use:

    Jar location: command-runner.jar
    Argument: spark-submit --deploy-mode client --class au.csiro.variantspark.cli.VariantSparkApp /mnt/variant-spark/variant-spark_2.11-all.jar <varint-spark arguments>


### Using  `vs-emr`

`vs-emr` is a wrapper on aws cli that simplified creation of EMR clusters for variant-spark and submission of variant-spark steps.


#### Installation

To install `vs-emr` cd to `python` directory and run:

    pip install [--user] . 
    
The --user is optional and should be used if you do not have permissions to install python packages in system location.

#### Configurtion

TODO: describe configuration:
(example is in conf) 
The file needs to be saved to ${HOME}/.vs_emr/default.yaml

#### Usage

To see available commands use:

    vs-emr  help
    
    
### Running on sample datasets


    vs-emr start-cluster 
    vs-emr submit-cmd --cluster-id j-1PH8VDFJUDV0K  importance -if s3://au.csiro.pbdava.test/variant-spark/data/chr22_1000.vcf -ff s3://au.csiro.pbdava.test/variant-spark/data/chr22-labels.csv -fc 22_16051249 -v -rn 500 -rbs 20

You can submit mulitple steps to the same cluster and also turn off the auto-terminate option.

In which case though please remember to terminate the cluster ... 


 
 
 






    


    
