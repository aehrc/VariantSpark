Running VariantSpark on AWS EMR
================================

This is a work in progress instruction on running `variant-spark` on Amazon EMR.

### Quick start

This is a simple guide on how to run variant-spark importance analysis similar to this described in [HipsterIndex Blog Post](https://databricks.com/blog/2017/07/26/breaking-the-curse-of-dimensionality-in-genomics-using-wide-random-forests.html) on Amazon AWS EMR (Elastic Map Reduce).

It assumes basic knowledge of python, some AWS services (S3, EC2 and EMR) and ideally familiarity with AWS command line interface.

System requirements include `python2.7+` and `pip`. 

Firstly install the AWS command line utility `aws cli` as described in [Installing the AWS Command Line Interface](http://docs.aws.amazon.com/cli/latest/userguide/installing.html). 

Then configure your `aws cli`, providing your default region, access keys, etc with:

    aws configure
    
(More info on the configuration process and using the `aws cli` can be found at [AWS CLI User Guide](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-welcome.html)



Now we can install the `vs-emr` - variant spark cli for EMR - with:

    pip install --user ./python 
    
(if you use some form of python virtual environment you can skip the --user option). 

To verify the installation and see the available commands use: 

    vs-emr --help
    
Create the `vs-emr` configuration:

    mkdir ~/.vs_emr
    cp conf/config.min.yaml ~/.vs_emr/config.yaml
    
Edit the configuration in `~/.vs_emr/config.yaml` replacing the values in `<<>>` with appropriate values.


The sample data for hipsterIndex demo are available at `s3://variant-spark/datasets/hipsterIndex/`.

To run the demo we first create a small cluster with two `m4.large` worker instances (as defined in `~/.vs_emr/config.yaml`):

    vs-emr start-cluster --cluster-id-file /tmp/cluster-id
    
This command will start an EMR cluster configured to run `variant-spark` and save its id to `/tmp/cluster-id`.  The cluster will auto-terminate after the last step has been completed so we need to submit the steps before the cluster setup has finished (which usually takes between 5 to 8 minutes).

To run the `variant-spark` importance on hipster index data use: 
    
    vs-emr submit-cmd --cluster-id-file /tmp/cluster-id  importance -if s3://variant-spark/datasets/hipsterIndex/hipster.vcf.bz2 -ff s3://variant-spark/datasets/hipsterIndex/hipster_labels.txt -fc label -v -rn 1000 -rbs 100 -on 100 -of s3://au.csiro.pbdava.test/variant-spark/output/hipster-importance-1.csv

This should complete in about 5 minutes (after the cluster setup has finished) and save the top 100 important variables to `s3://au.csiro.pbdava.test/variant-spark/output/hipster-importance-1.csv`

In this example we use on-demand instances for both the master and the workers. If you prefer to use spot instances at the max price of say $0.1 you can add the `--conf "bidPrice=0.1"` option to `start-cluster` e.g.
    
    vs-emr start-cluster --cluster-id-file /tmp/cluster-id --conf "bidPrice=0.1"
    
You can examine the cluster configuration and log files using AWS console or  `aws-cli` while its running as well as up to seven days after its termination.


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
The file needs to be saved to ${HOME}/.vs_emr/config.yaml

#### Usage

To see available commands use:

    vs-emr  help
    
### Running on sample datasets

    vs-emr start-cluster 
    vs-emr submit-cmd --cluster-id j-1PH8VDFJUDV0K  importance -if s3://au.csiro.pbdava.test/variant-spark/data/chr22_1000.vcf -ff s3://au.csiro.pbdava.test/variant-spark/data/chr22-labels.csv -fc 22_16051249 -v -rn 500 -rbs 20


    vs-emr submit-cmd --cluster-id-file /tmp/cluster-id  importance -if s3://variant-spark/datasets/hipsterIndex/hipster.vcf.bz2 -ff s3://variant-spark/datasets/hipsterIndex/hipster_labels.txt -fc label -v -rn 10000 -rbs 100 -of s3://au.csiro.pbdava.test/variant-spark/output/hipster-importance-1.csv -on 100

You can submit mulitple steps to the same cluster and also turn off the auto-terminate option.

In which case though please remember to terminate the cluster ... 
