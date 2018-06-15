Running VariantSpark on AWS EMR
================================

This page contains 'working' instructions for running `variant-spark` on [Amazon EMR](https://aws.amazon.com/emr/).  

NOTE: It is also possible to run VariantSpark on AWS EC2 with Hadoop/Spark libraries installed, if your team has expertise in managing EC2 and Hadoop clusters. We do **not** provide instructions for this scenario on this page.

ALTERNATIVE: If you'd prefer to quickly try a working sample of the `variant-spark` importance analysis, then you can use our example on the Databricks platform.  See this  [HipsterIndex Blog Post](https://databricks.com/blog/2017/07/26/breaking-the-curse-of-dimensionality-in-genomics-using-wide-random-forests.html) for detail.  You can sign up for the community edition and use it for one hour at a time for free. See this [link](https://github.com/aehrc/VariantSpark#databricks-notebook-examples) for the 6 quick steps to run this example.

### Process and Goal

You will set up both a client and a server.  

For the client you'll use/set up the following:
- your terminal
- `awscli` - the AWS client scripting utility
- `vs-emr` - a utility wrapper on `aws cli` that simplifies creation of EMR clusters for `variant-spark` and submission of `variant-spark` job steps
- server configuration file - `config.yaml`

For the server you'll use/set up the following:
- several S3 buckets
- at least one AWS EMR cluster with one master and one or more worker nodes w/Apache Spark
- an IAM role

We've included a diagram for your reference:

![AWS EMR Architecture](/images/AWS-EMR-VariantSpark.png)
### Client Setup 

- **verify** system requirements include `python2.7+` and associated version of  `pip`
- **install** the AWS command line utility `aws cli` as described in [Installing the AWS Command Line Interface](http://docs.aws.amazon.com/cli/latest/userguide/installing.html). 
- **configure** your `aws cli` providing your default region, access keys, etc using the `aws configure` command. More info on the configuration process and using the `aws cli` can be found at [AWS CLI User Guide](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-welcome.html)
- **run** `pip install --user vs-emr` to install the the `vs-emr` utility   
- **verify** the installation and see the available commands by running this command: `vs-emr --help`
- **configure** `vs-emr` by running: 
   - `vs-emr configure`  

NOTES on `vs-emr` install:  
- If you use some form of python virtual environment, or want to install `vs-emr` system-wide, you should skip the `--user` option. 

### Server Setup - First Part (for S3)
- **choose** S3 storage for `variant-spark` output files and for the EMR cluster logs, use existing S3 bucket, or create a new bucket.  We will use:
- **use** `s3://<your-bucket-name>/variant-spark/output/` as the output location
- **use** `s3://<your-bucket-name>/variant-spark/logs/` as the cluster log location.

NOTES on S3:
- Please note the trailing slashes! They are important to designate folders in S3.
- The sample data for hipsterIndex demo are available at `s3://variant-spark/datasets/hipsterIndex/`.


  ### Server Setup - Second Part (for the EMR cluster)
- **create** a small cluster with two `m4.large` worker instances (as defined in `~/.vs_emr/config.yaml`) by running this command:`vs-emr start-cluster --cluster-id-file /tmp/cluster-id`
    
NOTES on EMR:
- This command will start an EMR cluster configured to run `variant-spark` and save its id to `/tmp/cluster-id`.
- The cluster will auto-terminate after the last step has been completed so we need to submit the steps before the cluster setup has finished (which usually takes between 5 to 8 minutes).



  ### Run the Analysis
 - **run** the `variant-spark` importance on hipster index data run (use your S3 output bucket name): 
  
    `vs-emr submit-cmd --cluster-id-file /tmp/cluster-id  importance -if s3://variant-spark/datasets/hipsterIndex/hipster.vcf.bz2 -ff s3://variant-spark/datasets/hipsterIndex/hipster_labels.txt -fc label -v -rn 1000 -rbs 100 -on 100 -of s3://<your-bucket-name>/variant-spark/output/hipster-importance.csv`

NOTES on Analysis:
- Run the command above on a single line in your terminal
- This should complete in about 5 minutes (after the cluster setup has finished).
- This should save the top 100 important variables to `s3://<your-bucket-name>/variant-spark/output/hipster-importance-1.csv`. 
- You can now download the result file from S3 using the AWS console or `aws cli`


### Alternative Cluster Setup Options
In this example we use on-demand instances for both the master and the workers. If you prefer to use spot instances at the max price of say $0.1 you can add the `--conf "bidPrice=0.1"` option to `start-cluster` e.g.
    
    vs-emr start-cluster --cluster-id-file /tmp/cluster-id --conf "bidPrice=0.1"
    
You can examine the cluster configuration and log files using AWS console or  `aws cli` while it's running as well as up to seven days after its termination.

## Distributions 

`variant-spark` distributions are available on S3 (local to ap-southeast-2 region)

- development builds in `s3://variant-spark/unstable/<version>`
- release builds in: `s3://variant-spark/stable/<version>`

where `<version>` is:

- `<maj-ver>.<min-ver>/<maj-ver>.<min-ver>-<git-hash>` for unstable builds
- `<maj-ver>.<min-ver>/<maj-ver>.<min-ver>.<rev>` for stable builds

For example the distribution for release 0.0.2 is available at:
     `s3://variant-spark/stable/0.0/0.0.2`

### The basic of running on EMR 

To run variant-spark you need emr-5.7+ with Spark 2.1+ installed.
To install variant spark in EMR cluster add the use the following bootstrap step:

    s3://variant-spark/stable/<version>/bootstrap/install-variant-spark.sh --release-url s3://variant-spark/stable/<version>
    
This bootstrap action will deploy the desired version of `variant-spark's` assembly jar to the master node at `/mnt/variant-spark/variant-spark_2.11-all.jar`. This allows to run `variant-spark` in the `client` deploy mode.

To add an ApacheSpark step for variant spark use:

    Type: Spark application  
    Deploy mode: Client
    Spark-submit options: --class au.csiro.variantspark.cli.VariantSparkApp
    Jar location: /mnt/variant-spark/variant-spark_2.11-all.jar
    Arguments: <variant-spark arguments, e.g: importance -if s3://au.csiro.pbdava.test/variant-spark/data/chr22_1000.vcf ...>

To add a custom  EMR step for variant spark use:

    Jar location: command-runner.jar
    Argument: spark-submit --deploy-mode client --class au.csiro.variantspark.cli.VariantSparkApp /mnt/variant-spark/variant-spark_2.11-all.jar <variant-spark arguments>

#### Configuration

The default location for `vs-emr` configuration is `.vs_emr/config.yaml`. A different file can be passed with '--config' options.

The configuration file is in YAML format and consists of two sections:
 * `default` - with the default values for configuration parameters
 * `profiles` with a list of profiles that can be used to override the `default` values.
 
 For example:
 
    default:
      variantSparkReleaseUrl: "s3://variant-spark/unstable/0.1/0.1-134ed4e"
      instanceType: "m4.large"
      autoTerminate: true
      releaseLabel: "emr-5.8.0"
      bidPrice: 0.07
      ec2Attributes:
        AdditionalMasterSecurityGroups: ["sg-32323232", "sg-32323222"]
        KeyName: "mykey"
        SubnetId: "sub-323232"
        InstanceProfile: "EMR_EC2_DefaultRole"
      worker:
        instanceCount: 2
      conf:
        logBucketUri: "s3://my-bucket/logs/"
    profiles:
      db-small:   #profile name "db-small" 
        bidPrice: null
        instanceType: "r4.2xlarge"
        worker:
          instanceCount: 2
      db-large:
        bidPrice: 0.6
        instanceType: "r4.8xlarge"
        worker: 
          instanceCount: 4 
 
Profiles are activated with the `--profile` option:

    vs-emr start-cluster --profile db-large 

Multiple profiles can be activated together (when it makes senses) with repeated '--profile' options.

Specific values can be overridden with the --conf option (multiple entries are allowed) e.g.:

    vs-emr start-cluster —-profile --profile db-large --conf bidPrice=1.0

Some most common options are also exposed as explicit parameters  (e.g.: --worker-instances):

    vs-emr start-cluster —-profile large --conf bidPrice=1.0 --worker-instances 20

For the list of available configuration parameters and how they are translated to `aws emr create-cluster` call please check the `python/vs_emr/templates/spot-cluster.yaml` file. It's a [mustache template](https://mustache.github.io/) and hopefully it's self explanatory.

#### Running on sample datasets

As as a test you can can `variant-spark` on a small subset of 1000 Genome Project chromosome 22. The results (top 20 important variables are produced to `stdout`).

    vs-emr start-cluster 
    vs-emr submit-cmd --cluster-id <YOUR_CLUSTE_ID>  importance -if s3://variant-spark/datasets/examples/chr22_1000.vcf -ff s3://variant-spark/datasets/examples/chr22-labels.csv -fc 22_16051249 -v -rn 500 -rbs 20
