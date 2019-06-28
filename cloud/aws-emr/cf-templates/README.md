# VariantSpark CloudFormation templates for AWS EMR

## First time users

If this is the first time ever that you will be spinning
up an AWS EMR cluster, you must create the two default AWS roles:
`EMR_DefaultRole` and `EMR_EC2_DefaultRole`.

If you are using AWS EMR dashboard, these roles are created
automatically, but not when you are using a CloudFormation template.

More information on how to create these roles are in:
https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-iam-roles-defaultroles.html

You could also use the provided CloudFormation template `emr-roles.yaml`
to create the two required roles (the template is untested at this moment).

## The VariantSpark templates

The two templates to create a VariantSpark cluster on AWS EMR
service are:

- `VariantSpark_Hail_EMR_Step.yaml` for processing "batch" jobs or steps, and
- `VariantSpark_Hail_EMR_Notebook.yaml` for starting a cluster with a Jupyter notebook.

The first template provisions a cluster, runs a single step (VariantSpark
job) and then terminates the cluster.

The second template provisions a cluster. Once the cluster is up and running,
the user will need to connect to a Jupyter notebook through which to drive
the VariantSpark jobs and manually terminate the cluster once they are done using it.

The second template requires the user to provide a valid name of an SSH key pair, as well as a public subnet and a security group configured to allow
incoming traffic from their computer in order to be able to access the Jupyter
notebook.

## One-click deployment

It's possible to publish a link on a webpage, that would take a user
to CloudFormation dashboard with a template already loaded:

1. Upload the CloudFormation template to a public AWS S3 bucket.

2. On your webpage add a link in the form of:
`https://console.aws.amazon.com/cloudformation/home?region=ap-southeast-2#/stacks/new?stackName=VariantSparkSample&templateURL=https://s3-ap-southeast-2.amazonaws.com/bucket/path/published-template.yaml`
This is just an example, to make it work you need to make sure the AWS region,
S3 bucket path and the template name are correct for your case.

3. The user needs to log in to their AWS account.

4. When the user clicks on the link, they will be taken to AWS Cloudformation
screen with the template pre-loaded. They will then have a chance to provide
the user-configurable parameters and request the stack to be created.