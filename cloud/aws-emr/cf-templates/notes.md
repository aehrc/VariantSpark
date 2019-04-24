# CloudFormation templates for VariantSpark

## Templates

* `template.yaml` - the 'original' template

* `template-updated.yaml` - the template for 'batch job' style runs by end users

## How to run it

### End-user no install

1. End user needs to be signed in to their AWS account

2. Direct them to click in the browser on a link like that:

```
https://console.aws.amazon.com/cloudformation/home?region=ap-southeast-2#/stacks/new?stackName=VariantSparkSample&templateURL=https://s3-ap-southeast-2.amazonaws.com/csiro-tb/home/maciej/template-updated.yaml
```

Important:

* make sure that the template is public and published to a public s3 bucket (unless you want to restrict access to the users of the same AWS account)

* make sure the S3 bucket/template url is correct (currently it's just an example I've experimented with)

* make sure the AWS region in the link above is what you wanted it to be


### AWS CloudFormation console

* Go to AWS CloudFormation console and click "create stack"

* Click "template is ready" and then "Upload a template file"

* or alternatively upload beforehand your template to an s3 bucket and  click "Amazon S3 URL" as the source of the template

* Click next etc.


## Dev notes

Fails with "the bid price is invalid"

Change it to on demand or make sure bid is valid

