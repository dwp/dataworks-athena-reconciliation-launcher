# dataworks-athena-reconciliation-launcher

## A Lambda which triggers from AWS Glue success events and kicks off required AWS Batch jobs.

This repo contains Makefile to fit the standard pattern. This repo is a base to create new non-Terraform repos, adding the githooks submodule, making the repo ready for use.

After cloning this repo, please run:  
`make bootstrap`

## Environment variables

|Variable name|Example|Description|Required|
|:---|:---|:---|:---|
|AWS_PROFILE| default |The profile for making AWS calls to other services|No|
|AWS_REGION| eu-west-1 |The region the lambda is running in|No|
|ENVIRONMENT| dev |The environment the lambda is running in|No|
|APPLICATION| batch-job-handler |The name of the application|No|
|LOG_LEVEL| INFO |The logging level of the Lambda|No|
|MONITORING_SNS_TOPIC| |The arn of the sns topic to send monitoring messages to|Yes|
|MONITORING_ERRORS_SEVERITY| Critical/High/Medium/Low |The severity of the monitoring error messages|No (default is High)|
|MONITORING_ERRORS_TYPE| Error/Warning |The severity of the monitoring error messages|No (default is Warning)|
|SLACK_CHANNEL_OVERRIDE| dataworks-critical-errors |The name of the topic to send error messages to if overriding defaults|No|
|BATCH_JOB_QUEUE| athena-reconciliation-queue |The name of the queue for the batch job|Yes|
|BATCH_JOB_NAME| athena-reconciliation |The name of batch job to start|Yes|
|BATCH_JOB_DEFINITION_NAME| athena-reconciliation-definition |The name of the job definition for the batch job|Yes|
|BATCH_PARAMETERS_JSON| "{\"test_key\": \"test_value\"}" |Dumped json dict of the parameters desired if any required|No|

## Testing

There are tox unit tests in the module. To run them, you will need the module tox installed with pip install tox, then go to the root of the module and simply run tox to run all the unit tests.

The test may also be ran via `make unittest`.

You should always ensure they work before making a pull request for your branch.

If tox has an issue with Python version you have installed, you can specify such as `tox -e py38`.
