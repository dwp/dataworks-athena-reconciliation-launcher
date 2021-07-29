#!/usr/bin/env python3

"""batch_job_launcher_lambda"""
import argparse
import boto3
import json
import logging
import os
import sys
import socket
import botocore

UNSET_TEXT = "NOT_SET"

args = None
logger = None

boto_client_config = botocore.config.Config(
    max_pool_connections=100, retries={"max_attempts": 10, "mode": "standard"}
)


# Initialise logging
def setup_logging(logger_level):
    """Set the default logger with json output."""
    the_logger = logging.getLogger()
    for old_handler in the_logger.handlers:
        the_logger.removeHandler(old_handler)

    new_handler = logging.StreamHandler(sys.stdout)
    hostname = socket.gethostname()

    json_format = (
        f'{{ "timestamp": "%(asctime)s", "log_level": "%(levelname)s", "message": "%(message)s", '
        f'"environment": "{args.environment}", "application": "{args.application}", '
        f'"module": "%(module)s", "process":"%(process)s", '
        f'"thread": "[%(thread)s]", "host": "{hostname}" }}'
    )

    new_handler.setFormatter(logging.Formatter(json_format))
    the_logger.addHandler(new_handler)
    new_level = logging.getLevelName(logger_level)
    the_logger.setLevel(new_level)

    if the_logger.isEnabledFor(logging.DEBUG):
        # Log everything from boto3
        boto3.set_stream_logger()
        the_logger.debug(f'Using boto3", "version": "{boto3.__version__}')

    return the_logger


def get_parameters():
    """Parse the supplied command line arguments.

    Returns:
        args: The parsed and validated command line arguments

    """
    parser = argparse.ArgumentParser(
        description="Start up and shut down ASGs on demand"
    )

    # Parse command line inputs and set defaults
    parser.add_argument("--aws-profile", default="default")
    parser.add_argument("--aws-region", default="eu-west-2")
    parser.add_argument("--sns-topic", help="SNS topic ARN")
    parser.add_argument("--environment", help="Environment value", default=UNSET_TEXT)
    parser.add_argument("--application", help="Application", default=UNSET_TEXT)
    parser.add_argument(
        "--slack-channel-override",
        help="Slack channel to use for overriden jobs",
        default=UNSET_TEXT,
    )
    parser.add_argument("--log-level", help="Log level for lambda", default="INFO")

    _args = parser.parse_args()

    # Override arguments with environment variables where set
    if "AWS_PROFILE" in os.environ:
        _args.aws_profile = os.environ["AWS_PROFILE"]

    if "AWS_REGION" in os.environ:
        _args.aws_region = os.environ["AWS_REGION"]

    if "ENVIRONMENT" in os.environ:
        _args.environment = os.environ["ENVIRONMENT"]

    if "APPLICATION" in os.environ:
        _args.application = os.environ["APPLICATION"]

    if "MONITORING_SNS_TOPIC" in os.environ:
        _args.monitoring_sns_topic = os.environ["MONITORING_SNS_TOPIC"]

    if "MONITORING_ERRORS_SEVERITY" in os.environ:
        _args.severity = os.environ["MONITORING_ERRORS_SEVERITY"]
    else:
        _args.severity = "High"

    if "MONITORING_ERRORS_TYPE" in os.environ:
        _args.notification_type = os.environ["MONITORING_ERRORS_TYPE"]
    else:
        _args.notification_type = "Warning"

    if "SLACK_CHANNEL_OVERRIDE" in os.environ:
        _args.slack_channel_override = os.environ["SLACK_CHANNEL_OVERRIDE"]

    if "BATCH_JOB_QUEUE" in os.environ:
        _args.batch_job_queue = os.environ["BATCH_JOB_QUEUE"]

    if "BATCH_JOB_NAME" in os.environ:
        _args.batch_job_name = os.environ["BATCH_JOB_NAME"]

    if "BATCH_JOB_DEFINITION_NAME" in os.environ:
        _args.batch_job_definition_name = os.environ["BATCH_JOB_DEFINITION_NAME"]

    if "BATCH_PARAMETERS_JSON" in os.environ:
        _args.batch_parameters_json = os.environ["BATCH_PARAMETERS_JSON"]

    if "LOG_LEVEL" in os.environ:
        _args.log_level = os.environ["LOG_LEVEL"]

    return _args


def get_sns_client():
    global boto_client_config

    return boto3.client("sns", config=boto_client_config)


def get_batch_client():
    global boto_client_config

    return boto3.client("batch", config=boto_client_config)


def handler(event, context):
    """Handle the event from AWS.

    Args:
        event (Object): The event details from AWS
        context (Object): The context info from AWS

    """
    global args
    global logger

    args = get_parameters()
    logger = setup_logging(args.log_level)

    dumped_event = get_escaped_json_string(event)
    logger.info(f'SNS Event", "sns_event": {dumped_event}, "mode": "handler')

    if not args.monitoring_sns_topic:
        raise Exception("Monitoring SNS topic is not set")

    batch_client = get_batch_client()
    sns_client = get_sns_client()

    try:
        response = submit_batch_job(
            batch_client,
            args.batch_job_queue,
            args.batch_job_name,
            args.batch_job_definition_name,
            args.batch_parameters_json,
        )

        job_arn = response["jobArn"]
        job_id = response["jobId"]

        logger.info(
            f'Batch job submitted successfully", '
            + f'"job_queue": "{args.batch_job_queue}", "job_name": "{args.batch_job_name}", ' 
            + f'"job_definition_name": "{args.batch_job_definition_name}", '
            + f'"job_arn": "{job_arn}", "job_id": "{job_id}'
        )
    except botocore.exceptions.ClientError as err:
        error_message = err.response['Error']['Message']

        logger.error(
            f'Error occurred submitting batch job", "error_message": "{error_message}", '
            + f'"job_queue": "{args.batch_job_queue}", "job_name": "{args.batch_job_name}", ' 
            + f'"job_definition_name": "{args.batch_job_definition_name}'
        )

        payload = generate_monitoring_error_message_payload(
            args.slack_channel_override,
            args.batch_job_queue,
            args.batch_job_name,
            args.batch_job_definition_name,
            args.severity,
            args.notification_type,
            error_message,
        )

        send_sns_message(
            sns_client,
            payload,
            args.monitoring_sns_topic,
            args.batch_job_queue,
            args.batch_job_name,
            args.batch_job_definition_name,
        )


def generate_monitoring_error_message_payload(
    slack_channel_override,
    job_queue,
    job_name,
    job_definition_name,
    severity,
    notification_type,
    error_message,
):
    """Generates a payload for a monitoring message.

    Arguments:
        slack_channel_override (string): slack channel to override (or None)
        job_queue (string): the job queue arn
        job_name (string): batch job name
        job_definition_name (string): batch job definition name
        severity (string): the severity of the alert
        notification_type (string): the notification type of the alert
        error_message (string): the error message

    """
    custom_elements = generate_custom_elements(
        job_queue,
        job_name,
        job_definition_name,
        error_message,
    )

    title_text = f"Error starting batch job"

    payload = {
        "severity": severity,
        "notification_type": notification_type,
        "slack_username": "AWS Batch Job Error",
        "title_text": title_text,
        "custom_elements": custom_elements,
    }

    if slack_channel_override:
        payload["slack_channel_override"] = slack_channel_override

    dumped_payload = get_escaped_json_string(payload)
    logger.info(
        f'Generated monitoring SNS error payload", "payload": {dumped_payload}, "error_message": "{error_message}", '
        + f'"job_queue": "{job_queue}", "job_name": "{job_name}", "job_definition_name": "{job_definition_name}'
    )

    return payload


def generate_custom_elements(
    job_queue,
    job_name,
    job_definition_name,
    error_message,
):
    """Generates a custom elements array.

    Arguments:
        job_queue (string): the job queue arn
        job_name (string): batch job name
        job_definition_name (string): batch job definition name
        error_message (string): the error message

    """
    logger.info(
        f'Generating custom elements", "job_name": "{job_name}", "error_message": "{error_message}", '
        + f'"job_queue": "{job_queue}", "job_definition_name": "{job_definition_name}'
    )

    job_queue_end = job_queue.split("/")[-1]

    custom_elements = [
        {"key": "Job name", "value": job_name},
        {"key": "Job queue", "value": job_queue_end},
        {"key": "Job definition name", "value": job_definition_name},
        {"key": "Error", "value": error_message},
    ]

    return custom_elements


def send_sns_message(
    sns_client,
    payload,
    sns_topic_arn,
    job_queue,
    job_name,
    job_definition_name,
):
    """Publishes the message to sns.
    Arguments:
        sns_client (client): The boto3 client for SQS
        payload (dict): the payload to post to SNS
        sns_topic_arn (string): the arn for the SNS topic
        job_queue (dict): The job queue arn
        job_name (dict): The job name
        job_definition_name (string): batch job definition name
    """
    global logger

    json_message = json.dumps(payload)

    dumped_payload = get_escaped_json_string(payload)
    logger.info(
        f'Publishing payload to SNS", "payload": {dumped_payload}, "sns_topic_arn": "{sns_topic_arn}", '
        + f'"job_queue": "{job_queue}", "job_name": "{job_name}", "job_definition_name": "{job_definition_name}'
    )

    return sns_client.publish(TopicArn=sns_topic_arn, Message=json_message)


def submit_batch_job(
    batch_client,
    job_queue,
    job_name,
    job_definition_name,
    parameters,
):
    """Publishes the message to sns.

    Arguments:
        batch_client (client): The boto3 client for Batch
        job_queue (dict): The job queue arn
        job_name (dict): The job name
        job_definition_name (dict): The job name
        parameters (string): The parameters as a json string

    """
    global logger

    logger.info(
        f'Submitting batch job", "job_definition_name": "{job_definition_name}", '
        + f'"job_queue": "{job_queue}", "job_name": "{job_name}", "parameters": "{parameters}'
    )

    if parameters:
        parameters_json = json.dumps(parameters)
        return batch_client.submit_job(
            jobName=job_name,
            jobQueue=job_queue,
            jobDefinition=job_definition_name,
            parameters=parameters_json
        )
    else:
        return batch_client.submit_job(
            jobName=job_name,
            jobQueue=job_queue,
            jobDefinition=job_definition_name
        )


def get_escaped_json_string(json_string):
    try:
        escaped_string = json.dumps(json.dumps(json_string))
    except:
        escaped_string = json.dumps(json_string)

    return escaped_string


if __name__ == "__main__":
    try:
        args = get_parameters()
        logger = setup_logging("INFO")

        boto3.setup_default_session(
            profile_name=args.aws_profile, region_name=args.aws_region
        )
        logger.info(os.getcwd())
        json_content = json.loads(open("resources/event.json", "r").read())
        handler(json_content, None)
    except Exception as err:
        logger.error(f'Exception occurred for invocation", "error_message": {err}')
