#!/usr/bin/env python3

"""batch_job_launcher_lambda"""
import pytest
import argparse
import botocore
from batch_job_launcher_lambda import batch_job_launcher

import unittest
from unittest import mock
from unittest.mock import MagicMock
from unittest.mock import call

JOB_ARN_KEY = "jobArn"
JOB_ID_KEY = "jobId"

ERROR_NOTIFICATION_TYPE = "Error"
WARNING_NOTIFICATION_TYPE = "Warning"
INFORMATION_NOTIFICATION_TYPE = "Information"

CRITICAL_SEVERITY = "Critical"

JOB_QUEUE_NAME = "test/job_queue"
JOB_DEFINITION_NAME = "test/job_definition"
JOB_NAME = "test job"
JOB_ARN = "test arn"
JOB_ID = "test id"
ERROR_MESSAGE = "Test error has occurred"

SNS_TOPIC_ARN = "test-sns-topic-arn"
MOCK_CHANNEL = "test_slack_channel"

args = argparse.Namespace()
args.monitoring_sns_topic = SNS_TOPIC_ARN
args.slack_channel_override = MOCK_CHANNEL
args.log_level = "INFO"
args.severity = CRITICAL_SEVERITY
args.notification_type = ERROR_NOTIFICATION_TYPE
args.batch_job_queue = JOB_QUEUE_NAME
args.batch_job_name = JOB_NAME
args.batch_job_definition_name = JOB_DEFINITION_NAME
args.batch_parameters_json = None


class TestRetriever(unittest.TestCase):
    @mock.patch("batch_job_launcher_lambda.batch_job_launcher.send_sns_message")
    @mock.patch(
        "batch_job_launcher_lambda.batch_job_launcher.generate_monitoring_error_message_payload"
    )
    @mock.patch("batch_job_launcher_lambda.batch_job_launcher.submit_batch_job")
    @mock.patch("batch_job_launcher_lambda.batch_job_launcher.get_sns_client")
    @mock.patch("batch_job_launcher_lambda.batch_job_launcher.get_batch_client")
    @mock.patch("batch_job_launcher_lambda.batch_job_launcher.setup_logging")
    @mock.patch("batch_job_launcher_lambda.batch_job_launcher.get_parameters")
    @mock.patch("batch_job_launcher_lambda.batch_job_launcher.logger")
    def test_handler_processes_message(
        self,
        mock_logger,
        get_parameters_mock,
        setup_logging_mock,
        get_batch_client_mock,
        get_sns_client_mock,
        submit_batch_job_mock,
        generate_monitoring_error_message_payload_mock,
        send_sns_message_mock,
    ):
        batch_client_mock = mock.MagicMock()
        sns_client_mock = mock.MagicMock()
        get_batch_client_mock.return_value = batch_client_mock
        get_sns_client_mock.return_value = sns_client_mock
        get_parameters_mock.return_value = args

        response_dict = {
            JOB_ARN_KEY: JOB_NAME,
            JOB_ID_KEY: JOB_ID,
        }

        submit_batch_job_mock.return_value = response_dict

        payload = {
            "severity": CRITICAL_SEVERITY,
            "notification_type": ERROR_NOTIFICATION_TYPE,
            "slack_username": "AWS Batch Job Notification",
            "title_text": "Job changed to - _FAILED_",
            "custom_elements": [
                {"key": "Job name", "value": JOB_NAME},
                {"key": "Job queue", "value": JOB_QUEUE_NAME},
            ],
        }

        generate_monitoring_error_message_payload_mock.return_value = payload

        event = {
            "test_key": "test_value",
        }

        batch_job_launcher.handler(event, None)

        get_batch_client_mock.assert_called_once()
        get_sns_client_mock.assert_called_once()
        get_parameters_mock.assert_called_once()
        setup_logging_mock.assert_called_once()

        submit_batch_job_mock.assert_called_once_with(
            batch_client_mock,
            JOB_QUEUE_NAME,
            JOB_NAME,
            JOB_DEFINITION_NAME,
            None,
        )

        generate_monitoring_error_message_payload_mock.assert_not_called()
        send_sns_message_mock.assert_not_called()


class TestRetriever(unittest.TestCase):
    @mock.patch("batch_job_launcher_lambda.batch_job_launcher.send_sns_message")
    @mock.patch(
        "batch_job_launcher_lambda.batch_job_launcher.generate_monitoring_error_message_payload"
    )
    @mock.patch("batch_job_launcher_lambda.batch_job_launcher.submit_batch_job")
    @mock.patch("batch_job_launcher_lambda.batch_job_launcher.get_sns_client")
    @mock.patch("batch_job_launcher_lambda.batch_job_launcher.get_batch_client")
    @mock.patch("batch_job_launcher_lambda.batch_job_launcher.setup_logging")
    @mock.patch("batch_job_launcher_lambda.batch_job_launcher.get_parameters")
    @mock.patch("batch_job_launcher_lambda.batch_job_launcher.logger")
    def test_handler_processes_message_with_error(
        self,
        mock_logger,
        get_parameters_mock,
        setup_logging_mock,
        get_batch_client_mock,
        get_sns_client_mock,
        submit_batch_job_mock,
        generate_monitoring_error_message_payload_mock,
        send_sns_message_mock,
    ):
        batch_client_mock = mock.MagicMock()
        sns_client_mock = mock.MagicMock()
        get_batch_client_mock.return_value = batch_client_mock
        get_sns_client_mock.return_value = sns_client_mock
        get_parameters_mock.return_value = args

        response_dict = {
            JOB_ARN_KEY: JOB_NAME,
            JOB_ID_KEY: JOB_ID,
        }

        error_message = "test_error_message"
        error_response = {
            "Error": {"Code": "test_error_code", "Message": error_message}
        }
        client_error = botocore.exceptions.ClientError(
            error_response=error_response, operation_name="op_name"
        )

        submit_batch_job_mock.side_effect = client_error

        payload = {
            "severity": CRITICAL_SEVERITY,
            "notification_type": ERROR_NOTIFICATION_TYPE,
            "slack_username": "AWS Batch Job Notification",
            "title_text": "Job changed to - _FAILED_",
            "custom_elements": [
                {"key": "Job name", "value": JOB_NAME},
                {"key": "Job queue", "value": JOB_QUEUE_NAME},
            ],
        }

        generate_monitoring_error_message_payload_mock.return_value = payload

        event = {
            "test_key": "test_value",
        }

        batch_job_launcher.handler(event, None)

        get_batch_client_mock.assert_called_once()
        get_sns_client_mock.assert_called_once()
        get_parameters_mock.assert_called_once()
        setup_logging_mock.assert_called_once()

        submit_batch_job_mock.assert_called_once_with(
            batch_client_mock,
            JOB_QUEUE_NAME,
            JOB_NAME,
            JOB_DEFINITION_NAME,
            None,
        )

        generate_monitoring_error_message_payload_mock.assert_called_once_with(
            MOCK_CHANNEL,
            JOB_QUEUE_NAME,
            JOB_NAME,
            JOB_DEFINITION_NAME,
            CRITICAL_SEVERITY,
            ERROR_NOTIFICATION_TYPE,
            error_message,
        )
        send_sns_message_mock.assert_called_once_with(
            sns_client_mock,
            payload,
            args.monitoring_sns_topic,
            JOB_QUEUE_NAME,
            JOB_NAME,
            JOB_DEFINITION_NAME,
        )

    @mock.patch("batch_job_launcher_lambda.batch_job_launcher.generate_custom_elements")
    @mock.patch("batch_job_launcher_lambda.batch_job_launcher.logger")
    def test_sns_payload_generates_valid_payload(
        self,
        mock_logger,
        generate_custom_elements_mock,
    ):
        custom_elements = [
            {"key": "Job name", "value": JOB_NAME},
            {"key": "Job queue", "value": JOB_QUEUE_NAME},
            {"key": "Job definition name", "value": JOB_DEFINITION_NAME},
            {"key": "Error", "value": ERROR_MESSAGE},
        ]
        generate_custom_elements_mock.return_value = custom_elements

        expected_payload = {
            "severity": CRITICAL_SEVERITY,
            "notification_type": INFORMATION_NOTIFICATION_TYPE,
            "slack_username": "AWS Batch Job Error",
            "title_text": "Error starting batch job",
            "custom_elements": custom_elements,
        }

        actual_payload = batch_job_launcher.generate_monitoring_error_message_payload(
            None,
            JOB_QUEUE_NAME,
            JOB_NAME,
            JOB_DEFINITION_NAME,
            CRITICAL_SEVERITY,
            INFORMATION_NOTIFICATION_TYPE,
            ERROR_MESSAGE,
        )

        generate_custom_elements_mock.assert_called_once_with(
            JOB_QUEUE_NAME,
            JOB_NAME,
            JOB_DEFINITION_NAME,
            ERROR_MESSAGE,
        )

        self.assertEqual(expected_payload, actual_payload)

    @mock.patch("batch_job_launcher_lambda.batch_job_launcher.generate_custom_elements")
    @mock.patch("batch_job_launcher_lambda.batch_job_launcher.logger")
    def test_sns_payload_generates_valid_payload_with_overriden_slack_channel(
        self,
        mock_logger,
        generate_custom_elements_mock,
    ):
        custom_elements = [
            {"key": "Job name", "value": JOB_NAME},
            {"key": "Job queue", "value": JOB_QUEUE_NAME},
            {"key": "Job definition name", "value": JOB_DEFINITION_NAME},
            {"key": "Error", "value": ERROR_MESSAGE},
        ]
        generate_custom_elements_mock.return_value = custom_elements

        expected_payload = {
            "severity": CRITICAL_SEVERITY,
            "notification_type": INFORMATION_NOTIFICATION_TYPE,
            "slack_username": "AWS Batch Job Error",
            "title_text": "Error starting batch job",
            "custom_elements": custom_elements,
            "slack_channel_override": MOCK_CHANNEL,
        }

        actual_payload = batch_job_launcher.generate_monitoring_error_message_payload(
            MOCK_CHANNEL,
            JOB_QUEUE_NAME,
            JOB_NAME,
            JOB_DEFINITION_NAME,
            CRITICAL_SEVERITY,
            INFORMATION_NOTIFICATION_TYPE,
            ERROR_MESSAGE,
        )

        generate_custom_elements_mock.assert_called_once_with(
            JOB_QUEUE_NAME,
            JOB_NAME,
            JOB_DEFINITION_NAME,
            ERROR_MESSAGE,
        )

        self.assertEqual(expected_payload, actual_payload)

    @mock.patch("batch_job_launcher_lambda.batch_job_launcher.logger")
    def test_send_sns_message_sends_right_message(
        self,
        mock_logger,
    ):
        sns_mock = mock.MagicMock()
        sns_mock.publish = mock.MagicMock()

        payload = {"test_key": "test_value"}

        batch_job_launcher.send_sns_message(
            sns_mock,
            payload,
            SNS_TOPIC_ARN,
            JOB_QUEUE_NAME,
            JOB_NAME,
            JOB_DEFINITION_NAME,
        )

        sns_mock.publish.assert_called_once_with(
            TopicArn=SNS_TOPIC_ARN,
            Message='{"test_key": "test_value"}',
        )

    @mock.patch("batch_job_launcher_lambda.batch_job_launcher.logger")
    def test_generate_custom_elements_generates_valid_payload(
        self,
        mock_logger,
    ):
        expected_payload = [
            {"key": "Job name", "value": JOB_NAME},
            {"key": "Job queue", "value": "job_queue"},
            {"key": "Job definition name", "value": JOB_DEFINITION_NAME},
            {"key": "Error", "value": ERROR_MESSAGE},
        ]
        actual_payload = batch_job_launcher.generate_custom_elements(
            JOB_QUEUE_NAME,
            JOB_NAME,
            JOB_DEFINITION_NAME,
            ERROR_MESSAGE,
        )
        self.assertEqual(expected_payload, actual_payload)

    @mock.patch("batch_job_launcher_lambda.batch_job_launcher.logger")
    def test_submit_batch_job_sends_right_message(
        self,
        mock_logger,
    ):
        batch_mock = mock.MagicMock()
        batch_mock.submit_job = mock.MagicMock()

        parameters = '{"test_key": "test_value"}'

        batch_job_launcher.submit_batch_job(
            batch_mock,
            JOB_QUEUE_NAME,
            JOB_NAME,
            JOB_DEFINITION_NAME,
            parameters,
        )

        batch_mock.submit_job.assert_called_once_with(
            jobName=JOB_NAME,
            jobQueue=JOB_QUEUE_NAME,
            jobDefinition=JOB_DEFINITION_NAME,
            parameters=parameters,
        )


if __name__ == "__main__":
    unittest.main()
