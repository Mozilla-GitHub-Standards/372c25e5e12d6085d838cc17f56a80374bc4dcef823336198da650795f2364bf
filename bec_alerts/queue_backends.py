# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import json
import logging
import time

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError


class QueueBackend:
    """
    Base class for backends that listen for incoming Sentry events.
    """
    def receive_events(self):
        """
        :returns:
            An iterator filled with Sentry events. It may potentially
            be empty if no new events are available, and may block while
            it polls for new events.
        """
        raise NotImplementedError()


class StaticQueueBackend(QueueBackend):
    """Returns a static list of events, and nothing afterwards."""
    def __init__(self, event_lists):
        self.event_lists = event_lists

    def receive_events(self):
        if self.event_lists:
            return self.event_lists.pop(0)

        return []


class SQSQueueBackend(QueueBackend):
    """Listens for incoming events from an Amazon SQS queue."""
    DEFAULT_QUEUE_NAME = 'sentry_errors'

    def __init__(
        self,
        queue_url,
        endpoint_url,
        connect_timeout,
        read_timeout,
    ):
        self.logger = logging.getLogger('bec-alerts.sqs_queue')

        # If wait time and read_timeout are the same, the connection times out
        # before AWS (or at least localstack) can send us an empty response. A 2
        # second buffer helps avoid this.
        self.wait_time = min(20, read_timeout - 2)

        config = Config(connect_timeout=connect_timeout, read_timeout=read_timeout)
        self.sqs = boto3.client(
            'sqs',
            config=config,
            endpoint_url=endpoint_url,
        )

        if queue_url:
            self.queue_url = queue_url
        else:
            self.queue_url = self.create_queue()

    def create_queue(self, max_attempts=5, sleep_delay=5):
        attempts = 0
        last_error = None
        while attempts < max_attempts:
            try:
                response = self.sqs.create_queue(QueueName=self.DEFAULT_QUEUE_NAME)
                break
            except ClientError as err:
                last_error = err
                attempts += 1
                time.sleep(sleep_delay)

        if attempts == max_attempts:
            raise last_error

        return response['QueueUrl']

    def receive_events(self):
        response = self.sqs.receive_message(
            QueueUrl=self.queue_url,
            AttributeNames=[
                'SentTimestamp'
            ],
            MaxNumberOfMessages=10,
            MessageAttributeNames=[
                'All'
            ],
            VisibilityTimeout=30,
            WaitTimeSeconds=self.wait_time,
        )

        for message in response.get('Messages', []):
            receipt_handle = message['ReceiptHandle']
            yield json.loads(message['Body'])
            self.sqs.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle
            )
