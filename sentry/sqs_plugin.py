# Copyright 2017 Sentry (https://sentry.io) and individual contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This is a copy of the Amazon SQS plugin from
# https://github.com/getsentry/sentry-plugins with the following modifications:
#
# - Added an "Endpoint URL" parameter to the plugin that is passed to boto3 as
#   the endpoint_url parameter. This is required for local development to allow
#   boto3 to connect to the localstack docker container.
from __future__ import absolute_import

import boto3

from sentry.plugins.bases.data_forwarding import DataForwardingPlugin
from sentry.plugins.validators import URLValidator
from sentry.utils import json

from sentry_plugins.base import CorePluginMixin
from sentry_plugins.utils import get_secret_field_config


def OptionalURLValidator(value, **kwargs):
    """
    Validates URLs, but allows for empty strings, unlike the default URL
    validator.
    """
    if not value:
        return value
    return URLValidator(value, **kwargs)


def get_regions():
    return boto3.session.Session().get_available_regions('sqs')


class AmazonSQSPlugin(CorePluginMixin, DataForwardingPlugin):
    title = 'Amazon SQS Standalone'
    slug = 'amazon-sqs-standaone'
    description = 'Forward Sentry events to Amazon SQS.'
    conf_key = 'amazon-sqs-standalone'

    def post_process(self, event, **kwargs):
        super(AmazonSQSPlugin, self).post_process(event, **kwargs)

    def get_config(self, project, **kwargs):
        return [
            {
                'name': 'endpoint_url',
                'label': 'Endpoint URL',
                'type': 'text',
                'validators': [OptionalURLValidator],
                'placeholder': 'https://sqs-us-east-1.amazonaws.com',
                'required': False,
            },
            {
                'name': 'queue_url',
                'label': 'Queue URL',
                'type': 'url',
                'placeholder': 'https://sqs-us-east-1.amazonaws.com/12345678/myqueue',
            },
            {
                'name': 'region',
                'label': 'Region',
                'type': 'select',
                'choices': tuple((z, z) for z in get_regions()),
            },
            get_secret_field_config(
                name='access_key',
                label='Access Key',
                secret=self.get_option('access_key', project),
            ),
            get_secret_field_config(
                name='secret_key',
                label='Secret Key',
                secret=self.get_option('secret_key', project),
            ),
        ]

    def forward_event(self, event, payload):
        endpoint_url = self.get_option('endpoint_url', event.project)
        queue_url = self.get_option('queue_url', event.project)
        access_key = self.get_option('access_key', event.project)
        secret_key = self.get_option('secret_key', event.project)
        region = self.get_option('region', event.project)

        if not all((queue_url, access_key, secret_key, region)):
            return

        # TODO(dcramer): Amazon doesnt support payloads larger than 256kb
        # We could support this by simply trimming it and allowing upload
        # to S3
        message = json.dumps(payload)
        if len(message) > 256 * 1024:
            return False

        client = boto3.client(
            service_name='sqs',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            endpoint_url=endpoint_url or None,
        )

        client.send_message(
            QueueUrl=queue_url,
            MessageBody=message,
        )

        return True
