# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from textwrap import indent

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from bec_alerts.errors import capture_exception


class AlertBackend:
    """Base class for backends that output alerts."""
    def send_alert(self, to, subject, body):
        """
        :param to:
            Email address of user to send alert to.
        :param subject:
            Subject line for alert notification.
        :param body:
            Main body text of alert notification.
        """
        raise NotImplementedError()


class ConsoleAlertBackend(AlertBackend):
    """Outputs alert contents to stdout."""
    def send_alert(self, to, subject, body):
        print(f'== Sending Alert')
        print(f'   To: {to}')
        print(f'   Subject: {subject}')
        print('')
        print(indent(body, '   '))
        print('')


class EmailAlertBackend(AlertBackend):
    """Sends alert emails via Amazon SES."""
    def __init__(
        self,
        from_email,
        endpoint_url,
        connect_timeout,
        read_timeout,
        verify_email,
    ):
        super().__init__()
        self.from_email = from_email

        config = Config(connect_timeout=connect_timeout, read_timeout=read_timeout)
        self.ses = boto3.client(
            'ses',
            config=config,
            endpoint_url=endpoint_url,
        )

        if verify_email:
            self.ses.verify_email_identity(EmailAddress=self.from_email)

    def send_alert(self, to, subject, body):
        try:
            self.ses.send_email(
                Destination={'ToAddresses': [to]},
                Message={
                    'Body': {
                        'Text': {
                            'Charset': 'UTF-8',
                            'Data': body,
                        },
                    },
                    'Subject': {
                        'Charset': 'UTF-8',
                        'Data': subject,
                    },
                },
                Source=self.from_email
            )
        except ClientError as err:
            capture_exception(f'Could not send email: {err.response["Error"]["Message"]}')
