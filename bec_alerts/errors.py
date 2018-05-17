# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
import traceback

import raven


reporter = None
logger = logging.getLogger('bec-alerts.errors')


def initialize_error_reporting(sentry_dsn=None):
    """
    Choose which reporting backend to use. Must be called before
    captureException.
    """
    global reporter
    if sentry_dsn:
        reporter = SentryReporter(sentry_dsn)
    else:
        reporter = LoggingReporter()


def capture_exception(message=None):
    """Call capture_exception on the configured reporting backend."""
    global reporter
    if reporter is None:
        raise RuntimeError('Cannot capture exception: initialize_error_reporting was not called')
    reporter.capture_exception(message)


class SentryReporter:
    """Reports errors to a Sentry instance."""
    def __init__(self, sentry_dsn):
        self.client = raven.Client(sentry_dsn)

    def capture_exception(self, message):
        self.client.captureException(extra={'message': message})


class LoggingReporter:
    """Logs errors to the bec-alerts.errors logger."""
    def capture_exception(self, message):
        if message:
            logger.error(message)
        logger.error(traceback.format_exc())
