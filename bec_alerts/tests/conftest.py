# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import sys
from collections import namedtuple

import pytest

from bec_alerts import errors


CollectedError = namedtuple('CollectedError', ['message', 'value'])


class RecordingErrorReporter:
    def __init__(self):
        self.errors = []

    def capture_exception(self, message):
        exc_type, exc_value, exc_traceback = sys.exc_info()
        self.errors.append(CollectedError(message=message, value=exc_value))


class ReraisingErrorReporter:
    def capture_exception(self, message):
        exc_type, exc_value, exc_traceback = sys.exc_info()
        raise exc_value


@pytest.fixture
def reraise_errors():
    old_reporter = errors.reporter
    errors.reporter = ReraisingErrorReporter()
    yield errors.reporter
    errors.reporter = old_reporter


@pytest.fixture
def collect_errors():
    old_reporter = errors.reporter
    errors.reporter = RecordingErrorReporter()
    yield errors.reporter
    errors.reporter = old_reporter
