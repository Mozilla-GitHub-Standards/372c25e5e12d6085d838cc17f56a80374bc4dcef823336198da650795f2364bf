# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from collections import namedtuple

import pytest

from bec_alerts.models import User
from bec_alerts.tests import IssueFactory
from bec_alerts.triggers import AlwaysNotifyTrigger
from bec_alerts.watcher import TriggerEvaluator


Alert = namedtuple('Alert', ['to', 'subject', 'body'])


class CollectingAlertBackend:
    def __init__(self):
        self.alerts = []

    def send_alert(self, to, subject, body):
        self.alerts.append(Alert(to=to, subject=subject, body=body))


@pytest.fixture
def set_trigger_classes(mocker):
    mock_get = mocker.patch('bec_alerts.watcher.get_trigger_classes')

    def func_set_trigger_classes(triggers):
        mock_get.return_value = triggers
    return func_set_trigger_classes


@pytest.mark.django_db
def test_evaluator_works(reraise_errors):
    alert_backend = CollectingAlertBackend()
    evaluator = TriggerEvaluator(alert_backend, dry_run=False)
    evaluator.run_job()


@pytest.mark.django_db
def test_evaluator_always(reraise_errors, set_trigger_classes):
    alert_backend = CollectingAlertBackend()
    evaluator = TriggerEvaluator(alert_backend, dry_run=False)
    set_trigger_classes([AlwaysNotifyTrigger])

    issue = IssueFactory.create(message='AlwaysTest')
    evaluator.run_job()

    assert len(alert_backend.alerts) == 1
    alert = alert_backend.alerts[0]
    assert alert.to == 'test@example.com'
    assert alert.subject == '[Firefox Browser Errors] Error: AlwaysTest'
    assert 'AlwaysTest' in alert.body
    assert issue.groupId in alert.body

    user = User.objects.get(email='test@example.com')
    assert user.has_been_notified_about(issue)

    # If we run again, the issue will not be re-evaluated
    evaluator.run_job()
    assert len(alert_backend.alerts) == 1


@pytest.mark.django_db
def test_evaluator_dry_run(reraise_errors, set_trigger_classes):
    alert_backend = CollectingAlertBackend()
    evaluator = TriggerEvaluator(alert_backend, dry_run=True)
    set_trigger_classes([AlwaysNotifyTrigger])

    issue = IssueFactory.create(message='AlwaysTest')
    evaluator.run_job()

    assert len(alert_backend.alerts) == 1

    # Dry runs do not store that a user was notified
    user = User.objects.get(email='test@example.com')
    assert not user.has_been_notified_about(issue)

    # If we run again, the issue will be re-evaluated since it was a dry
    # run
    evaluator.run_job()
    assert len(alert_backend.alerts) == 2
