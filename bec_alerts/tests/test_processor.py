# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from datetime import datetime
from random import randint
from uuid import uuid4

import pytest
from django.utils import timezone

from bec_alerts.processor import listen
from bec_alerts.models import Issue, IssueBucket
from bec_alerts.queue_backends import StaticQueueBackend
from bec_alerts.utils import aware_datetime


def stack_frame(**kwargs):
    return {
        'function': kwargs.get('function', 'funcname'),
        'module': kwargs.get('module', 'resource://fake.jsm'),
        'lineMo': kwargs.get('lineNo', 17),
        'colNo': kwargs.get('colNo', 56),
    }


def sentry_event(date=None, module='resource://fake.jsm', stack_frames=None, **kwargs):
    """
    Create a mock Sentry event. This only covers attributes we use.
    """
    event = {
        'eventID': kwargs.get('eventID', str(uuid4())),
        'message': kwargs.get('message', 'Error: fake error'),
        'dateReceived': (date or timezone.now()).strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
        'groupID': kwargs.get('groupID', randint(1, 999999)),
        'entries': kwargs.get('entries', [
            {
                'type': 'exception',
                'data': {
                    'values': [
                        {
                            'module': module,
                            'stacktrace': {
                                'frames': stack_frames or [
                                    stack_frame(),
                                ],
                            },
                        },
                    ],
                },
            },
        ]),
    }

    return event


@pytest.mark.django_db
def test_listen_works(reraise_errors):
    queue_backend = StaticQueueBackend([
        [sentry_event()],
    ])
    listen(queue_backend=queue_backend, worker_message_count=1)


@pytest.mark.django_db
def test_listen_message_count(reraise_errors):
    queue_backend = StaticQueueBackend([
        [sentry_event(groupID='asdf'), sentry_event(groupID='asdf')],
        [sentry_event(groupID='asdf')],
        [sentry_event(groupID='qwer')],
    ])
    listen(queue_backend=queue_backend, worker_message_count=3)

    # The last message should never have been processed
    assert not Issue.objects.filter(group_id='qwer').exists()


@pytest.mark.django_db
def test_listen_ignore_invalid(collect_errors):
    missing_id = sentry_event()
    del missing_id['eventID']
    missing_group_id = sentry_event()
    del missing_group_id['groupID']
    missing_message = sentry_event()
    del missing_message['message']

    queue_backend = StaticQueueBackend([
        [sentry_event(groupID='asdf')],
        [missing_id, missing_group_id, missing_message],
        [sentry_event(groupID='zxcv')],
    ])
    listen(queue_backend=queue_backend, worker_message_count=2)

    assert len(collect_errors.errors) == 3
    error = collect_errors.errors[0]
    assert error.message == 'Error processing event'

    # The last event should have been processed since the middle one
    # failed.
    assert Issue.objects.filter(group_id='zxcv').exists()


@pytest.mark.django_db
def test_listen_processing(reraise_errors):
    stack_frames = [
        stack_frame()
    ]
    event = sentry_event(
        groupID='asdf',
        message='Fake message',
        date=datetime(2018, 1, 1),
        module='resource://Browser.jsm',
        stack_frames=stack_frames,
    )
    queue_backend = StaticQueueBackend([
        [
            event,
            sentry_event(groupID='asdf', date=datetime(2018, 1, 2)),
        ],
    ])
    listen(queue_backend=queue_backend, worker_message_count=2)

    issue = Issue.objects.get(group_id='asdf')
    assert issue.message == 'Fake message'
    assert issue.last_seen == aware_datetime(2018, 1, 2)
    assert issue.module == 'resource://Browser.jsm'
    assert issue.stack_frames == stack_frames

    # Check that counts are being bucketed per-date
    assert IssueBucket.objects.event_count(issue=issue) == 2
    assert IssueBucket.objects.event_count(issue=issue, start_date=aware_datetime(2018, 1, 2)) == 1
