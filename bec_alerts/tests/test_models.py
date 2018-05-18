# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from datetime import date

import pytest

from bec_alerts.models import IssueBucket
from bec_alerts.tests import IssueFactory


@pytest.mark.django_db
def test_issuebucket_event_count_uniques():
    issue = IssueFactory.create()

    issue.count_event('asdf', date(2018, 1, 1))
    issue.count_event('asdf', date(2018, 1, 1))
    issue.count_event('asdf', date(2018, 1, 2))
    issue.count_event('asdf', date(2018, 1, 3))
    issue.count_event('qwer', date(2018, 1, 1))
    assert IssueBucket.objects.event_count(issue=issue) == 2


@pytest.mark.django_db
def test_issuebucket_event_count_date_ranges():
    issue = IssueFactory.create()

    issue.count_event('day1-1', date(2018, 1, 1))
    issue.count_event('day1-2', date(2018, 1, 1))
    issue.count_event('day2-1', date(2018, 1, 2))
    issue.count_event('day3-1', date(2018, 1, 3))
    issue.count_event('day3-2', date(2018, 1, 3))

    assert IssueBucket.objects.event_count(start_date=date(2018, 1, 1)) == 5
    assert IssueBucket.objects.event_count(start_date=date(2018, 1, 1), end_date=date(2018, 1, 2)) == 3
    assert IssueBucket.objects.event_count(start_date=date(2018, 1, 2), end_date=date(2018, 1, 2)) == 1
    assert IssueBucket.objects.event_count(end_date=date(2018, 1, 2)) == 3
    assert IssueBucket.objects.event_count(start_date=date(2018, 1, 1), end_date=date(2018, 1, 3)) == 5
    assert IssueBucket.objects.event_count(start_date=date(2018, 1, 6)) == 0


@pytest.mark.django_db
def test_issuebucket_event_count_multiple_issues():
    issue1, issue2 = IssueFactory.create_batch(2)

    issue1.count_event('asdf', date(2018, 1, 1))
    issue1.count_event('qwer', date(2018, 1, 1))
    issue2.count_event('asdf', date(2018, 1, 2))

    assert IssueBucket.objects.event_count(issue=issue1) == 2
    assert IssueBucket.objects.event_count(issue=issue2) == 1


@pytest.mark.django_db
def test_issuebucket_top_issue_counts():
    issue1, issue2, issue3 = IssueFactory.create_batch(3)

    for k in range(10):
        issue1.count_event(str(k), date(2018, 1, 1))
    for k in range(20):
        issue2.count_event(str(k), date(2018, 1, 1))
    for k in range(5):
        issue3.count_event(str(k), date(2018, 1, 1))

    assert IssueBucket.objects.top_issue_counts(limit=3) == [
        (20, issue2),
        (10, issue1),
        (5, issue3),
    ]

    assert IssueBucket.objects.top_issue_counts(limit=2) == [
        (20, issue2),
        (10, issue1),
    ]
