# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from datetime import date

import pytest

from bec_alerts.models import Issue
from bec_alerts.tests import IssueFactory


@pytest.mark.django_db
def test_issue_with_event_counts_uniques():
    issue = IssueFactory.create()

    issue.count_event('asdf', date(2018, 1, 1))
    issue.count_event('asdf', date(2018, 1, 1))
    issue.count_event('asdf', date(2018, 1, 2))
    issue.count_event('asdf', date(2018, 1, 3))
    issue.count_event('qwer', date(2018, 1, 1))

    issue = Issue.objects.with_event_counts().get(pk=issue.pk)
    assert issue.event_count == 2


@pytest.mark.django_db
def test_issue_with_event_counts_date_ranges():
    issue = IssueFactory.create()

    issue.count_event('day1-1', date(2018, 1, 1))
    issue.count_event('day1-2', date(2018, 1, 1))
    issue.count_event('day2-1', date(2018, 1, 2))
    issue.count_event('day3-1', date(2018, 1, 3))
    issue.count_event('day3-2', date(2018, 1, 3))

    assert Issue.objects.filter_dates(
        start_date=date(2018, 1, 1),
    ).event_count() == 5

    assert Issue.objects.filter_dates(
        start_date=date(2018, 1, 1),
        end_date=date(2018, 1, 2),
    ).event_count() == 3

    assert Issue.objects.filter_dates(
        start_date=date(2018, 1, 2),
        end_date=date(2018, 1, 2),
    ).event_count() == 1

    assert Issue.objects.filter_dates(
        end_date=date(2018, 1, 2),
    ).event_count() == 3

    assert Issue.objects.filter_dates(
        start_date=date(2018, 1, 1),
        end_date=date(2018, 1, 3),
    ).event_count() == 5

    assert Issue.objects.filter_dates(
        start_date=date(2018, 1, 6),
    ).event_count() == 0


@pytest.mark.django_db
def test_issuebucket_event_count_multiple_issues():
    issue1, issue2 = IssueFactory.create_batch(2)

    issue1.count_event('asdf', date(2018, 1, 1))
    issue1.count_event('qwer', date(2018, 1, 1))
    issue2.count_event('asdf', date(2018, 1, 2))

    assert Issue.objects.filter(pk=issue1.pk).event_count() == 2
    assert Issue.objects.filter(pk=issue2.pk).event_count() == 1
