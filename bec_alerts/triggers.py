# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""
This module contains alert trigger definitions that describe when alerts
should be sent to users. To subscribe to an alert, you must add it to
this file as a new subclass of the Trigger class, or add your email to
the list of emails for an existing trigger subclass.
"""
import logging
from datetime import timedelta

from django.template.loader import render_to_string
from django.utils import timezone
from django.utils.functional import cached_property

from bec_alerts.models import IssueBucket, User, UserIssue


logger = logging.getLogger('bec-alerts.triggers')


class Trigger:
    """
    Parent class for alert triggers. Subclasses should override methods
    in this class to customize the behavior of their alert.
    """
    #: name is ideally unique, and is included by default in the subject
    name = 'Error'

    # List of emails to alert when triggered
    emails = []

    #: Path to the template file to use for alerts sent by this trigger.
    #: Paths are relative to the jinja2 directory and use Jinja2 for
    #: templating.
    template = 'basic_alert.txt'

    #: String template for alert subject lines. Uses ''.format() syntax
    #: and has the following values available:
    #:   trigger: instance of this trigger subclass
    #:   issue: Issue instance of the issue that we're triggering on
    #:   user: User instance that we're alerting
    subject_template = '[Firefox Browser Errors] {trigger.name}: {issue.message}'

    #: If False, the trigger will not be evaluated. Used mostly by
    #: example triggers.
    enabled = True

    def __init__(self, alert_backend, dry_run, now):
        self.alert_backend = alert_backend
        self.dry_run = dry_run
        self.now = now

    @cached_property
    def users(self):
        """
        Return a list of User objects for the emails in the emails class
        attribute.
        """
        users = []
        for email in self.emails:
            user, created = User.objects.get_or_create(email=email)
            users.append(user)
        return users

    def alert_user(self, user, issue):
        """Send an alert to a user about the given issue."""
        class_name = self.__class__.__name__
        logger.info(
            f'Trigger {class_name} alerting user {user.email} of issue {issue.fingerprint}'
        )

        alert_body = render_to_string(self.template, {
            'user': user,
            'issue': issue,
            'trigger': self,
        })
        subject = self.subject_template.format(
            issue=issue,
            user=user,
            trigger=self,
        )
        self.alert_backend.send_alert(to=user.email, subject=subject, body=alert_body)

        if not self.dry_run:
            user_issue, created = UserIssue.objects.get_or_create(user=user, issue=issue)
            user_issue.last_notified = self.now
            user_issue.save()

    def evaluate(self, issue):
        """
        Check the given issue against the conditions of this trigger.
        This method is called whenever at least one event for the given
        issue has been received since the last time the watcher was
        called.

        Subclasses must implement this and, if a user should be alerted,
        call self.alert_user(user, issue) to send the alert.
        """
        raise NotImplementedError()


def get_trigger_classes():
    return [trigger_class for trigger_class in Trigger.__subclasses__() if trigger_class.enabled]


# The following are example trigger illustrating how triggers work #####


class AlwaysNotifyTrigger(Trigger):
    """Notify users every time we receive events for an issue."""
    emails = ['test@example.com']
    name = 'Error'
    enabled = False

    def evaluate(self, issue):
        for user in self.users:
            self.alert_user(user, issue)


class NewNotifyTrigger(Trigger):
    """
    Notify users every time a new issue they've never been notified
    about has been seen.
    """
    emails = ['test@example.com']
    name = 'New Error'
    enabled = False

    def evaluate(self, issue):
        for user in self.users:
            if not user.has_been_notified_about(issue):
                self.alert_user(user, issue)


# Add Trigger subclasses below this line ###############################


class NewTopIssueTrigger(Trigger):
    """
    Notify users when a new issue they've never been notified about is a
    top 10 crasher for the past week.
    """
    emails = ['mkelly@mozilla.com']
    name = 'New top crasher'

    @cached_property
    def top_issue_counts(self):
        now = timezone.now()
        week_ago = now - timedelta(days=7)
        return IssueBucket.objects.top_issue_counts(
            start_date=week_ago.date(),
            limit=10,
        )

    def evaluate(self, issue):
        is_top_issue = False
        for event_count, top_issue in self.top_issue_counts:
            # Ignore issues with less than 200 events just to avoid
            # problems with new deployments or weird date edge cases.
            if top_issue == issue and event_count > 200:
                is_top_issue = True
                break

        if not is_top_issue:
            return

        for user in self.users:
            if not user.has_been_notified_about(issue):
                self.alert_user(user, issue)
