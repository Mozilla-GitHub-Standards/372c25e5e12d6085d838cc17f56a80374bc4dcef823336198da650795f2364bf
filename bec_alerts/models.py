# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from functools import lru_cache

from django.contrib.postgres.fields import JSONField
from django.db import connection, models
from django.utils import timezone


class User(models.Model):
    """An email address and associated user info."""
    email = models.EmailField(unique=True)
    issues = models.ManyToManyField('Issue', through='UserIssue')

    def has_been_notified_about(self, issue):
        try:
            return UserIssue.objects.get(user=self, issue=issue).last_notified is not None
        except UserIssue.DoesNotExist:
            return False


class EventCount(models.Aggregate):
    function = 'hll_cardinality'
    template = 'hll_cardinality(hll_union_agg(%(expressions)s))'
    output_field = models.FloatField()


class IssueQuerySet(models.QuerySet):
    def with_event_counts(self):
        return self.annotate(event_count=EventCount('issuebucket__count_set'))

    def event_count(self):
        return self.aggregate(count=EventCount('issuebucket__count_set'))['count'] or 0

    def filter_dates(self, start_date=None, end_date=None):
        filters = {}
        if start_date is not None:
            filters['issuebucket__date__gte'] = start_date
        if end_date is not None:
            filters['issuebucket__date__lte'] = end_date

        return self.filter(**filters)


class Issue(models.Model):
    """
    A Sentry issue uniquely identified by a group_id.

    Stores data needed to build error alerts for the issue. That data is
    saved the first time we receive an event for this issue.
    """
    group_id = models.CharField(max_length=255, unique=True)
    last_seen = models.DateTimeField(null=True, default=None)
    module = models.CharField(max_length=255, default='')
    stack_frames = JSONField(default=list)
    message = models.TextField(default='')

    objects = IssueQuerySet.as_manager()

    def count_event(self, event_id, date):
        bucket, created = IssueBucket.objects.get_or_create(
            issue=self,
            date=date,
        )
        bucket.count_event(event_id)


class UserIssue(models.Model):
    """Stores when a user was last notified about an issue."""
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    issue = models.ForeignKey(Issue, on_delete=models.CASCADE)
    last_notified = models.DateTimeField(null=True, default=None)

    class Meta:
        unique_together = ['user', 'issue']


class TriggerRun(models.Model):
    """
    Stores when the last successfully-finished evaluation of alert
    triggers was.
    """
    ran_at = models.DateTimeField()
    finished = models.BooleanField(default=False)


class HyperLogLogField(models.Field):
    """Custom database field for a postgresql-hll column."""
    def __init__(self, *args, **kwargs):
        kwargs['default'] = self.default_value
        super().__init__(*args, **kwargs)

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        del kwargs['default']
        return name, path, args, kwargs

    @lru_cache(maxsize=2)
    def default_value(self):
        """
        The default value is a blob of binary data from the hll_empty()
        Postgres function.
        """
        with connection.cursor() as cursor:
            cursor.execute('SELECT hll_empty()')
            return cursor.fetchone()[0]

    def db_type(self, connection):
        return 'hll'


class IssueBucket(models.Model):
    """Bucket for storing event counts per-issue, bucketed per-day."""
    issue = models.ForeignKey(Issue, on_delete=models.CASCADE)
    date = models.DateField(default=timezone.now)
    count_set = HyperLogLogField()

    class Meta:
        unique_together = ['issue', 'date']

    def count_event(self, event_id):
        with connection.cursor() as cursor:
            cursor.execute(
                '''
                UPDATE bec_alerts_issuebucket
                    SET count_set = hll_add(count_set, hll_hash_text(%s))
                WHERE id = %s
                ''',
                (event_id, self.id),
            )
