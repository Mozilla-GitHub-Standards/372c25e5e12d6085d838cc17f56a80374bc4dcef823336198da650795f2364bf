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


class IssueBucketManager(models.Manager):
    def event_count(self, issue=None, start_date=None, end_date=None):
        """
        Count the total number of unique events seen, filtered by the
        given parameters.

        :param Issue issue:
            Only count events for this issue, if given.
        :param datetime.date start_date:
            Only count events occurring on or after this date, if given.
        :param datetime.date end_date:
            Only count events occurring on or before this date, if
            given.
        """
        with connection.cursor() as cursor:
            # We need raw SQL to call the postgresql-hll functions
            query = '''
                SELECT
                    hll_cardinality(hll_union_agg(count_set))
                FROM bec_alerts_issuebucket
            '''

            where_clauses = []
            params = {}

            if issue:
                where_clauses.append('issue_id = %(issue_id)s')
                params['issue_id'] = issue.id

            if start_date:
                where_clauses.append('date >= %(start_date)s')
                params['start_date'] = start_date

            if end_date:
                where_clauses.append('date <= %(end_date)s')
                params['end_date'] = end_date

            if where_clauses:
                query += f'WHERE {" AND ".join(where_clauses)}'

            cursor.execute(query, params)
            return cursor.fetchone()[0] or 0

    def top_issue_counts(self, start_date=None, end_date=None, limit=10):
        """
        Return a list of tuples of (event_count, Issue), ordered by
        their event count over the requested period in descending order.

        :param datetime.date start_date:
            Only count events occurring on or after this date, if given.
        :param datetime.date end_date:
            Only count events occurring on or before this date, if
            given.
        :param limit:
            Limit the amount of top issues returned.
        """
        with connection.cursor() as cursor:
            where_clauses = []
            params = {'limit': limit}

            if start_date:
                where_clauses.append('date >= %(start_date)s')
                params['start_date'] = start_date

            if end_date:
                where_clauses.append('date <= %(end_date)s')
                params['end_date'] = end_date

            where_query = ''
            if where_clauses:
                where_query = f'WHERE {" AND ".join(where_clauses)}'

            query = f'''
                SELECT
                    issue_id,
                    hll_cardinality(hll_union_agg(count_set)) AS event_count
                FROM bec_alerts_issuebucket
                {where_query}
                GROUP BY issue_id
                ORDER BY event_count DESC
                LIMIT %(limit)s
            '''

            cursor.execute(query, params)
            rows = cursor.fetchall()

            issue_ids = [row[0] for row in rows]
            issues_by_id = {issue.id: issue for issue in Issue.objects.filter(id__in=issue_ids)}

            return [(row[1], issues_by_id[row[0]]) for row in rows]


class IssueBucket(models.Model):
    """Bucket for storing event counts per-issue, bucketed per-day."""
    issue = models.ForeignKey(Issue, on_delete=models.CASCADE)
    date = models.DateField(default=timezone.now)
    count_set = HyperLogLogField()

    objects = IssueBucketManager()

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
