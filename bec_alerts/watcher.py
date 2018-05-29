# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
import time
from datetime import timedelta

import click
import datadog
from django.db import transaction
from django.utils import timezone

from bec_alerts.alert_backends import ConsoleAlertBackend, EmailAlertBackend
from bec_alerts.errors import capture_exception, initialize_error_reporting
from bec_alerts.models import Issue, TriggerRun
from bec_alerts.triggers import get_trigger_classes
from bec_alerts.utils import latest_nightly_appbuildid


logger = logging.getLogger('bec-alerts.watcher')


class TriggerEvaluator:
    """
    Creates Triggers from the definitions in bec_alerts.triggers and
    evaluates them against all issues with new events since the last
    evaluation run.
    """
    def __init__(self, alert_backend, dry_run):
        self.alert_backend = alert_backend
        self.dry_run = dry_run

        self.now = timezone.now()

    @transaction.atomic
    def run_job(self):
        if self.dry_run:
            logger.info('--dry-run passed; no run logs will be saved.')
            self.evaluate_triggers()
        else:
            current_run = TriggerRun(ran_at=self.now, finished=False)
            current_run.save()

            self.evaluate_triggers()

            current_run.finished = True
            current_run.save()

            # Remove run logs older than 7 days
            TriggerRun.objects.filter(ran_at__lte=self.now - timedelta(days=7)).delete()

    def evaluate_triggers(self):
        last_finished_run = TriggerRun.objects.filter(finished=True).order_by('-ran_at').first()
        if last_finished_run:
            issues = Issue.objects.filter(last_seen__gte=last_finished_run.ran_at)
        else:
            issues = Issue.objects.all()

        logger.info(f'Found {len(issues)} issues since last finished run.')

        # Clear caches since we're starting a new run
        latest_nightly_appbuildid.cache_clear()

        # Evaluate triggers
        for trigger_class in get_trigger_classes():
            trigger = trigger_class(self.alert_backend, self.dry_run, self.now)
            for issue in issues:
                logger.debug(
                    f'Evaluting {trigger_class.__name__} against issue {issue.group_id}.'
                )

                # Don't let a single failure block all trigger evaluations
                try:
                    trigger.evaluate(issue)
                except Exception:
                    capture_exception(
                        f'Error while running trigger {trigger.__name__} against issue '
                        f'{issue.group_id}'
                    )


@click.command()
@click.option(
    '--once',
    is_flag=True,
    default=False,
)
@click.option(
    '--dry-run',
    is_flag=True,
    default=False,
)
@click.option(
    '--console-alerts',
    is_flag=True,
    default=False,
)
@click.option(
    '--verify-email',
    is_flag=True,
    default=False,
    envvar='SES_VERIFY_EMAIL',
)
@click.option(
    '--sleep-delay',
    default=300,
    envvar='WATCHER_SLEEP_DELAY',
)
@click.option(
    '--endpoint-url',
    envvar='SES_ENDPOINT_URL',
)
@click.option(
    '--connect-timeout',
    default=30,
    envvar='AWS_CONNECT_TIMEOUT',
)
@click.option(
    '--read-timeout',
    default=30,
    envvar='AWS_READ_TIMEOUT',
)
@click.option(
    '--sentry-dsn',
    envvar='SENTRY_DSN',
)
@click.option(
    '--datadog-counter-name',
    default='bec-alerts.watcher.health',
    envvar='DATADOG_COUNTER_NAME',
)
@click.option(
    '--from-email',
    default='notifications@sentry.prod.mozaws.net',
    envvar='SES_FROM_EMAIL',
)
def main(
    once,
    dry_run,
    console_alerts,
    sleep_delay,
    from_email,
    endpoint_url,
    connect_timeout,
    read_timeout,
    verify_email,
    datadog_counter_name,
    sentry_dsn,
):
    """Evaluate alert triggers and send alerts."""
    initialize_error_reporting(sentry_dsn)

    try:
        datadog.initialize()

        if console_alerts:
            alert_backend = ConsoleAlertBackend()
        else:
            alert_backend = EmailAlertBackend(
                from_email=from_email,
                endpoint_url=endpoint_url,
                connect_timeout=connect_timeout,
                read_timeout=read_timeout,
                verify_email=verify_email,
            )
    except Exception:
        # Just make sure Sentry knows that we failed on startup
        capture_exception('Failed during watcher startup')
        raise

    while True:
        try:
            evaluator = TriggerEvaluator(alert_backend, dry_run)
            evaluator.run_job()
        except Exception as err:
            capture_exception('Error evaluating triggers')
        finally:
            datadog.statsd.increment(datadog_counter_name)

        if once:
            break
        time.sleep(sleep_delay)
