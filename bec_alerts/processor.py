# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import itertools
import logging
import os
import time
from datetime import datetime
from multiprocessing import Process

import click
from django.utils import timezone
from django.utils.functional import cached_property

from bec_alerts.errors import capture_exception, initialize_error_reporting
from bec_alerts.models import Issue
from bec_alerts.queue_backends import SQSQueueBackend


class SentryEvent:
    """Container for parsing event data from Sentry."""
    def __init__(self, data):
        self.data = data

        naive_datetime_received = datetime.strptime(data['dateReceived'], '%Y-%m-%dT%H:%M:%S.%fZ')
        self.datetime_received = timezone.make_aware(naive_datetime_received, timezone=timezone.utc)

    @cached_property
    def id(self):
        """Unique ID for the event."""
        return self.data['eventID']

    @cached_property
    def message(self):
        return self.data['message']

    @cached_property
    def groupId(self):
        """ID of the issue this event is grouped under."""
        return self.data['groupID']

    @cached_property
    def fingerprint(self):
        """
        Fingerprints are actually an array of values, but we want to
        treat it as a single hash-like value.
        """
        return ':'.join(self.data['fingerprints'])

    def get_entry(self, entry_type):
        for entry in self.data.get('entries', []):
            if entry['type'] == entry_type:
                return entry

        return None

    @cached_property
    def exception(self):
        try:
            exception = self.get_entry('exception')

            # Sometimes the data is in a values attribute? But not always.
            data = exception['data']
            if data.get('values', None):
                data = data['values'][0]

            return data
        except KeyError:
            return {}

    @cached_property
    def module(self):
        return self.exception.get('module', '')

    @cached_property
    def stack_frames(self):
        stacktrace = self.exception.get('stacktrace')
        if stacktrace:
            return stacktrace.get('frames', [])

        return []


def process_event(event):
    """
    Generate and save aggregated data for the given event to the
    database.
    """
    # Create issue, or update the last_seen date for it
    issue, created = Issue.objects.get_or_create(fingerprint=event.fingerprint, defaults={
        'message': event.message,
        'groupId': event.groupId,
        'last_seen': event.datetime_received,
        'module': event.module,
        'stack_frames': event.stack_frames,
    })
    if not created and issue.last_seen < event.datetime_received:
        issue.last_seen = event.datetime_received
        issue.save()

    # Increment the event count bucket
    issue.count_event(event_id=event.id, date=event.datetime_received.date())


def listen(
    sleep_delay,
    queue_backend,
    worker_message_count,
):
    """
    Listen for incoming events and process them.

    This is the entrypoint for worker processes.
    """
    logger = logging.getLogger('bec-alerts.processor.worker')
    logger.info('Waiting for an event')

    # Exit after worker_message_count events have been processed.
    messages_processed = 0
    while messages_processed < worker_message_count:
        try:
            for event_data in queue_backend.receive_events():
                event = SentryEvent(event_data)
                logger.debug(f'Received event ID: {event.id}')

                # The nested try avoids errors on a single event stopping us
                # from processing the rest of the received events.
                try:
                    process_event(event)
                    messages_processed += 1
                except Exception as err:
                    capture_exception(f'Error processing event: {event.id}')
        except Exception as err:
            capture_exception('Error receiving message')


@click.command()
@click.option(
    '--sleep-delay',
    default=20,
    envvar='PROCESSOR_SLEEP_DELAY',
)
@click.option(
    '--queue-name',
    default='sentry_errors',
    envvar='SQS_QUEUE_NAME',
)
@click.option(
    '--endpoint-url',
    envvar='SQS_ENDPOINT_URL',
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
    '--process_count',
    default=os.cpu_count(),
    envvar='PROCESSOR_PROCESS_COUNT',
)
@click.option(
    '--worker-message-count',
    default=200,
    envvar='PROCESSOR_WORKER_MESSAGE_COUNT',
)
@click.option(
    '--sentry-dsn',
    envvar='SENTRY_DSN',
)
def main(
    sleep_delay,
    queue_name,
    endpoint_url,
    connect_timeout,
    read_timeout,
    process_count,
    worker_message_count,
    sentry_dsn,
):
    """
    Listen for incoming events from Sentry and aggregate the data we
    care about from them.

    Manages a pool of subprocesses that perform the listening and
    processing.
    """
    initialize_error_reporting(sentry_dsn)
    logger = logging.getLogger('bec-alerts.processor')
    worker_ids = itertools.count()

    try:
        queue_backend = SQSQueueBackend(
            queue_name=queue_name,
            endpoint_url=endpoint_url,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
        )
    except Exception:
        capture_exception('Error initializing queue backend, will exit.')
        return

    logger.info('Starting processor workers')
    processes = []
    listen_kwargs = {
        'sleep_delay': sleep_delay,
        'queue_backend': queue_backend,
        'worker_message_count': worker_message_count,
    }
    for k in range(process_count):
        process = Process(target=listen, kwargs=listen_kwargs)
        process.name = f'worker-{next(worker_ids)}'
        processes.append(process)

    try:
        for process in processes:
            process.start()

        # Watch for terminated processes and replace them
        while True:
            for k, process in enumerate(processes):
                if not process.is_alive():
                    logger.info('Worker died, restarting process.')
                    processes[k] = Process(target=listen, kwargs=listen_kwargs)
                    processes[k].name = f'worker-{next(worker_ids)}'
                    processes[k].start()
                time.sleep(5)
    except KeyboardInterrupt:
        for process in processes:
            if process.is_alive():
                process.terminate()
    except Exception:
        capture_exception()
