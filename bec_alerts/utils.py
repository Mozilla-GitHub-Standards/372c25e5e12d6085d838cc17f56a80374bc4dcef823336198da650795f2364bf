# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from datetime import datetime
from functools import lru_cache

import requests
from django.utils import timezone


@lru_cache(maxsize=2)
def latest_nightly_appbuildid():
    """
    Fetches the latest AppBuildID for Nightly from BuildHub. The return
    value is cached, and reset between watcher runs.

    See https://github.com/mozilla-services/buildhub/issues/431 for more
    info.
    """
    response = requests.post(
        'https://buildhub.prod.mozaws.net/v1/buckets/build-hub/collections/releases/search',
        json={
            "aggs": {
                "build_ids": {
                    "terms": {
                        "field": "build.id",
                        "size": 1,
                        "order": {
                            "_term": "desc",
                        },
                    },
                },
            },
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"target.channel": "nightly"}},
                        {"term": {"source.product": "firefox"}},
                        {"term": {"target.locale": "en-US"}},
                    ],
                },
            },
            "size": 0,
        },
    )
    response.raise_for_status()

    response_data = response.json()
    return response_data['aggregations']['build_ids']['buckets'][0]['key']


def aware_datetime(*args, **kwargs):
    new_date = datetime(*args, **kwargs)
    return timezone.make_aware(new_date, timezone=timezone.utc)
