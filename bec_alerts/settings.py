# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import os
from pathlib import Path

import dj_database_url


BASE_DIR = Path(__file__).joinpath('..', '..').resolve()

DATABASES = {
    'default': dj_database_url.config(),
}

USE_TZ = True

INSTALLED_APPS = (
    'bec_alerts',
)

SECRET_KEY = os.environ['DJANGO_SECRET_KEY']

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.jinja2.Jinja2',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {},
    },
]
