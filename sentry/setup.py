#!/usr/bin/env python
from setuptools import setup, find_packages

install_requires = [
    'sentry>=5.3.3',
    'sentry-plugins>=8.20.0',
    'boto3>=1.4.4,<1.5.0',
]

setup(
    name='sentry-amazon-sqs-standalone',
    version='0.0.1',
    author='Michael Kelly',
    author_email='mkelly@mozilla.com',
    url='http://github.com/Osmose/sentry-amazon-sqs-standalone',
    description='A standalone version of the official SQS plugin',
    long_description='',
    license='MPL',
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    install_requires=install_requires,
    entry_points={
        'sentry.apps': [
            'amazon_sqs_standalone = sqs_plugin',
        ],
        'sentry.plugins': [
            'amazon_sqs_standalone = sqs_plugin:AmazonSQSPlugin'
        ],
    },
    include_package_data=True,
    zip_safe=False,
)
