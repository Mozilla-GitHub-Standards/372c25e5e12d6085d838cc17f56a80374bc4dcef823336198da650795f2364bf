# Sentry Docker Image

This is a custom Sentry docker image with the following modifications:

- A fork of the Amazon SQS plugin from [sentry-plugins][] that adds an extra `endpoint_url` config option is installed as the "Amazon SQS Standalone" plugin. This extra config option is required to correctly send events to the fake SQS queue provided by [localstack][].

[sentry-plugins]: https://github.com/getsentry/sentry-plugins
[localstack]: https://hub.docker.com/r/localstack/localstack/
