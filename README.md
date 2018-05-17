# Browser Error Collection Notifications

This is a prototype service that reads processed events from Sentry (exported via Amazon SQS) and evaluates several rules to determine if it should send email alerts.

## Development Setup

Prerequisites:

- Docker 18.03.0
- docker-compose 1.21.0

1. Clone the repository:

   ```sh
   git clone https://github.com/mozilla/bec-alerts.git
   cd bec-alerts
   ```
2. Build the Docker image:

   ```sh
   docker-compose build
   ```
3. Initialize Sentry and create an admin account (requires user input):

   ```sh
   docker-compose run sentry sentry upgrade
   ```
4. Initialize processor database:

   ```sh
   docker-compose run processor bec-alerts manage migrate
   ```
5. Start up services:

   ```sh
   docker-compose up
   ```
6. Visit http://localhost:9000 and finish Sentry setup via the web interface:

   - Use `http://localhost:9000` as the base URL of the install.
   - Create a new project. It's easiest to select "Python" as the project type.
   - Once you see the "Getting Started" page that includes a code sample of sending an error via Python, copy the DSN (which looks like `http://really_long_hash@localhost:9000/2`) to a `.env` file at the root of your repo that should look like this:

     ```
     SIMULATE_SENTRY_DSN=http://really_long_hash@sentry:9000/2
     ```

     __Important:__ You must change the `localhost` in the DSN to `sentry`, since it will be used in a Docker container where Sentry is not running on localhost.
   - After copying the DSN, go to the project settings for the project you just created. Select the All Integrations subsection, and click the link to configure the "Amazon SQS Standalone" plugin.
   - Enter the following config values:
     - __Endpoint URL:__ http://localstack:6000
     - __Queue URL:__ http://localstack:6000/queue/sentry_errors
     - __Region:__ (Anything besides the default value)
     - __Access Key:__ `asdf`
     - __Secret Key:__ `asdf`
   - Save the config and click the "Enable Plugin" button in the top right.

   After configuring the plugin and enabling it, you should be able to submit errors to Sentry and see logging confirming that the processor received the post-processed events from Sentry.

### Running the Processor

To run the processor and related services:

```sh
docker-compose up
```

### Simulating an Error

If you've set `SIMULATE_SENTRY_DSN` in your `.env` file properly, you can simulate sending an error to your running processor instance with the `simulate_error` command:

```sh
docker-compose run processor bec-alerts simulate_error
```

### Evaluating Triggers / Running Watcher

After you've processed an error, you can evaluate the configured alert triggers
with the watcher:

```sh
docker-compose run processor bec-alerts watcher --once --console-alerts --dry-run
```

- `--once` instructs the watcher to only run once; normally it will continue to run once every 5 minutes after it starts up.
- `--console-alerts` enables logging alerts instead of sending them as emails.
- `--dry-run` disables saving records of the run and other persistent data. This helps avoid issues with triggers that don't notify users of issues they've already seen.

You should see log output stating how many issues were evaluated, and messages
logged to the console if any alerts were triggered. It may be useful to set the
`enabled` property on the example triggers in `bec_alerts/triggers.py`, which
will trigger alerts whenever a new event is seen or when new events that the user has not seen before are seen.

### Running Tests

You can run the test suite using pytest in the processor container:

```sh
docker-compose run processor pytest /app
```

## Deployment

The service consists of a few AWS services and a Docker image that is intended to be run on EC2. The required AWS resources are:

- An SQS queue (named `sentry_errors` by default) that Sentry can write to and the app can read from. The app will create the queue on startup itself.
- An SES account for sending email notifications
- An RDS resource running a Postgres 10 database for storing aggregation data
  - Postgres is expected to have the `postgresql-hll` extension installed and enabled.

AWS credentials are pulled from IAM. The region is provided by the `AWS_DEFAULT_REGION` environment variable.

The Docker image defined in `Dockerfile` is used to run two separate processes: the processor, and the watcher, described in the following sections.

The following environment variables are available for all processes:

| Name | Required? | Default | Description |
| ---- | --------- | ------- | ----------- |
| `AWS_DEFAULT_REGION` | :white_check_mark: | | Region for connecting to AWS |
| `DATABASE_URL` | :white_check_mark: | | URL with connection data for the database. Typically a postgres URL of the form `postgres://user:password@host/database_name`. |
| `DJANGO_SECRET_KEY` | :white_check_mark: | | Secret key for Django's cryptographic signing. We don't really use it but Django fails to start without it. Set to a secret, random string. |
| `SENTRY_DSN` | :white_check_mark: | | DSN used to send errors to Sentry. |
| `AWS_CONNECT_TIMEOUT` | :x: | `30` | Timeout for connecting to AWS |
| `AWS_READ_TIMEOUT` | :x: | `30` | Timeout for reading a response from AWS |
| `LOG_FORMAT` | :x: | `mozlog` | Format to output logs in. Use `mozlog` for mozlog-compatible logs, or `compose` for a human-readable format suited for docker-compose output. |
| `LOG_LEVEL` | :x: | `INFO` | Minimum log level to output. One of `CRITICAL`, `ERROR`, `WARNING`, `INFO`, or `DEBUG`. |

### Processor

Command: `bec-alerts processor`

Reads from SQS to fetch incoming events from Sentry, processes them, and saves the resulting data to Postgres. This process will launch and manage a pool of subprocesses for listening and processing data.

The following environment variables are available for the processor:

| Name | Required? | Default | Description |
| ---- | --------- | ------- | ----------- |
| `SQS_QUEUE_NAME` | :x: | `sentry_errors` | Name of the queue to poll for events. |
| `SQS_ENDPOINT_URL` | :x: | | Endpoint URL for connection to AWS. Only required for local development. |
| `PROCESSOR_PROCESS_COUNT` | :x: | System CPU count | Number of worker processes to start |
| `PROCESSOR_WORKER_MESSAGE_COUNT` | :x: | `200` | Number of messages a worker should process before terminating; a new worker process is started to take its place. Workers may process slightly more messages than this number before shutting down due to message batching. |


### Watcher

Command: `bec-alerts watcher`

Periodically checks for events that have been processed since the last run, and evaluates alert triggers to determine if we should send a notification to users. This process implements its own sleep timer.

The following environment variables are available for the watcher:

| Name | Required? | Default | Description |
| ---- | --------- | ------- | ----------- |
| `SES_FROM_EMAIL` | :white_check_mark: | `notifications@sentry.prod.mozaws.net` | Email to use in the From field for notifications |
| `DATADOG_API_KEY` | :white_check_mark: | | API key for incrementing the healthcheck counter in Datadog via DogStatsD. |
| `DATADOG_COUNTER_NAME` | :x: | `bec-alerts.watcher.health` | Name of the DogStatsD counter to increment for health checks. |
| `SES_VERIFY_EMAIL` | :x: | `False` | If True, the watcher will attempt to verify the `SES_VERIFY_EMAIL` via API on startup. Should probably be False in production. |
| `WATCHER_SLEEP_DELAY` | :x: | `300` | Seconds to wait between checking for new events and alert triggers |
| `SES_ENDPOINT_URL` | :x: | | Endpoint URL for connection to AWS. Only required for local development. |

## Monitoring

- Each time the Watcher process evalutes triggers and potentially sends emails, it increments a counter in Datadog using DogStatsD. This behavior is controlled by the `DATADOG_API_KEY` and `DATADOG_COUNTER_NAME` environment variables.

  This counter can be used to monitor if the watcher is still running,

## License

Browser Error Collection Notifications is licensed under the MPL 2.0. See the `LICENSE` file for details.

The file `sentry/sqs_plugin.py` is a fork of an official Sentry plugin provided by Sentry. It is covered under the Apache License, version 2.0. See the file's comments for details.
