# Boston 311 Scraping

This project polls the Boston 311 Open311 API every 10 minutes, enqueues service requests to SQS, and publishes lifecycle events to SNS — building an event stream for real-time visualization of Boston's 311 data.

## Architecture

### Workspace Packages

| Package | Description |
|---|---|
| `polling` | Lambda function that polls the Boston 311 API on a cron trigger |
| `shared` | Lambda layer with shared Pydantic models (DTOs) |
| `infra` | AWS CDK infrastructure written in Python |

### Data Flow

```
EventBridge (cron) → Lambda → Boston 311 Open311 API
                                       ↓
                              SQS Queue (service requests)
                              SNS Topic (execution summaries / lifecycle events)
                                       ↓
                              DLQ (failed messages)
```

- **Observability**: OpenTelemetry + AWS X-Ray tracing

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Node.js + `npm install -g aws-cdk`
- Docker (for CDK Python bundling)
- AWS credentials configured

## Local Setup

```bash
uv sync --frozen --all-groups --all-packages
```

## Testing

```bash
uv run pytest -m "not integration"   # unit tests
uv run pytest -m "integration"       # integration tests (uses moto)
```

## Lint & Type Check

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy polling/src shared/src
```

## Deployment

```bash
cdk synth --app "./synth.sh"
cdk deploy Boston311Polling        # prod
cdk deploy Boston311Polling-dev    # dev
```

## Configuration

Environment variables (infrastructure-managed values are set automatically by CDK):

| Variable | Description | Default |
|---|---|---|
| `SERVICE_REQUESTS_QUEUE_URL` | SQS queue URL | set by CDK |
| `APP_EVENTS_TOPIC_ARN` | SNS topic ARN | set by CDK |
| `POLLING_LOOKBACK_MINUTES` | How far back to poll (minutes) | `20` |
