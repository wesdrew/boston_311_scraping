from opentelemetry.metrics import Counter, Meter, get_meter

_meter: Meter = get_meter("boston311.polling")

requests_ingested_counter: Counter = _meter.create_counter(
    name="polling.requests_ingested",
    description="Number of service requests ingested from the Boston 311 API",
    unit="{request}",
)
