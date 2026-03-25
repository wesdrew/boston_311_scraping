from consumer.consumer_lambda import collect_batch_failures, summarize_write_results
from db.service_request_dao import WriteResult
from shared.internal.internal_service_request import InternalServiceRequest

# -- collect_batch_failures ---------------------------------------------------


def test_returns_empty_failures_when_all_succeed():
    records = [
        ("msg-1", InternalServiceRequest(service_request_id="sr-1", status="open", event_hash="abc")),
        ("msg-2", InternalServiceRequest(service_request_id="sr-2", status="open", event_hash="def")),
    ]

    result = collect_batch_failures(records)

    assert result == {"batchItemFailures": []}


def test_returns_failure_for_none_record():
    records = [("msg-1", None)]

    result = collect_batch_failures(records)

    assert result == {"batchItemFailures": [{"itemIdentifier": "msg-1"}]}


def test_returns_only_failed_records():
    internal = InternalServiceRequest(service_request_id="sr-2", status="open", event_hash="abc")
    records = [
        ("msg-1", None),
        ("msg-2", internal),
        ("msg-3", None),
    ]

    result = collect_batch_failures(records)

    assert result == {
        "batchItemFailures": [
            {"itemIdentifier": "msg-1"},
            {"itemIdentifier": "msg-3"},
        ]
    }


def test_returns_all_failures_when_all_none():
    records = [("msg-1", None), ("msg-2", None), ("msg-3", None)]

    result = collect_batch_failures(records)

    assert len(result["batchItemFailures"]) == 3


# -- summarize_write_results --------------------------------------------------


def test_summarize_counts_insertions():
    results = [WriteResult.INSERTED, WriteResult.INSERTED, WriteResult.NO_CHANGE]

    summary = summarize_write_results(results)

    assert summary == {"rows_not_changed": 1, "rows_inserted": 2, "rows_updated": 0}


def test_summarize_counts_updates():
    results = [WriteResult.UPDATED, WriteResult.NO_CHANGE, WriteResult.NO_CHANGE]

    summary = summarize_write_results(results)

    assert summary == {"rows_not_changed": 2, "rows_inserted": 0, "rows_updated": 1}


def test_summarize_empty_list():
    assert summarize_write_results([]) == {"rows_not_changed": 0, "rows_inserted": 0, "rows_updated": 0}
