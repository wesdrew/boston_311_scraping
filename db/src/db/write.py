import hashlib
from enum import IntEnum

import pymysql
from shared.internal.internal_attribute import InternalAttribute
from shared.internal.internal_extended_attributes import InternalExtendedAttributes
from shared.internal.internal_service_request import InternalServiceRequest

_VALID_ROWCOUNTS = frozenset({0, 1, 2})

_ATTRIBUTE_UPSERT_SQL = """
    INSERT INTO attribute (service_request_id, label, value, name, code, row_hash)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        label    = VALUES(label),
        value    = VALUES(value),
        name     = VALUES(name),
        code     = VALUES(code),
        row_hash = VALUES(row_hash)
"""


class WriteResult(IntEnum):
    NO_CHANGE = 0
    INSERTED = 1
    UPDATED = 2


def _hash(*parts: str | None) -> str:
    value = "|".join(p or "" for p in parts)
    return hashlib.sha256(value.encode()).hexdigest()


def _to_write_result(rowcount: int) -> WriteResult:
    if rowcount not in _VALID_ROWCOUNTS:
        raise ValueError(f"Unexpected rowcount: {rowcount}")
    return WriteResult(rowcount)


def upsert_service_request(conn: pymysql.connections.Connection, req: InternalServiceRequest) -> WriteResult:
    if not req.service_request_id:
        raise ValueError("service_request_id is required")

    sql = """
        INSERT INTO service_request (
            service_request_id, status, status_notes, service_name, service_code,
            description, requested_datetime, updated_datetime, expected_datetime,
            address, address_id, zipcode, lat, `long`, event_hash
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            status              = VALUES(status),
            status_notes        = VALUES(status_notes),
            service_name        = VALUES(service_name),
            service_code        = VALUES(service_code),
            description         = VALUES(description),
            requested_datetime  = VALUES(requested_datetime),
            updated_datetime    = VALUES(updated_datetime),
            expected_datetime   = VALUES(expected_datetime),
            address             = VALUES(address),
            address_id          = VALUES(address_id),
            zipcode             = VALUES(zipcode),
            lat                 = VALUES(lat),
            `long`              = VALUES(`long`),
            event_hash          = VALUES(event_hash)
    """
    with conn.cursor() as cursor:
        cursor.execute(
            sql,
            (
                req.service_request_id,
                req.status,
                req.status_notes,
                req.service_name,
                req.service_code,
                req.description,
                req.requested_datetime,
                req.updated_datetime,
                req.expected_datetime,
                req.address,
                req.address_id,
                req.zipcode,
                req.lat,
                req.long,
                req.event_hash,
            ),
        )
        return _to_write_result(cursor.rowcount)


def upsert_extended_attributes(
    conn: pymysql.connections.Connection,
    service_request_id: str,
    ext: InternalExtendedAttributes,
) -> WriteResult:
    if not service_request_id:
        raise ValueError("service_request_id is required")

    x = str(ext.x) if ext.x is not None else None
    y = str(ext.y) if ext.y is not None else None
    row_hash = _hash(service_request_id, x, y)

    sql = """
        INSERT INTO extended_attributes (service_request_id, x, y, row_hash)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            x        = VALUES(x),
            y        = VALUES(y),
            row_hash = VALUES(row_hash)
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (service_request_id, x, y, row_hash))
        return _to_write_result(cursor.rowcount)


def upsert_attribute(
    conn: pymysql.connections.Connection,
    service_request_id: str,
    attr: InternalAttribute,
) -> WriteResult:
    if not service_request_id:
        raise ValueError("service_request_id is required")

    row_hash = _hash(service_request_id, attr.label, attr.name, attr.code, attr.value)

    with conn.cursor() as cursor:
        cursor.execute(
            _ATTRIBUTE_UPSERT_SQL, (service_request_id, attr.label, attr.value, attr.name, attr.code, row_hash)
        )
        return _to_write_result(cursor.rowcount)


def upsert_service_request_with_children(
    conn: pymysql.connections.Connection,
    req: InternalServiceRequest,
) -> WriteResult:
    try:
        result = upsert_service_request(conn, req)

        if req.extended_attributes is not None:
            if isinstance(req.extended_attributes, list):
                if len(req.extended_attributes) > 1:
                    raise ValueError(f"expected at most 1 extended_attributes, got {len(req.extended_attributes)}")
                ext = req.extended_attributes[0]
            else:
                ext = req.extended_attributes
            upsert_extended_attributes(conn, req.service_request_id, ext)

        if req.attributes:
            attr_params = [
                (
                    req.service_request_id,
                    attr.label,
                    attr.value,
                    attr.name,
                    attr.code,
                    _hash(req.service_request_id, attr.label, attr.name, attr.code, attr.value),
                )
                for attr in req.attributes
            ]
            with conn.cursor() as cursor:
                cursor.executemany(_ATTRIBUTE_UPSERT_SQL, attr_params)

        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return result
