from enum import IntEnum

import pymysql
from shared.internal.internal_attribute import InternalAttribute
from shared.internal.internal_extended_attributes import InternalExtendedAttributes
from shared.internal.internal_service_request import InternalServiceRequest


class ServiceRequestSaveException(Exception):
    pass


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

_SERVICE_REQUEST_UPSERT_SQL = """
    INSERT INTO service_request (
        service_request_id, status, status_notes, service_name, service_code,
        description, requested_datetime, updated_datetime, expected_datetime,
        address, address_id, zipcode, latitude, longitude, event_hash
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
        latitude            = VALUES(latitude),
        longitude           = VALUES(longitude),
        event_hash          = VALUES(event_hash)
"""

_EXTENDED_ATTRIBUTES_UPSERT_SQL = """
    INSERT INTO extended_attributes (service_request_id, x, y, row_hash)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        x        = VALUES(x),
        y        = VALUES(y),
        row_hash = VALUES(row_hash)
"""


class WriteResult(IntEnum):
    NO_CHANGE = 0
    INSERTED = 1
    UPDATED = 2

    def __str__(self) -> str:
        return self.name.lower()


def _to_write_result(rowcount: int) -> WriteResult:
    if rowcount not in _VALID_ROWCOUNTS:
        raise ValueError(f"Unexpected rowcount: {rowcount}")
    return WriteResult(rowcount)


class ServiceRequestDAO:
    def __init__(self, conn: pymysql.connections.Connection) -> None:
        self._conn = conn

    def ping(self) -> None:
        self._conn.ping(reconnect=True)

    def get_service_request(self, service_request_id: str) -> dict | None:
        sql = """
            SELECT service_request_id, status, status_notes, service_name, service_code,
                   description, requested_datetime, updated_datetime, expected_datetime,
                   address, address_id, zipcode, latitude, longitude, event_hash, ingested_datetime
            FROM service_request
            WHERE service_request_id = %s
            LIMIT 1
        """
        with self._conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(sql, (service_request_id,))
            return cursor.fetchone()

    def _upsert_service_request(self, req: InternalServiceRequest) -> WriteResult:
        if not req.service_request_id:
            raise ValueError("service_request_id is required")

        with self._conn.cursor() as cursor:
            cursor.execute(
                _SERVICE_REQUEST_UPSERT_SQL,
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

    def _upsert_extended_attributes(
        self,
        service_request_id: str,
        ext: InternalExtendedAttributes,
    ) -> WriteResult:
        if not service_request_id:
            raise ValueError("service_request_id is required")

        x = str(ext.x) if ext.x is not None else None
        y = str(ext.y) if ext.y is not None else None

        sql = """
            INSERT INTO extended_attributes (service_request_id, x, y, row_hash)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                x        = VALUES(x),
                y        = VALUES(y),
                row_hash = VALUES(row_hash)
        """
        with self._conn.cursor() as cursor:
            cursor.execute(sql, (service_request_id, x, y, ext.row_hash))
            return _to_write_result(cursor.rowcount)

    def _upsert_attribute(
        self,
        service_request_id: str,
        attr: InternalAttribute,
    ) -> WriteResult:
        if not service_request_id:
            raise ValueError("service_request_id is required")

        with self._conn.cursor() as cursor:
            cursor.execute(
                _ATTRIBUTE_UPSERT_SQL,
                (service_request_id, attr.label, attr.value, attr.name, attr.code, attr.row_hash),
            )
            return _to_write_result(cursor.rowcount)

    def _upsert_one(self, req: InternalServiceRequest) -> WriteResult:
        result = self._upsert_service_request(req)

        if req.extended_attributes is not None:
            if isinstance(req.extended_attributes, list):
                if len(req.extended_attributes) > 1:
                    raise ValueError(f"expected at most 1 extended_attributes, got {len(req.extended_attributes)}")
                ext = req.extended_attributes[0]
            else:
                ext = req.extended_attributes
            self._upsert_extended_attributes(req.service_request_id, ext)

        if req.attributes:
            attr_params = [
                (req.service_request_id, attr.label, attr.value, attr.name, attr.code, attr.row_hash)
                for attr in req.attributes
            ]
            with self._conn.cursor() as cursor:
                cursor.executemany(_ATTRIBUTE_UPSERT_SQL, attr_params)

        return result

    def upsert_service_request(self, req: InternalServiceRequest) -> WriteResult:
        try:
            result = self._upsert_one(req)
            self._conn.commit()
        except Exception as e:
            self._conn.rollback()
            raise ServiceRequestSaveException(str(e)) from e
        return result

    def upsert_service_requests(self, reqs: list[InternalServiceRequest]) -> list[WriteResult]:
        if not reqs:
            return []
        try:
            ext_params = []
            for req in reqs:
                if req.extended_attributes is not None:
                    ext = (
                        req.extended_attributes[0]
                        if isinstance(req.extended_attributes, list)
                        else req.extended_attributes
                    )
                    x = str(ext.x) if ext.x is not None else None
                    y = str(ext.y) if ext.y is not None else None
                    ext_params.append((req.service_request_id, x, y, ext.row_hash))
            attr_params = [
                (req.service_request_id, attr.label, attr.value, attr.name, attr.code, attr.row_hash)
                for req in reqs
                for attr in (req.attributes or [])
            ]
            with self._conn.cursor() as cursor:
                results = []
                for req in reqs:
                    cursor.execute(
                        _SERVICE_REQUEST_UPSERT_SQL,
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
                    results.append(_to_write_result(cursor.rowcount))
                if ext_params:
                    cursor.executemany(_EXTENDED_ATTRIBUTES_UPSERT_SQL, ext_params)
                if attr_params:
                    cursor.executemany(_ATTRIBUTE_UPSERT_SQL, attr_params)
            self._conn.commit()
        except Exception as e:
            self._conn.rollback()
            raise ServiceRequestSaveException(str(e)) from e
        return results
