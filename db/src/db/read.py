import pymysql


def get_service_request(conn: pymysql.connections.Connection, service_request_id: str) -> dict | None:
    sql = """
        SELECT service_request_id, status, status_notes, service_name, service_code,
               description, requested_datetime, updated_datetime, expected_datetime,
               address, address_id, zipcode, lat, `long`, event_hash, ingested_datetime
        FROM service_request
        WHERE service_request_id = %s
        LIMIT 1
    """
    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute(sql, (service_request_id,))
        return cursor.fetchone()
