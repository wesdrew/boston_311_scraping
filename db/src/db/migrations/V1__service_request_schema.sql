CREATE TABLE IF NOT EXISTS service_request (
    rowid               INT AUTO_INCREMENT PRIMARY KEY,
    service_request_id  VARCHAR(64)  NOT NULL,
    status              VARCHAR(64)  NOT NULL,
    status_notes        TEXT         NULL,
    service_name        VARCHAR(255) NULL,
    service_code        VARCHAR(64)  NULL,
    description         TEXT         NULL,
    requested_datetime  DATETIME     NULL,
    updated_datetime    DATETIME     NULL,
    expected_datetime   DATETIME     NULL,
    address             TEXT         NULL,
    address_id          VARCHAR(64)  NULL,
    zipcode             VARCHAR(16)  NULL,
    latitude            FLOAT        NULL,
    longitude           FLOAT        NULL,
    event_hash          VARCHAR(64)  NOT NULL,
    ingested_datetime   DATETIME     NOT NULL DEFAULT (UTC_TIMESTAMP()),
    UNIQUE KEY uq_service_request_id (service_request_id),
    KEY idx_service_request_status (status),
    KEY idx_service_request_updated_datetime (updated_datetime),
    KEY idx_service_request_requested_datetime (requested_datetime)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS extended_attributes (
    rowid               INT AUTO_INCREMENT PRIMARY KEY,
    service_request_id  VARCHAR(64)  NOT NULL,
    x                   VARCHAR(64)  NULL,
    y                   VARCHAR(64)  NULL,
    row_hash            VARCHAR(64)  NOT NULL,
    ingested_datetime   DATETIME     NOT NULL DEFAULT (UTC_TIMESTAMP()),
    UNIQUE KEY uq_extended_attributes_service_request_id (service_request_id),
    UNIQUE KEY uq_extended_attributes_row_hash (row_hash),
    CONSTRAINT fk_extended_attributes_service_request
        FOREIGN KEY (service_request_id)
        REFERENCES service_request (service_request_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS attribute (
    rowid               INT AUTO_INCREMENT PRIMARY KEY,
    service_request_id  VARCHAR(64)  NOT NULL,
    label               VARCHAR(255) NULL,
    value               TEXT         NULL,
    name                VARCHAR(255) NULL,
    code                VARCHAR(64)  NULL,
    row_hash            VARCHAR(64)  NOT NULL,
    ingested_datetime   DATETIME     NOT NULL DEFAULT (UTC_TIMESTAMP()),
    UNIQUE KEY uq_attribute_row_hash (row_hash),
    KEY idx_attribute_service_request_id (service_request_id),
    CONSTRAINT fk_attribute_service_request
        FOREIGN KEY (service_request_id)
        REFERENCES service_request (service_request_id)
) ENGINE=InnoDB;
