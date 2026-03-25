import json
import os

import boto3
import pymysql
import pymysql.connections
from shared.constants import DB_SECRET_ARN


def create_connection() -> pymysql.connections.Connection:
    secret_arn = os.environ[DB_SECRET_ARN]
    secrets_client = boto3.client("secretsmanager")
    secret = json.loads(secrets_client.get_secret_value(SecretId=secret_arn)["SecretString"])
    return pymysql.connect(
        host=secret["host"],
        port=int(secret["port"]),
        user=secret["username"],
        password=secret["password"],
        database=secret["dbname"],
        autocommit=False,
        cursorclass=pymysql.cursors.Cursor,
    )
