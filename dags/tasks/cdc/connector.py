from __future__ import annotations

import json
import logging
import urllib.request
import urllib.error

from tasks.config import CONNECT_URL, PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD

log = logging.getLogger(__name__)

CONNECTOR_NAME = "pg-cdc-connector"

CONNECTOR_CONFIG = {
    "name": CONNECTOR_NAME,
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": PG_HOST,
        "database.port": str(PG_PORT),
        "database.user": PG_USER,
        "database.password": PG_PASSWORD,
        "database.dbname": PG_DB,
        "topic.prefix": "dbserver1",
        "plugin.name": "pgoutput",
        "schema.include.list": "public",
        "table.include.list": "public.customers,public.drivers",
    },
}


def ensure_connector(**ctx):
    """Create the Debezium CDC connector if it doesn't already exist."""
    url = f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}"
    req = urllib.request.Request(url)
    try:
        urllib.request.urlopen(req)
        log.info("Connector %s already exists", CONNECTOR_NAME)
        return
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise

    log.info("Connector %s not found — creating", CONNECTOR_NAME)
    create_req = urllib.request.Request(
        f"{CONNECT_URL}/connectors",
        data=json.dumps(CONNECTOR_CONFIG).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    resp = urllib.request.urlopen(create_req)
    log.info("Connector created: %s", resp.read().decode())
