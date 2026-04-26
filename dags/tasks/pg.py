from __future__ import annotations

from tasks.config import PG_DB, PG_HOST, PG_PASSWORD, PG_PORT, PG_USER


def pg_execute(sql: str, fetch: bool = False):
    import psycopg2
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                            user=PG_USER, password=PG_PASSWORD)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(sql)
    result = cur.fetchall() if fetch else None
    cur.close()
    conn.close()
    return result
