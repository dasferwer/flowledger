import json
import logging
import time
import uuid

from psycopg.types.json import Jsonb

from flowledger.db import init, target
from flowledger.queue import connect

logger = logging.getLogger(__name__)


def apply(message):
    with target() as conn:
        state = conn.execute("SELECT * FROM state WHERE id=1 FOR UPDATE").fetchone()
        if not state or str(state["generation"]) != message["generation"]:
            return False
        if state["status"] != "ready":
            raise RuntimeError("Снимок ещё не готов")
        if message["lsn"] <= state["applied"]:
            return False
        generation = uuid.UUID(message["generation"])
        for change in message["changes"]:
            if change["kind"] == "truncate":
                conn.execute("DELETE FROM projection WHERE generation=%s", (generation,))
            elif change["kind"] == "delete":
                conn.execute(
                    "DELETE FROM projection WHERE generation=%s AND id=%s",
                    (generation, change["id"]),
                )
            else:
                row = change["data"]
                if change.get("old_id") is not None and change["old_id"] != row["id"]:
                    previous = conn.execute(
                        "SELECT data FROM projection WHERE generation=%s AND id=%s",
                        (generation, change["old_id"]),
                    ).fetchone()
                    if previous:
                        row = {**previous["data"], **row}
                    conn.execute(
                        "DELETE FROM projection WHERE generation=%s AND id=%s",
                        (generation, change["old_id"]),
                    )
                conn.execute(
                    """INSERT INTO projection VALUES (%s,%s,%s) ON CONFLICT(generation,id)
                    DO UPDATE SET data=projection.data || EXCLUDED.data""",
                    (generation, row["id"], Jsonb(row)),
                )
        conn.execute("UPDATE state SET applied=%s WHERE id=1", (message["lsn"],))
        return True


def main():
    logging.basicConfig(level=logging.WARNING)
    init()
    while True:
        connection = None
        try:
            connection, channel = connect()
            channel.basic_qos(prefetch_count=1)
            for method, properties, body in channel.consume("changes", inactivity_timeout=1):
                if method:
                    apply(json.loads(body))
                    channel.basic_ack(method.delivery_tag)
        except Exception:
            logger.exception("Проекция не обновлена; сообщение остаётся неподтверждённым")
        finally:
            if connection and connection.is_open:
                connection.close()
        time.sleep(1)


if __name__ == "__main__":
    main()
