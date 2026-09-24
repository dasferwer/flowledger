import json
import logging
import os
import select
import time
import uuid

import pika
import psycopg
import psycopg2
from psycopg import sql
from psycopg.types.json import Jsonb
from psycopg2.extras import LogicalReplicationConnection

from flowledger.db import init, schema, source, target
from flowledger.protocol import Decoder
from flowledger.queue import connect

logger = logging.getLogger(__name__)


class RebuildNeeded(RuntimeError):
    pass


def lsn_number(value):
    high, low = value.split("/")
    return (int(high, 16) << 32) + int(low, 16)


def bootstrap(control, replication, fingerprint, old):
    if old:
        slot = control.execute(
            "SELECT active FROM pg_replication_slots WHERE slot_name=%s", (old["slot"],)
        ).fetchone()
        if slot:
            if slot["active"]:
                raise RuntimeError("Предыдущий слот ещё занят")
            control.execute("SELECT pg_drop_replication_slot(%s)", (old["slot"],))
    generation = uuid.uuid4()
    slot = "flowledger_" + generation.hex[:20]
    control.execute("ALTER PUBLICATION flowledger_pub SET TABLE public.items")
    with target() as conn:
        conn.execute(
            """INSERT INTO state VALUES (1,%s,%s,'copying',%s,0,0)
            ON CONFLICT(id) DO UPDATE SET generation=EXCLUDED.generation,slot=EXCLUDED.slot,
            status=EXCLUDED.status,schema=EXCLUDED.schema,published=0,applied=0""",
            (generation, slot, Jsonb(fingerprint)),
        )
    cursor = replication.cursor()
    cursor.execute(f"CREATE_REPLICATION_SLOT {slot} LOGICAL pgoutput EXPORT_SNAPSHOT")
    _, consistent, snapshot, _ = cursor.fetchone()
    position = lsn_number(consistent)
    with target() as conn:
        conn.execute("UPDATE state SET published=%s,applied=%s WHERE id=1", (position, position))
    with psycopg.connect(os.environ["SOURCE_URL"]) as snapshot_conn:
        snapshot_conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        snapshot_conn.execute(sql.SQL("SET TRANSACTION SNAPSHOT {}").format(sql.Literal(snapshot)))
        snapshot_conn.execute("LOCK TABLE public.items IN ACCESS SHARE MODE")
        with snapshot_conn.cursor(name="snapshot_rows") as rows:
            rows.execute("SELECT id,row_to_json(items) FROM public.items ORDER BY id")
            while batch := rows.fetchmany(500):
                with target() as conn, conn.cursor() as out:
                    out.executemany(
                        "INSERT INTO projection VALUES (%s,%s,%s)",
                        [(generation, identity, Jsonb(data)) for identity, data in batch],
                    )
                time.sleep(float(os.environ.get("COPY_DELAY", "0")))
    if schema(control) != fingerprint:
        raise RebuildNeeded("Схема изменилась во время снимка")
    with target() as conn:
        conn.execute("UPDATE state SET status='ready' WHERE id=1 AND generation=%s", (generation,))
        return conn.execute("SELECT * FROM state WHERE id=1").fetchone()


def run():
    with source() as control:
        if not control.execute("SELECT pg_try_advisory_lock(390028) AS acquired").fetchone()[
            "acquired"
        ]:
            raise RuntimeError("Другой издатель уже работает")
        fingerprint = schema(control)
        with target() as conn:
            state = conn.execute("SELECT * FROM state WHERE id=1").fetchone()
        slot = (
            control.execute(
                "SELECT wal_status FROM pg_replication_slots WHERE slot_name=%s", (state["slot"],)
            ).fetchone()
            if state
            else None
        )
        replication = psycopg2.connect(
            os.environ["SOURCE_URL"], connection_factory=LogicalReplicationConnection
        )
        broker = None
        try:
            if (
                not state
                or state["status"] != "ready"
                or state["schema"] != fingerprint
                or not slot
                or slot["wal_status"] == "lost"
            ):
                state = bootstrap(control, replication, fingerprint, state)
            broker, channel = connect()
            channel.confirm_delivery()
            decoder = Decoder()
            cursor = replication.cursor()
            cursor.start_replication(
                slot_name=state["slot"],
                start_lsn=int(state["published"]),
                options={"proto_version": "1", "publication_names": "flowledger_pub"},
                decode=False,
                status_interval=1,
            )
            changes, size = [], 0
            last_check = time.monotonic()
            while True:
                if time.monotonic() - last_check >= 1:
                    if schema(control) != fingerprint:
                        raise RebuildNeeded("Схема изменилась: нужен новый снимок")
                    last_check = time.monotonic()
                    broker.process_data_events(time_limit=0)
                message = cursor.read_message()
                if message is None:
                    select.select([replication], [], [], 0.2)
                    continue
                decoded = decoder.parse(message.payload)
                kind = decoded["kind"]
                if kind == "begin":
                    changes, size = [], 0
                elif kind in {"upsert", "delete", "truncate"}:
                    changes.append(decoded)
                    size += len(message.payload)
                    if len(changes) > 10000 or size > 8 * 1024 * 1024:
                        raise ValueError("Транзакция превышает лимит: 10 000 изменений или 8 МиБ")
                elif kind == "commit":
                    position = decoded["lsn"]
                    if schema(control) != fingerprint:
                        raise RebuildNeeded("Схема изменилась до публикации транзакции")
                    if position > state["published"]:
                        payload = {
                            "generation": str(state["generation"]),
                            "lsn": position,
                            "changes": changes,
                        }
                        channel.basic_publish(
                            exchange="",
                            routing_key="changes",
                            body=json.dumps(payload),
                            properties=pika.BasicProperties(
                                delivery_mode=2, content_type="application/json"
                            ),
                            mandatory=True,
                        )
                        with target() as conn:
                            conn.execute(
                                "UPDATE state SET published=%s WHERE id=1 AND generation=%s",
                                (position, state["generation"]),
                            )
                        state["published"] = position
                    cursor.send_feedback(flush_lsn=position)
                    changes, size = [], 0
        finally:
            replication.close()
            if broker and broker.is_open:
                broker.close()


def main():
    logging.basicConfig(level=logging.WARNING)
    init()
    while True:
        try:
            run()
        except Exception:
            logger.exception("CDC остановлен; позиция неподтверждённых изменений сохранена")
        time.sleep(1)


if __name__ == "__main__":
    main()
