import json
import subprocess
import time
import uuid

from psycopg import sql

from flowledger.db import source, tables, target
from scripts.migrate_demo import main as migrate


def docker(*args):
    subprocess.run(["docker", "compose", *args], check=True, capture_output=True)


def state():
    with target() as conn:
        return conn.execute("SELECT * FROM state WHERE id=1").fetchone()


def wait(predicate, seconds=120):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        result = predicate()
        if result:
            return result
        time.sleep(0.05)
    raise TimeoutError("CDC не достиг ожидаемого состояния")


def equal():
    original = {}
    with source() as conn:
        for table in tables():
            rows = conn.execute(
                sql.SQL("SELECT id,row_to_json(t) AS data FROM {} t").format(
                    sql.Identifier(*table.split("."))
                )
            ).fetchall()
            original.update({(table, r["id"]): r["data"] for r in rows})
    with target() as conn:
        current = conn.execute("SELECT * FROM state WHERE id=1").fetchone()
        if not current or current["status"] != "ready":
            return False
        projected = {
            (r["relation"], r["id"]): r["data"]
            for r in conn.execute(
                "SELECT relation,id,data FROM projection WHERE generation=%s",
                (current["generation"],),
            ).fetchall()
        }
    return original == projected


def remove_slot():
    current = state()
    if current:
        with source() as conn:
            slot = conn.execute(
                "SELECT slot_name FROM pg_replication_slots WHERE slot_name=%s", (current["slot"],)
            ).fetchone()
            if slot:
                conn.execute("SELECT pg_drop_replication_slot(%s)", (current["slot"],))


def main():
    migrate()
    docker("stop", "publisher", "projector")
    try:
        remove_slot()
        previous = state()
        docker("start", "publisher", "projector")
        wait(
            lambda: (
                (s := state())
                and s["status"] == "copying"
                and (not previous or s["generation"] != previous["generation"])
            )
        )
        with source() as conn:
            conn.execute(
                "UPDATE items SET payload=%s,version=version+1 WHERE id<=200",
                ("Во время снимка: 'цитата' и \\ путь",),
            )
            conn.execute("DELETE FROM items WHERE id BETWEEN 4900 AND 4910")
            conn.execute(
                "INSERT INTO items(id,payload,version) VALUES (10001,'new',1) ON CONFLICT(id) DO UPDATE SET version=items.version+1"
            )
        with source() as conn:
            conn.execute("DELETE FROM items WHERE id=900001")
            conn.execute(
                "INSERT INTO items(id,payload,version) VALUES (1,%s,1) ON CONFLICT(id) DO UPDATE SET payload=EXCLUDED.payload",
                ("Большое значение " * 1000,),
            )
            conn.execute("UPDATE items SET id=900001 WHERE id=1")
            conn.execute("UPDATE items SET version=version+1 WHERE id=900001")
        wait(equal)
        first_generation = state()["generation"]
        docker("stop", "projector")
        with source() as conn:
            conn.execute(
                "UPDATE items SET payload='consumer-restart',version=version+1 WHERE id BETWEEN 201 AND 230"
            )
        wait(lambda: state()["published"] > state()["applied"])
        docker("start", "projector")
        wait(equal)
        docker("kill", "-s", "SIGKILL", "publisher")
        with source() as conn:
            conn.execute(
                "UPDATE items SET payload='publisher-restart',version=version+1 WHERE id BETWEEN 231 AND 250"
            )
        docker("start", "publisher")
        wait(equal)
        docker("stop", "publisher")
        remove_slot()
        with source() as conn:
            conn.execute("DELETE FROM items WHERE id=251")
        docker("start", "publisher")
        wait(lambda: state()["generation"] != first_generation and equal())
        before_schema = state()["generation"]
        column = "note_" + uuid.uuid4().hex[:6]
        with source() as conn:
            conn.execute(
                sql.SQL("ALTER TABLE items ADD COLUMN {} text DEFAULT 'added'").format(
                    sql.Identifier(column)
                )
            )
        wait(lambda: state()["generation"] != before_schema and equal())
        with source() as conn:
            count = conn.execute("SELECT count(*) AS n FROM items").fetchone()["n"]
        print(
            json.dumps(
                {
                    "source_rows": count,
                    "full_reconciliation": True,
                    "writes_during_snapshot": True,
                    "publisher_restart": True,
                    "projector_restart": True,
                    "lost_slot_rebuilt": True,
                    "added_column_rebuilt": True,
                },
                indent=2,
            )
        )
    finally:
        docker("start", "publisher", "projector")


if __name__ == "__main__":
    main()
