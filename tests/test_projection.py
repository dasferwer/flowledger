import os
import uuid

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb

from flowledger.db import init, target
from flowledger.projector import apply


@pytest.fixture
def generation(monkeypatch):
    base = os.environ["TARGET_URL"]
    schema = "test_" + uuid.uuid4().hex
    with psycopg.connect(base, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    monkeypatch.setenv("TARGET_URL", make_conninfo(base, options=f"-c search_path={schema}"))
    init()
    generation = uuid.uuid4()
    with target() as conn:
        conn.execute(
            "INSERT INTO state VALUES (1,%s,'unit','ready',%s,10,10)", (generation, Jsonb([]))
        )
    try:
        yield str(generation)
    finally:
        with psycopg.connect(base, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def message(generation, position, changes):
    return {"generation": generation, "lsn": position, "changes": changes}


def test_replay_partial_update_delete_and_old_generation(generation):
    first = message(generation, 20, [{"kind": "upsert", "data": {"id": 1, "payload": "first"}}])
    assert apply(first)
    assert not apply(first)
    assert not apply(message(str(uuid.uuid4()), 30, [{"kind": "truncate"}]))
    assert apply(message(generation, 30, [{"kind": "upsert", "data": {"id": 1, "version": 2}}]))
    with target() as conn:
        assert conn.execute("SELECT data FROM projection").fetchone()["data"] == {
            "id": 1,
            "payload": "first",
            "version": 2,
        }
    assert apply(message(generation, 40, [{"kind": "delete", "id": 1}]))
    with target() as conn:
        assert conn.execute("SELECT count(*) AS n FROM projection").fetchone()["n"] == 0


def test_transaction_and_checkpoint_rollback(generation):
    with pytest.raises(KeyError):
        apply(
            message(
                generation,
                20,
                [{"kind": "upsert", "data": {"id": 1}}, {"kind": "upsert", "data": {}}],
            )
        )
    with target() as conn:
        assert conn.execute("SELECT count(*) AS n FROM projection").fetchone()["n"] == 0
        assert conn.execute("SELECT applied FROM state").fetchone()["applied"] == 10


def test_primary_key_change_and_truncate(generation):
    apply(message(generation, 20, [{"kind": "upsert", "data": {"id": 1, "payload": "x"}}]))
    apply(message(generation, 30, [{"kind": "upsert", "data": {"id": 2}, "old_id": 1}]))
    with target() as conn:
        assert conn.execute("SELECT data FROM projection").fetchone()["data"] == {
            "id": 2,
            "payload": "x",
        }
    apply(message(generation, 40, [{"kind": "truncate"}]))
    with target() as conn:
        assert conn.execute("SELECT count(*) AS n FROM projection").fetchone()["n"] == 0
