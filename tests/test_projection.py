import uuid

import pytest

from flowledger.db import target
from flowledger.projector import apply


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


def test_same_primary_key_in_two_tables_and_scoped_truncate(generation):
    assert apply(
        message(
            generation,
            20,
            [
                {
                    "kind": "upsert",
                    "relation": "public.items",
                    "data": {"id": 1, "payload": "item"},
                },
                {"kind": "upsert", "relation": "public.stores", "data": {"id": 1, "name": "store"}},
            ],
        )
    )
    with target() as conn:
        assert conn.execute("SELECT count(*) AS n FROM projection").fetchone()["n"] == 2
    assert apply(message(generation, 30, [{"kind": "truncate", "relations": ["public.stores"]}]))
    with target() as conn:
        rows = conn.execute("SELECT relation,data FROM projection").fetchall()
        assert len(rows) == 1
        assert rows[0]["relation"] == "public.items"


def test_cross_table_transaction_and_position_roll_back_together(generation):
    with pytest.raises(KeyError):
        apply(
            message(
                generation,
                20,
                [
                    {"kind": "upsert", "relation": "public.items", "data": {"id": 1}},
                    {"kind": "upsert", "relation": "public.stores", "data": {}},
                ],
            )
        )
    with target() as conn:
        assert conn.execute("SELECT count(*) AS n FROM projection").fetchone()["n"] == 0
        assert conn.execute("SELECT applied FROM state").fetchone()["applied"] == 10
