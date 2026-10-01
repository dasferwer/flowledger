import uuid

from flowledger import db


def test_slot_reports_retained_wal_and_confirmation_lag():
    name = "test_status_" + uuid.uuid4().hex[:20]
    identity = int(uuid.uuid4().hex[:12], 16)
    with db.source() as control:
        control.execute("SELECT pg_create_logical_replication_slot(%s,'pgoutput')", (name,))
        try:
            with db.bootstrap_source() as writer:
                writer.execute(
                    "INSERT INTO public.items(id,payload) VALUES (%s,%s)",
                    (identity, "lag-check-" * 10000),
                )
            status = db.slot_status(name)
            assert status["exists"] is True
            assert status["slot"] == name
            assert status["active"] is False
            assert status["wal_status"] in {"reserved", "extended"}
            assert (
                status["restart_lsn"]
                and status["confirmed_flush_lsn"]
                and status["current_wal_lsn"]
            )
            assert status["wal_retained_bytes"] >= status["confirmation_lag_bytes"] > 0
        finally:
            control.execute("SELECT pg_drop_replication_slot(%s)", (name,))
            with db.bootstrap_source() as writer:
                writer.execute("DELETE FROM public.items WHERE id=%s", (identity,))
    assert db.slot_status(name) == {"exists": False, "slot": name}
