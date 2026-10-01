import os

import psycopg2
import pytest
from psycopg2.extras import LogicalReplicationConnection

from flowledger.db import schema, source, target
from flowledger.publisher import bootstrap


def test_snapshot_and_repeat_rebuild_work_with_runtime_login(generation):
    with source() as control:
        replication = psycopg2.connect(
            os.environ["SOURCE_URL"], connection_factory=LogicalReplicationConnection
        )
        try:
            fingerprint = schema(control)
            first = bootstrap(control, replication, fingerprint, None)
            replication.close()
            replication = psycopg2.connect(
                os.environ["SOURCE_URL"], connection_factory=LogicalReplicationConnection
            )
            second = bootstrap(control, replication, fingerprint, first)
            assert first["generation"] != second["generation"]
            assert second["status"] == "ready"
            with target() as conn:
                count = conn.execute(
                    "SELECT count(*) AS n FROM projection WHERE generation=%s",
                    (second["generation"],),
                ).fetchone()["n"]
            expected = sum(
                control.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"]
                for table in ("public.items", "public.stores")
            )
            assert count == expected
        finally:
            replication.close()
            with target() as conn:
                current = conn.execute("SELECT slot FROM state WHERE id=1").fetchone()
            if current and current["slot"].startswith("flowledger_"):
                control.execute("SELECT pg_drop_replication_slot(%s)", (current["slot"],))


def test_unconfigured_publication_fails_before_creating_snapshot(generation):
    with source() as control:
        replication = psycopg2.connect(
            os.environ["SOURCE_URL"], connection_factory=LogicalReplicationConnection
        )
        try:
            fingerprint = schema(control)
            fingerprint.pop("public.stores")
            with pytest.raises(ValueError, match="publication"):
                bootstrap(control, replication, fingerprint, None)
            with target() as conn:
                assert (
                    conn.execute("SELECT slot FROM state WHERE id=1").fetchone()["slot"] == "unit"
                )
        finally:
            replication.close()
