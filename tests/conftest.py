import os
import uuid

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb

from flowledger.db import init, target


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
            "INSERT INTO state(id,generation,slot,status,schema,published,applied) VALUES (1,%s,'unit','ready',%s,10,10)",
            (generation, Jsonb([])),
        )
    try:
        yield str(generation)
    finally:
        with psycopg.connect(base, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
