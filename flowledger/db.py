import os

import psycopg
from psycopg.rows import dict_row


def source():
    return psycopg.connect(os.environ["SOURCE_URL"], row_factory=dict_row, autocommit=True)


def target():
    return psycopg.connect(os.environ["TARGET_URL"], row_factory=dict_row)


def init():
    with target() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(390029)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS state (
                id integer PRIMARY KEY CHECK(id=1), generation uuid NOT NULL,
                slot text NOT NULL,status text NOT NULL,schema jsonb NOT NULL,
                published numeric(20) NOT NULL DEFAULT 0,applied numeric(20) NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS projection (
                generation uuid NOT NULL,id bigint NOT NULL,data jsonb NOT NULL,
                PRIMARY KEY(generation,id));
        """)


def schema(conn):
    rows = conn.execute("""SELECT a.attname AS name,a.atttypid AS oid,a.attrelid AS relation FROM pg_attribute a
        WHERE a.attrelid='public.items'::regclass AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum""").fetchall()
    if not any(r["name"] == "id" and r["oid"] == 20 for r in rows):
        raise ValueError("Нужен ключ id типа bigint")
    if any(row["oid"] not in {16, 20, 21, 23, 25, 1043} for row in rows):
        raise ValueError("Поддерживаются только целые числа, bool и текст")
    key = conn.execute("""SELECT 1 FROM pg_constraint WHERE conrelid='public.items'::regclass AND contype='p'
        AND conkey=ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid='public.items'::regclass AND attname='id')]::smallint[]""").fetchone()
    if not key:
        raise ValueError("Нужен первичный ключ только по id")
    return rows
