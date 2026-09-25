import os
import re

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
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='projection' AND column_name='relation') THEN
                    ALTER TABLE projection ADD COLUMN relation text NOT NULL DEFAULT 'public.items';
                    ALTER TABLE projection DROP CONSTRAINT projection_pkey;
                    ALTER TABLE projection ADD PRIMARY KEY(generation,relation,id);
                END IF;
            END $$;
            ALTER TABLE state ADD COLUMN IF NOT EXISTS rebuild_requested boolean NOT NULL DEFAULT false;
            CREATE TABLE IF NOT EXISTS fault_points (name text PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS publisher_health (
                id integer PRIMARY KEY CHECK(id=1),seen_at timestamptz NOT NULL DEFAULT now(),error text);
        """)


def tables():
    names = sorted(
        set(os.environ.get("FLOWLEDGER_TABLES", "public.items,public.stores").split(","))
    )
    if (
        not names
        or len(names) > 20
        or any(
            not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}\.[a-z_][a-z0-9_]{0,62}", name)
            for name in names
        )
    ):
        raise ValueError("Укажите от 1 до 20 таблиц в формате schema.table")
    return names


def schema(conn):
    result = {}
    for table in tables():
        rows = conn.execute(
            """SELECT a.attname AS name,a.atttypid AS oid,a.attrelid AS relation FROM pg_attribute a
            WHERE a.attrelid=%s::regclass AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum""",
            (table,),
        ).fetchall()
        if not any(r["name"] == "id" and r["oid"] == 20 for r in rows):
            raise ValueError("Нужен ключ id типа bigint в " + table)
        if any(row["oid"] not in {16, 20, 21, 23, 25, 1043} for row in rows):
            raise ValueError("Поддерживаются целые числа, bool и текст: " + table)
        key = conn.execute(
            """SELECT 1 FROM pg_constraint WHERE conrelid=%s::regclass AND contype='p'
            AND conkey=ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid=%s::regclass AND attname='id')]::smallint[]""",
            (table, table),
        ).fetchone()
        if not key:
            raise ValueError("Нужен первичный ключ только по id: " + table)
        result[table] = rows
    return result
