import shutil
import subprocess
import time
import uuid
from pathlib import Path

import pytest


@pytest.fixture
def legacy_source():
    if shutil.which("docker") is None:
        pytest.skip("Проверка обновления старой БД требует Docker")
    name = "flowledger-upgrade-test-" + uuid.uuid4().hex
    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--name",
            name,
            "--label",
            "flowledger.upgrade-test=true",
            "-e",
            "POSTGRES_USER=demo",
            "-e",
            "POSTGRES_PASSWORD=demo",
            "-e",
            "POSTGRES_DB=source",
            "postgres:17",
            "postgres",
            "-c",
            "wal_level=logical",
        ],
        check=True,
        capture_output=True,
    )

    def sql(statement):
        return subprocess.run(
            [
                "docker",
                "exec",
                "-i",
                name,
                "psql",
                "-X",
                "-At",
                "-v",
                "ON_ERROR_STOP=1",
                "-U",
                "demo",
                "-d",
                "source",
            ],
            input=statement,
            text=True,
            capture_output=True,
            check=False,
        )

    try:
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            ready = subprocess.run(
                [
                    "docker",
                    "exec",
                    name,
                    "pg_isready",
                    "-h",
                    "127.0.0.1",
                    "-U",
                    "demo",
                    "-d",
                    "source",
                ],
                capture_output=True,
                check=False,
            )
            if ready.returncode == 0 and sql("SELECT 1").returncode == 0:
                break
            time.sleep(0.2)
        else:
            pytest.fail("Временная PostgreSQL не запустилась")
        initial = sql("""
            CREATE TABLE public.items(id bigint PRIMARY KEY,payload text NOT NULL,
                                      version integer NOT NULL DEFAULT 1);
            ALTER TABLE public.items REPLICA IDENTITY FULL;
            CREATE PUBLICATION flowledger_pub FOR TABLE public.items;
            INSERT INTO public.items SELECT i,'initial-'||i,1 FROM generate_series(1,5000) i;
            UPDATE public.items SET payload='Сохранить старые данные', version=7 WHERE id=42;
        """)
        assert initial.returncode == 0, initial.stderr
        yield sql
    finally:
        subprocess.run(["docker", "rm", "-fv", name], check=True, capture_output=True)


def upgrade(sql):
    return sql((Path(__file__).resolve().parents[1] / "sql/source-roles.sql").read_text())


def rows_digest(sql):
    result = sql(
        "SELECT md5(string_agg(row_to_json(t)::text, E'\\n' ORDER BY id)) FROM public.items t"
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def test_upgrade_one_table_volume_preserves_rows_and_is_repeatable(legacy_source):
    sql = legacy_source
    before = rows_digest(sql)
    first = upgrade(sql)
    assert first.returncode == 0, first.stderr
    assert rows_digest(sql) == before
    assert (
        sql("SET ROLE flowledger_cdc; SELECT count(*) FROM public.items")
        .stdout.strip()
        .endswith("5000")
    )
    assert (
        sql("SET ROLE flowledger_cdc; SELECT count(*) FROM public.stores")
        .stdout.strip()
        .endswith("2")
    )
    assert (
        sql("SET ROLE flowledger_cdc; UPDATE public.items SET version=8 WHERE id=42").returncode
        != 0
    )
    assert sql("UPDATE public.stores SET name='Сохранить магазин' WHERE id=1").returncode == 0
    second = upgrade(sql)
    assert second.returncode == 0, second.stderr
    assert rows_digest(sql) == before
    assert sql("SELECT name FROM public.stores WHERE id=1").stdout.strip() == "Сохранить магазин"
    assert (
        sql(
            "SELECT string_agg(tablename,',' ORDER BY tablename) FROM pg_publication_tables WHERE pubname='flowledger_pub'"
        ).stdout.strip()
        == "items,stores"
    )


def test_failed_upgrade_rolls_back_role_acl_and_owner_changes(legacy_source):
    sql = legacy_source
    assert sql("DROP PUBLICATION flowledger_pub").returncode == 0
    before = rows_digest(sql)
    acl_query = "SELECT coalesce(datacl::text,'default') FROM pg_database WHERE datname='source'"
    before_acl = sql(acl_query).stdout.strip()
    result = upgrade(sql)
    assert result.returncode != 0
    assert rows_digest(sql) == before
    assert sql(acl_query).stdout.strip() == before_acl
    assert (
        sql(
            "SELECT tableowner FROM pg_tables WHERE schemaname='public' AND tablename='items'"
        ).stdout.strip()
        == "demo"
    )
    assert (
        sql(
            "SELECT count(*) FROM pg_roles WHERE rolname IN ('flowledger_owner','flowledger_cdc')"
        ).stdout.strip()
        == "0"
    )
    assert sql("SELECT to_regclass('public.stores') IS NULL").stdout.strip() == "t"
