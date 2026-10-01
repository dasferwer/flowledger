import pytest
from psycopg.errors import InsufficientPrivilege

from flowledger.db import source


def test_runtime_replication_login_has_no_superuser_or_role_management():
    with source() as conn:
        role = conn.execute(
            "SELECT rolsuper,rolreplication,rolcreatedb,rolcreaterole,rolbypassrls FROM pg_roles WHERE rolname=current_user"
        ).fetchone()
        assert role == {
            "rolsuper": False,
            "rolreplication": True,
            "rolcreatedb": False,
            "rolcreaterole": False,
            "rolbypassrls": False,
        }
        assert conn.execute("SELECT count(*) AS n FROM public.items").fetchone()["n"] > 0
        assert conn.execute("SELECT count(*) AS n FROM public.stores").fetchone()["n"] > 0


@pytest.mark.parametrize(
    "statement",
    [
        "INSERT INTO public.items(id,payload) VALUES (999999999,'forbidden')",
        "UPDATE public.items SET payload='forbidden' WHERE id=100",
        "DELETE FROM public.items WHERE id=100",
        "TRUNCATE public.stores",
        "CREATE TABLE public.forbidden(id bigint)",
        "ALTER TABLE public.items ADD COLUMN forbidden text",
        "ALTER PUBLICATION flowledger_pub SET TABLE public.items",
        "SET ROLE demo",
    ],
)
def test_runtime_cannot_write_or_administer_source(statement):
    with source() as conn:
        try:
            with pytest.raises(InsufficientPrivilege):
                conn.execute("BEGIN")
                conn.execute(statement)
        finally:
            conn.execute("ROLLBACK")
