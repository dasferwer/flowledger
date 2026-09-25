import os

from flowledger.db import target


def hit(name):
    """Одноразовый аварийный выход доступен только явно включённому испытательному стенду."""
    if os.environ.get("FLOWLEDGER_FAULTS") != "1":
        return
    with target() as conn:
        armed = conn.execute(
            "DELETE FROM fault_points WHERE name=%s RETURNING name", (name,)
        ).fetchone()
    if armed:
        os._exit(91)
