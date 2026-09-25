from flowledger.db import target

with target() as conn:
    if not conn.execute(
        "UPDATE state SET rebuild_requested=true WHERE id=1 RETURNING id"
    ).fetchone():
        raise SystemExit("Начальный снимок ещё не создан")
print("Новый снимок запрошен; проверьте смену поколения и полную сверку.")
