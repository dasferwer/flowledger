import json

from flowledger.db import target

with target() as conn:
    state = conn.execute("SELECT * FROM state WHERE id=1").fetchone()
    if state:
        count = conn.execute(
            "SELECT count(*) AS n FROM projection WHERE generation=%s", (state["generation"],)
        ).fetchone()["n"]
        print(
            json.dumps(
                {
                    "generation": str(state["generation"]),
                    "status": state["status"],
                    "slot": state["slot"],
                    "published_lsn": str(state["published"]),
                    "applied_lsn": str(state["applied"]),
                    "rows": count,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        print("Издатель ещё не создал снимок.")
