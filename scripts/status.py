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
                    "tables": conn.execute(
                        "SELECT relation,count(*) AS rows FROM projection WHERE generation=%s GROUP BY relation ORDER BY relation",
                        (state["generation"],),
                    ).fetchall(),
                    "publisher": conn.execute(
                        "SELECT seen_at::text,error,clock_timestamp()-seen_at<interval '5 seconds' AS recent FROM publisher_health WHERE id=1"
                    ).fetchone(),
                    "rebuild_requested": state["rebuild_requested"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        print("Издатель ещё не создал снимок.")
