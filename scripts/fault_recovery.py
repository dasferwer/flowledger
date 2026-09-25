import json
import subprocess
import uuid

from flowledger.db import source, target
from flowledger.queue import connect
from scripts.recovery import docker, equal, state, wait


def arm(name):
    with target() as conn:
        conn.execute("INSERT INTO fault_points VALUES (%s) ON CONFLICT DO NOTHING", (name,))


def exited(service):
    result = subprocess.run(
        ["docker", "compose", "ps", "--status", "running", "--services"],
        check=True,
        capture_output=True,
        text=True,
    )
    return service not in result.stdout.splitlines()


def mutate():
    marker = uuid.uuid4().hex
    with source() as conn, conn.transaction():
        conn.execute("UPDATE items SET payload=%s,version=version+1 WHERE id=100", (marker,))
        conn.execute("UPDATE stores SET name=%s WHERE id=1", (marker,))


def rebuild():
    with target() as conn:
        conn.execute("UPDATE state SET rebuild_requested=true WHERE id=1")


def main():
    for service in ("publisher", "projector"):
        mode = subprocess.run(
            ["docker", "compose", "exec", "-T", service, "printenv", "FLOWLEDGER_FAULTS"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        if mode != "1":
            raise RuntimeError("Пересоздайте стенд с FLOWLEDGER_FAULTS=1")
    wait(equal)
    verified = []
    try:
        for point, service in [
            ("publisher_after_publish", "publisher"),
            ("publisher_after_checkpoint", "publisher"),
            ("projector_after_commit", "projector"),
        ]:
            arm(point)
            mutate()
            wait(lambda service=service: exited(service), seconds=30)
            docker("start", service)
            wait(equal)
            verified.append(point)
        generation = state()["generation"]
        arm("snapshot_after_chunk")
        rebuild()
        wait(lambda: exited("publisher"), seconds=30)
        partial = state()["generation"]
        assert partial != generation and state()["status"] == "copying"
        docker("start", "publisher")
        wait(lambda: state()["generation"] != partial and equal())
        verified.append("snapshot_after_chunk")
        docker("stop", "projector")
        mutate()
        wait(lambda: state()["published"] > state()["applied"])
        broker, channel = connect()
        try:
            channel.queue_purge("changes")
        finally:
            broker.close()
        assert not equal()
        previous = state()["generation"]
        rebuild()
        docker("start", "projector")
        wait(lambda: state()["generation"] != previous and equal())
        verified.append("broker_loss_explicit_rebuild")
        print(
            json.dumps(
                {"verified_boundaries": verified, "tables": 2, "full_reconciliation": True},
                indent=2,
            )
        )
    finally:
        with target() as conn:
            conn.execute("DELETE FROM fault_points")
        docker("start", "publisher", "projector")


if __name__ == "__main__":
    main()
