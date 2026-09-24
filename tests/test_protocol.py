import struct

import pytest

from flowledger.protocol import Decoder
from flowledger.publisher import lsn_number


def relation():
    return (
        b"R"
        + struct.pack("!I", 1)
        + b"public\0items\0f"
        + struct.pack("!H", 2)
        + b"\x01id\0"
        + struct.pack("!II", 20, 0xFFFFFFFF)
        + b"\x00payload\0"
        + struct.pack("!II", 25, 0xFFFFFFFF)
    )


def values(identity, payload):
    fields = [str(identity).encode(), payload.encode()]
    return struct.pack("!H", 2) + b"".join(
        b"t" + struct.pack("!I", len(value)) + value for value in fields
    )


def test_insert_update_delete_and_unicode():
    decoder = Decoder()
    decoder.parse(relation())
    content = "Кавычка ' и путь \\"
    change = decoder.parse(b"I" + struct.pack("!I", 1) + b"N" + values(1, content))
    assert change["data"] == {"id": 1, "payload": content}
    changed = decoder.parse(
        b"U" + struct.pack("!I", 1) + b"O" + values(1, content) + b"N" + values(2, "new")
    )
    assert changed["old_id"] == 1 and changed["data"]["id"] == 2
    assert decoder.parse(b"D" + struct.pack("!I", 1) + b"O" + values(2, "new")) == {
        "kind": "delete",
        "id": 2,
    }


def test_commit_end_position_and_malformed_message():
    assert Decoder().parse(b"C" + struct.pack("!BQQQ", 0, 100, 120, 0)) == {
        "kind": "commit",
        "lsn": 120,
    }
    assert lsn_number("1/10") == 2**32 + 16
    with pytest.raises(ValueError):
        Decoder().parse(b"R")
    with pytest.raises(ValueError):
        Decoder().parse(b"?")


def test_primary_key_change_preserves_unchanged_toast():
    decoder = Decoder()
    decoder.parse(relation())
    new_tuple = struct.pack("!H", 2) + b"t" + struct.pack("!I", 1) + b"2u"
    message = b"U" + struct.pack("!I", 1) + b"O" + values(1, "large-text") + b"N" + new_tuple
    decoded = decoder.parse(message)
    assert decoded["data"] == {"id": 2, "payload": "large-text"}
