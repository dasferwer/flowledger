import struct


class Reader:
    def __init__(self, data):
        self.data, self.position = data, 0

    def read(self, size):
        value = self.data[self.position : self.position + size]
        if len(value) != size:
            raise ValueError("Оборванное сообщение pgoutput")
        self.position += size
        return value

    def number(self, format):
        return struct.unpack("!" + format, self.read(struct.calcsize("!" + format)))[0]

    def string(self):
        end = self.data.find(b"\0", self.position)
        if end < 0:
            raise ValueError("Строка pgoutput без терминатора")
        return self.read(end - self.position + 1)[:-1].decode()


def convert(value, oid):
    if oid in {20, 21, 23}:
        return int(value)
    if oid == 16:
        return value == "t"
    if oid in {25, 1043}:
        return value
    raise ValueError("Неподдерживаемый тип колонки")


class Decoder:
    def __init__(self, allowed=None):
        self.relations = {}
        self.allowed = set(allowed or ["public.items"])
        self.names = {}

    def tuple(self, reader, columns):
        count = reader.number("H")
        if count != len(columns):
            raise ValueError("Число колонок не совпало с Relation")
        row = {}
        for name, oid in columns:
            kind = reader.read(1)
            if kind == b"n":
                row[name] = None
            elif kind == b"u":
                continue
            elif kind == b"t":
                row[name] = convert(reader.read(reader.number("I")).decode(), oid)
            else:
                raise ValueError("Поддерживается только текстовое представление значений")
        return row

    def parse(self, payload):
        reader = Reader(payload)
        kind = reader.read(1)
        if kind == b"B":
            return {"kind": "begin"}
        if kind == b"C":
            reader.read(1)
            reader.number("Q")
            return {"kind": "commit", "lsn": reader.number("Q")}
        if kind == b"R":
            identity = reader.number("I")
            namespace, name = reader.string(), reader.string()
            reader.read(1)
            columns = []
            for _ in range(reader.number("H")):
                reader.read(1)
                column = reader.string()
                oid = reader.number("I")
                reader.number("I")
                columns.append((column, oid))
            if namespace + "." + name not in self.allowed:
                raise ValueError("Публикация содержит неожиданную таблицу")
            self.relations[identity] = columns
            self.names[identity] = namespace + "." + name
            return {"kind": "relation"}
        if kind in {b"I", b"U", b"D"}:
            relation_id = reader.number("I")
            columns = self.relations[relation_id]
            relation = self.names[relation_id]
            marker = reader.read(1)
            old = None
            if marker in {b"K", b"O"}:
                old = self.tuple(reader, columns)
                if kind != b"D":
                    marker = reader.read(1)
            if kind == b"D":
                return {"kind": "delete", "id": old["id"], "relation": relation}
            if marker != b"N":
                raise ValueError("Нет нового значения строки")
            row = self.tuple(reader, columns)
            return {
                "kind": "upsert",
                "relation": relation,
                "data": {**(old or {}), **row},
                "old_id": old.get("id") if old else None,
            }
        if kind == b"T":
            count = reader.number("I")
            reader.read(1)
            truncated = []
            for _ in range(count):
                identity = reader.number("I")
                if identity not in self.relations:
                    raise ValueError("Неизвестная таблица TRUNCATE")
                truncated.append(self.names[identity])
            return {"kind": "truncate", "relations": truncated}
        raise ValueError("Неподдерживаемое сообщение pgoutput: " + repr(kind))
