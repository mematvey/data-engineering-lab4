import csv
import io
from collections import Counter
from itertools import combinations

from common import BUCKET, CSV_KEY, ROLES, is_variant, parse_skills

CHUNK_BYTES = 16 << 20
OVERLAP = 1 << 20
ROLE_BYTES = tuple(role.encode("utf-8") for role in ROLES)

csv.field_size_limit(10 ** 8)


def object_size(s3):
    return s3.head_object(Bucket=BUCKET, Key=CSV_KEY)["ContentLength"]


def read_header(s3):
    body = s3.get_object(Bucket=BUCKET, Key=CSV_KEY, Range="bytes=0-65535")["Body"].read()
    line = body.split(b"\n")[0].decode("utf-8").rstrip("\r")
    columns = next(csv.reader([line]))
    return {"role": columns.index("role"), "name": columns.index("name"), "skills": columns.index("key_skills")}


def split_chunks(size):
    chunks = []
    start = 0
    while start < size:
        end = min(start + CHUNK_BYTES, size)
        chunks.append((len(chunks), start, end))
        start = end
    return chunks


def stream_lines(s3, size, start, end):
    first = max(start - 1, 0)
    last = min(end + OVERLAP, size)
    body = s3.get_object(Bucket=BUCKET, Key=CSV_KEY, Range=f"bytes={first}-{last - 1}")["Body"]
    pos = first
    skip_head = start > 0
    pending = b""
    try:
        for piece in body.iter_chunks(1 << 20):
            parts = (pending + piece).split(b"\n")
            pending = parts.pop()
            for part in parts:
                if skip_head:
                    skip_head = False
                    pos += len(part) + 1
                    continue
                if pos >= end:
                    return
                pos += len(part) + 1
                yield part.rstrip(b"\r")
        if pending and not skip_head and pos < end:
            if last < size:
                raise RuntimeError("строка длиннее запаса OVERLAP")
            yield pending.rstrip(b"\r")
    finally:
        body.close()


def map_chunk(s3, columns, size, start, end):
    pairs = Counter()
    stat = Counter()

    for number, raw in enumerate(stream_lines(s3, size, start, end)):
        if start == 0 and number == 0:
            continue

        stat["rows"] += 1
        if not any(role in raw for role in ROLE_BYTES):
            continue

        row = next(csv.reader([raw.decode("utf-8")]))
        if not is_variant(row[columns["role"]], row[columns["name"]]):
            continue
        stat["variant"] += 1

        skills = parse_skills(row[columns["skills"]])
        if not skills:
            continue
        stat["with_skills"] += 1

        pairs.update(combinations(skills, 2))

    return stat, pairs


def reduce_pairs(parts):
    stat, pairs = Counter(), Counter()
    for part_stat, part_pairs in parts:
        stat.update(part_stat)
        pairs.update(part_pairs)
    return stat, pairs


def to_csv_bytes(counter):
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(["skills", "count"])
    for key, count in sorted(counter.items(), key=lambda item: (-item[1], item[0])):
        writer.writerow([" | ".join(key), count])
    return out.getvalue().encode("utf-8")
