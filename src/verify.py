import argparse
import csv
import hashlib
import io
import sys

from common import BUCKET, s3_client

REFERENCE_KEY = "results/mapreduce/stack_2.csv"


def read_counts(s3, key):
    data = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    rows = list(csv.reader(io.StringIO(data.decode("utf-8"))))[1:]
    return data, {row[0]: int(row[1]) for row in rows}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", required=True)
    args = parser.parse_args()

    s3 = s3_client()
    reference_data, reference = read_counts(s3, REFERENCE_KEY)
    print(f"ДЗ 3: {REFERENCE_KEY}: {len(reference)} пар, сумма {sum(reference.values())}, md5 {hashlib.md5(reference_data).hexdigest()}")

    objects = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"results/zk/{args.run}/").get("Contents", [])
    ok = bool(objects)

    for obj in objects:
        data, counts = read_counts(s3, obj["Key"])

        only_reference = len(reference.keys() - counts.keys())
        only_here = len(counts.keys() - reference.keys())
        different = sum(1 for key in reference.keys() & counts.keys() if reference[key] != counts[key])
        same_bytes = data == reference_data
        ok &= only_reference == only_here == different == 0 and same_bytes

        print(f"{obj['Key']}: {len(counts)} пар, сумма {sum(counts.values())}, md5 {hashlib.md5(data).hexdigest()}")
        print(
            f"  только в ДЗ 3: {only_reference}, только здесь: {only_here}, "
            f"разные счётчики: {different}, файлы побайтово равны: {same_bytes}"
        )

    print("ИТОГ: " + ("СОВПАЛО" if ok else "РАСХОЖДЕНИЕ"))

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
