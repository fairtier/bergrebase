"""Drives pyiceberg for the e2e suite: one JSON request on stdin, one JSON reply on stdout.

Run by test/harness/writers.go. The rows match the DuckDB shapes in
real_writers_e2e_test.go: id, g=id%10, s='v'||id, d, ts.
"""

import datetime as dt
import hashlib
import json
import sys

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import AlwaysTrue, And, GreaterThanOrEqual, LessThan
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import BucketTransform, DayTransform, IdentityTransform
from pyiceberg.types import DateType, IntegerType, LongType, NestedField, StringType, TimestampType

SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "g", IntegerType()),
    NestedField(3, "s", StringType()),
    NestedField(4, "d", DateType()),
    NestedField(5, "ts", TimestampType()),
    identifier_field_ids=[1],
)
ARROW = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("g", pa.int32()),
        pa.field("s", pa.large_string()),
        pa.field("d", pa.date32()),
        pa.field("ts", pa.timestamp("us")),
    ]
)


def rows(lo, hi, prefix="v"):
    ids = list(range(lo, hi))
    base = dt.datetime(2026, 1, 1)
    return pa.table(
        {
            "id": ids,
            "g": [i % 10 for i in ids],
            "s": [f"{prefix}{i}" for i in ids],
            "d": [dt.date(2026, 1, 1) + dt.timedelta(days=i % 40) for i in ids],
            "ts": [base + dt.timedelta(hours=i) for i in ids],
        },
        schema=ARROW,
    )


class Driver:
    def __init__(self, req):
        s3 = req["s3"]
        self.ns = req["namespace"]
        self.catalog = load_catalog(
            "e2e",
            **{
                "type": "rest",
                "uri": req["catalog_uri"],
                "warehouse": req["warehouse"],
            },
        )
        # The catalog hands out its in-network S3 endpoint, which only resolves
        # inside the Docker network, so every table gets this FileIO instead.
        self.io = PyArrowFileIO(
            {
                "s3.endpoint": s3["endpoint"],
                "s3.access-key-id": s3["access_key"],
                "s3.secret-access-key": s3["secret_key"],
                "s3.region": "us-east-1",
                "s3.force-virtual-addressing": False,
            }
        )

    def table(self, name):
        t = self.catalog.load_table((self.ns, name))
        t.io = self.io
        return t

    def create(self, name, spec=None):
        kwargs = {"partition_spec": spec} if spec else {}
        t = self.catalog.create_table((self.ns, name), schema=SCHEMA, **kwargs)
        t.io = self.io
        return t

    def write_shapes(self, _req):
        self.catalog.create_namespace_if_not_exists(self.ns)
        by_g = PartitionSpec(PartitionField(2, 1000, IdentityTransform(), "g"))
        by_d = PartitionSpec(PartitionField(4, 1000, IdentityTransform(), "d"))
        xform = PartitionSpec(
            PartitionField(5, 1000, DayTransform(), "ts_day"),
            PartitionField(1, 1001, BucketTransform(4), "id_b"),
        )
        self.create("empty")

        t = self.create("inserts")
        t.append(rows(1, 1001))
        t.append(rows(1001, 1101))

        # The delete is the last commit, so the current snapshot's manifest list
        # holds the DELETED entries a rewrite must not turn back into ADDED.
        t = self.create("deletes")
        t.append(rows(1, 1001))
        t.delete(And(GreaterThanOrEqual("id", 100), LessThan("id", 200)))

        # dlt's replace disposition: truncate in its own commit, then append.
        t = self.create("replace")
        t.append(rows(1, 501))
        t.delete(delete_filter=AlwaysTrue())
        t.append(rows(1, 301))

        # dlt's merge disposition.
        t = self.create("upsert")
        t.append(rows(1, 501))
        t.upsert(rows(400, 601, "new-"))

        t = self.create("part", by_g)
        t.append(rows(1, 501))
        t.append(rows(501, 601))

        t = self.create("part_xform", xform)
        t.append(rows(1, 201))

        t = self.create("part_del", by_d)
        t.append(rows(1, 501))
        t.delete(And(GreaterThanOrEqual("id", 100), LessThan("id", 200)))
        return {}

    def v1_upgrade(self, req):
        """A format-version 1 table upgraded to 2: v1 manifests carried into v2 lists."""
        self.catalog.create_namespace_if_not_exists(self.ns)
        by_g = PartitionSpec(PartitionField(2, 1000, IdentityTransform(), "g"))
        t = self.catalog.create_table((self.ns, req["table"]), schema=SCHEMA, partition_spec=by_g, properties={"format-version": "1"})
        t.io = self.io
        t.append(rows(1, 101))
        t.append(rows(101, 201))
        t.delete(LessThan("id", 20))
        with t.transaction() as tx:
            tx.upgrade_table_version(2)
        t = self.table(req["table"])
        t.append(rows(201, 301))
        t.delete(LessThan("id", 150))
        return {}

    def append_batches(self, req):
        self.catalog.create_namespace_if_not_exists(self.ns)
        t = self.create(req["table"], PartitionSpec(PartitionField(2, 1000, IdentityTransform(), "g")))
        size = req["batch_rows"]
        for k in range(req["batches"]):
            t.append(rows(1 + k * size, 1 + (k + 1) * size))
        t.delete(And(GreaterThanOrEqual("id", 100), LessThan("id", 200)))
        return {}

    def compact(self, req):
        """Swap every data file for one, the way iceberg-maintenance's compaction does."""
        t = self.table(req["table"])
        data = t.scan().to_arrow()
        with t.transaction() as tx:
            tx.delete(delete_filter=AlwaysTrue())
            tx.append(data)
        return {}

    def read(self, req):
        t = self.table(req["table"])
        data = t.scan().to_arrow()
        lines = sorted(json.dumps(r, default=str, sort_keys=True) for r in data.to_pylist())
        return {
            "count": data.num_rows,
            "digest": hashlib.sha256("\n".join(lines).encode()).hexdigest(),
            "snapshots": {str(s.snapshot_id): t.scan(snapshot_id=s.snapshot_id).count() for s in t.metadata.snapshots},
        }


def main():
    req = json.load(sys.stdin)
    json.dump(getattr(Driver(req), req["op"])(req), sys.stdout)


if __name__ == "__main__":
    main()
