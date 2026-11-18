import json
import os
import tempfile
import unittest

from object_index import (
    DuplicateKeyError,
    InvalidCursorError,
    KeyNotFoundError,
    LayoutMismatchError,
    MetadataIndex,
    ObjectRecord,
    ShardLayout,
    StaleVersionError,
)
from object_index.persistence import CorruptLogError


class IndexTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = self.tmp.name

    def tearDown(self):
        self.tmp.cleanup()

    def log_path(self, index):
        return os.path.join(index.directory, index.LOG_NAME)


class PrefixOverlapTest(IndexTestBase):
    def test_empty_index_and_empty_prefix(self):
        layout = ShardLayout.hash_layout(2)
        with MetadataIndex(self.dir, layout) as index:
            page = index.scan()
            self.assertEqual(page.records, ())
            self.assertTrue(page.finished)
            self.assertEqual(page.summary.shards_scanned, 2)
            self.assertEqual(page.summary.index_version, 0)
            with self.assertRaises(KeyNotFoundError):
                index.get("missing")

    def test_nested_prefixes_overlap_without_skipping_keys(self):
        layout = ShardLayout.range_layout(["img/", "img/2024/", "img/2025/"])
        with MetadataIndex(self.dir, layout) as index:
            keys = [
                "img/2024/a.png",
                "img/2024/b.png",
                "img/2025/c.png",
                "img/other/d.png",
                "video/e.mp4",
            ]
            index.commit(
                [
                    index.put(key, f"v-{i}", 100 + i, etag=f"etag-{i}")
                    for i, key in enumerate(keys)
                ]
            )

            page = index.scan(prefix="img/2024/")
            self.assertEqual(
                [r.key for r in page.records],
                ["img/2024/a.png", "img/2024/b.png"],
            )
            self.assertEqual(page.summary.returned, 2)
            self.assertEqual(page.summary.live, 2)
            self.assertEqual(page.summary.tombstones, 0)
            self.assertTrue(page.finished)

            broader = index.scan_all(prefix="img/")
            self.assertEqual(
                [r.key for r in broader],
                [
                    "img/2024/a.png",
                    "img/2024/b.png",
                    "img/2025/c.png",
                    "img/other/d.png",
                ],
            )

            all_page = index.scan(prefix="")
            self.assertEqual(all_page.summary.total_matched, 5)
            self.assertEqual(
                list(all_page.summary.shards_hit),
                ["range-2", "range-3"],
            )

    def test_hash_layout_scans_every_candidate_shard(self):
        layout = ShardLayout.hash_layout(4)
        with MetadataIndex(self.dir, layout) as index:
            keys = [f"logs/{i:04d}.log" for i in range(20)]
            index.commit(
                [index.put(key, "1", 10) for key in keys]
            )
            page = index.scan(prefix="logs/")
            self.assertEqual(page.summary.shards_scanned, 4)
            self.assertEqual(page.summary.returned, 20)
            self.assertLessEqual(len(page.summary.shards_hit), 4)
            self.assertEqual(
                set(r.shard_id for r in page.records),
                set(page.summary.shards_hit),
            )


class ShardMigrationTest(IndexTestBase):
    def test_moved_key_uses_new_shard_while_snapshot_keeps_old(self):
        old_layout = ShardLayout.range_layout(["m/"])
        with MetadataIndex(self.dir, old_layout) as index:
            key = "m/object.bin"
            old_shard = old_layout.shard_for_key(key)
            index.ingest(key, "obj-v1", 1024, etag="e1")
            v1 = index.index_version
            self.assertEqual(index.get(key).shard_id, old_shard)

            new_layout = ShardLayout.range_layout(["a/", "m/"])
            new_shard = new_layout.shard_for_key(key)
            self.assertNotEqual(old_shard, new_shard)

            with self.assertRaises(LayoutMismatchError):
                index.commit(
                    [ObjectRecord(key, "obj-v2", 2048, "e2", False, new_shard)]
                )

        migrated_dir = os.path.join(self.tmp.name, "migrated")
        with MetadataIndex(migrated_dir, new_layout) as migrated:
            migrated.ingest(key, "obj-v2", 2048, etag="e2")
            self.assertEqual(migrated.get(key).shard_id, new_shard)
            self.assertEqual(migrated.get(key).version, "obj-v2")
            page = migrated.scan(prefix="m/")
            self.assertEqual(page.summary.shards_hit, (new_shard,))

        reopened = MetadataIndex.open(self.dir)
        try:
            old_view = reopened.snapshot(v1)
            self.assertEqual(old_view.get(key).shard_id, old_shard)
        finally:
            reopened.close()


class ObjectRewriteTest(IndexTestBase):
    def test_rewrite_advances_object_version_but_preserves_history(self):
        layout = ShardLayout.hash_layout(3)
        with MetadataIndex(self.dir, layout) as index:
            key = "bucket/rewritten.txt"
            index.ingest(key, "rev-1", 10, etag="etag-1")
            v1 = index.index_version
            index.ingest(key, "rev-2", 20, etag="etag-2")
            v2 = index.index_version
            index.ingest(key, "rev-3", 30, etag="etag-3")
            v3 = index.index_version

            self.assertEqual(index.get(key).version, "rev-3")
            self.assertEqual(index.get(key).size, 30)
            self.assertEqual(index.snapshot(v1).get(key).version, "rev-1")
            self.assertEqual(index.snapshot(v1).get(key).size, 10)
            self.assertEqual(index.snapshot(v2).get(key).etag, "etag-2")

            page = index.scan(prefix="bucket/")
            self.assertEqual(page.summary.total_matched, 1)
            self.assertEqual(page.records[0].index_version, v3)

    def test_commit_requires_strict_next_index_version(self):
        layout = ShardLayout.hash_layout(2)
        with MetadataIndex(self.dir, layout) as index:
            index.ingest("k1", "1", 1)
            with self.assertRaises(StaleVersionError):
                index.commit([index.put("k2", "1", 1)], index_version=5)
            with self.assertRaises(DuplicateKeyError):
                index.commit(
                    [index.put("dup", "1", 1), index.put("dup", "2", 2)]
                )


class TombstoneTest(IndexTestBase):
    def test_deleted_key_invisible_to_get_but_readable_in_snapshot(self):
        layout = ShardLayout.range_layout(["d/"])
        with MetadataIndex(self.dir, layout) as index:
            index.ingest("d/a", "1", 10)
            index.ingest("d/b", "1", 10)
            alive_version = index.index_version

            index.commit([index.tombstone("d/a", "2")])
            delete_version = index.index_version

            self.assertIsNone(index.find("d/a"))
            with self.assertRaises(KeyNotFoundError):
                index.get("d/a")
            self.assertEqual(
                index.snapshot(alive_version).get("d/a").version, "1"
            )

            page = index.scan(prefix="d/")
            self.assertEqual([r.key for r in page.records], ["d/b"])
            self.assertEqual(page.summary.live, 1)
            self.assertEqual(page.summary.tombstones, 1)
            self.assertEqual(page.summary.total_matched, 2)

            with self.assertRaises(KeyNotFoundError):
                index.snapshot(delete_version).get("d/a")

            index.ingest("d/a", "3", 99, etag="reborn")
            reborn = index.get("d/a")
            self.assertEqual(reborn.version, "3")
            self.assertFalse(reborn.deleted)


class ScanInterruptionTest(IndexTestBase):
    def test_pagination_resumes_from_cursor_on_fixed_version(self):
        layout = ShardLayout.hash_layout(4)
        with MetadataIndex(self.dir, layout) as index:
            keys = [f"p/key-{i:02d}" for i in range(10)]
            index.commit([index.put(key, "1", 1) for key in keys])
            pinned = index.index_version

            first = index.scan(prefix="p/", limit=4)
            self.assertEqual(len(first.records), 4)
            self.assertFalse(first.finished)
            self.assertEqual(first.next_cursor, "p/key-03")

            index.ingest("p/key-late", "9", 5)
            index.commit([index.tombstone("p/key-05", "9")])

            second = index.snapshot(pinned).scan(
                prefix="p/", limit=4, cursor=first.next_cursor
            )
            self.assertEqual(
                [r.key for r in second.records],
                [f"p/key-{i:02d}" for i in range(4, 8)],
            )
            self.assertFalse(second.finished)
            self.assertEqual(second.summary.index_version, pinned)

            third = index.snapshot(pinned).scan(
                prefix="p/", limit=4, cursor=second.next_cursor
            )
            self.assertEqual(
                [r.key for r in third.records],
                ["p/key-08", "p/key-09"],
            )
            self.assertTrue(third.finished)
            self.assertIsNone(third.next_cursor)

            full = index.snapshot(pinned).scan_all(prefix="p/")
            self.assertEqual([r.key for r in full], keys)

    def test_cursor_outside_prefix_is_rejected(self):
        layout = ShardLayout.range_layout(["x/"])
        with MetadataIndex(self.dir, layout) as index:
            index.ingest("x/1", "1", 1)
            with self.assertRaises(InvalidCursorError):
                index.scan(prefix="x/", cursor="w/0")


class CacheRecoveryTest(IndexTestBase):
    def test_reopen_replays_log_and_keeps_snapshot_reads(self):
        layout = ShardLayout.hash_layout(3)
        with MetadataIndex(self.dir, layout) as index:
            index.ingest("cache/a", "1", 11, etag="a1")
            v1 = index.index_version
            index.ingest("cache/b", "1", 22)
            index.commit([index.tombstone("cache/a", "2")])
            latest = index.index_version

        recovered = MetadataIndex.open(self.dir)
        try:
            self.assertEqual(recovered.index_version, latest)
            self.assertIsNone(recovered.find("cache/a"))
            self.assertEqual(recovered.get("cache/b").size, 22)
            self.assertEqual(
                recovered.snapshot(v1).get("cache/a").etag, "a1"
            )
            page = recovered.scan(prefix="cache/")
            self.assertEqual(page.summary.tombstones, 1)
            self.assertEqual(page.summary.live, 1)

            recovered.ingest("cache/c", "1", 33)
            self.assertEqual(recovered.index_version, latest + 1)
        finally:
            recovered.close()

        again = MetadataIndex.open(self.dir)
        try:
            self.assertEqual(again.index_version, latest + 1)
            self.assertEqual(again.get("cache/c").size, 33)
        finally:
            again.close()

    def test_torn_tail_from_crash_is_truncated(self):
        layout = ShardLayout.hash_layout(2)
        with MetadataIndex(self.dir, layout) as index:
            index.ingest("safe/1", "1", 1)

        log = self.log_path(index)
        index.close()
        with open(log, "a", encoding="utf-8") as handle:
            handle.write('{"index_version": 2, "records": [{"incomplete":')
            handle.flush()
            os.fsync(handle.fileno())

        recovered = MetadataIndex.open(self.dir)
        try:
            self.assertEqual(recovered.index_version, 1)
            self.assertEqual(recovered.get("safe/1").version, "1")
            recovered.ingest("safe/2", "2", 2)
            self.assertEqual(recovered.index_version, 2)
        finally:
            recovered.close()

    def test_corrupt_complete_line_raises(self):
        layout = ShardLayout.hash_layout(2)
        with MetadataIndex(self.dir, layout) as index:
            index.ingest("safe/1", "1", 1)
        log = self.log_path(index)
        index.close()
        with open(log, "a", encoding="utf-8") as handle:
            handle.write("{not valid json}\n")
        with self.assertRaises(CorruptLogError):
            MetadataIndex.open(self.dir)

    def test_log_entries_are_json_lines_with_summary_fields(self):
        layout = ShardLayout.range_layout(["j/"])
        with MetadataIndex(self.dir, layout) as index:
            index.ingest("j/1", "1", 7, etag="e")
            page = index.scan(prefix="j/")
            summary_dict = page.summary.to_dict()
            self.assertEqual(
                set(summary_dict),
                {
                    "index_version",
                    "prefix",
                    "shards_hit",
                    "candidates",
                    "shards_scanned",
                    "total_matched",
                    "returned",
                    "live",
                    "tombstones",
                    "finished",
                },
            )
        with open(self.log_path(index), encoding="utf-8") as handle:
            entry = json.loads(handle.readline())
        self.assertEqual(entry["index_version"], 1)
        self.assertEqual(entry["records"][0]["key"], "j/1")


if __name__ == "__main__":
    unittest.main()
