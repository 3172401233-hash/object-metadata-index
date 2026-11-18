"""MVCC 对象元数据索引：固定版本读取、墓碑、快照与断点续扫。"""

import bisect
import os
from typing import Any, Dict, Iterable, List, Optional, Sequence

from .model import ObjectRecord, ScanPage, ScanSummary
from .persistence import LogStore, Manifest
from .sharding import ShardLayout, prefix_upper_bound


class KeyNotFoundError(LookupError):
    """key 在指定索引版本上不存在或已被删除。"""


class StaleVersionError(ValueError):
    """提交的索引版本不是当前版本的严格后继。"""


class DuplicateKeyError(ValueError):
    """同一批次中出现了同一个 key 的多条记录。"""


class LayoutMismatchError(ValueError):
    """记录的分片归属与索引布局不一致，或恢复时布局发生变化。"""


class InvalidCursorError(ValueError):
    """断点续扫使用了非法游标。"""


class Snapshot:
    """固定在某个索引版本上的不可变只读视图。"""

    def __init__(self, index: "MetadataIndex", index_version: int) -> None:
        self._index = index
        self._index_version = index_version

    @property
    def index_version(self) -> int:
        return self._index_version

    def find(self, key: str) -> Optional[ObjectRecord]:
        return self._index._find_at(self._index_version, key)

    def get(self, key: str) -> ObjectRecord:
        record = self.find(key)
        if record is None:
            raise KeyNotFoundError(
                f"key {key!r} 在索引版本 {self._index_version} 不存在或已删除"
            )
        return record

    def scan(
        self,
        prefix: str = "",
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
    ) -> ScanPage:
        return self._index._scan(self._index_version, prefix, limit, cursor)

    def scan_all(self, prefix: str = "") -> List[ObjectRecord]:
        records: List[ObjectRecord] = []
        cursor: Optional[str] = None
        while True:
            page = self.scan(prefix=prefix, limit=256, cursor=cursor)
            records.extend(page.records)
            if page.finished:
                return records
            cursor = page.next_cursor


class MetadataIndex:
    """本地对象元数据索引。

    每次 :meth:`commit` 产生一个新的不可变索引版本；旧版本通过
    :meth:`snapshot` 持续可读。普通查询永远作用于固定版本，扫描过程中
    即使索引继续提交，结果也不会跨版本跳变。
    """

    LOG_NAME = "index.log"
    MANIFEST_NAME = "manifest.json"

    def __init__(
        self,
        directory: str,
        layout: ShardLayout,
        fsync: bool = True,
    ) -> None:
        self.directory = directory
        self.layout = layout
        self._version = 0
        self._events: List[Dict[str, Any]] = []
        self._chains: Dict[str, List[int]] = {}
        self._sorted_keys: List[str] = []
        self._log = LogStore(os.path.join(directory, self.LOG_NAME), fsync=fsync)
        self._manifest = Manifest(os.path.join(directory, self.MANIFEST_NAME))
        self._persist_manifest(layout)

    @classmethod
    def open(cls, directory: str, fsync: bool = True) -> "MetadataIndex":
        """从本地缓存目录恢复索引，重放全部 WAL 条目。"""
        manifest = Manifest(os.path.join(directory, cls.MANIFEST_NAME))
        payload = manifest.read()
        if payload is None:
            raise FileNotFoundError(f"目录 {directory!r} 缺少 manifest.json")
        layout = ShardLayout.from_dict(payload["layout"])
        index = cls(directory, layout, fsync=fsync)
        for entry in index._log.replay():
            records = [ObjectRecord.from_dict(item) for item in entry["records"]]
            index._apply(entry["index_version"], records, persist=False)
        return index

    def _persist_manifest(self, layout: ShardLayout) -> None:
        if not self._manifest.exists():
            self._manifest.write({"layout": layout.to_dict()})
            return
        existing = ShardLayout.from_dict(self._manifest.read()["layout"])
        if existing != layout:
            raise LayoutMismatchError("恢复目录的分片布局与传入布局不一致")

    @property
    def index_version(self) -> int:
        return self._version

    def close(self) -> None:
        self._log.close()

    def __enter__(self) -> "MetadataIndex":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def commit(
        self,
        records: Iterable[ObjectRecord],
        index_version: Optional[int] = None,
    ) -> int:
        """原子提交一批记录，返回新的索引版本。"""
        batch = list(records)
        seen = set()
        for record in batch:
            if not isinstance(record.key, str) or not record.key:
                raise ValueError("record.key 必须是非空字符串")
            if record.key in seen:
                raise DuplicateKeyError(f"同一批次中 key {record.key!r} 重复")
            seen.add(record.key)
            if record.size < 0:
                raise ValueError(f"key {record.key!r} 的 size 不能为负数")
            expected = self.layout.shard_for_key(record.key)
            if record.shard_id != expected:
                raise LayoutMismatchError(
                    f"key {record.key!r} 声明分片 {record.shard_id!r}，"
                    f"布局计算结果为 {expected!r}"
                )
        if index_version is None:
            index_version = self._version + 1
        elif index_version != self._version + 1:
            raise StaleVersionError(
                f"期望索引版本 {self._version + 1}，收到 {index_version}"
            )
        self._apply(index_version, batch, persist=True)
        return index_version

    def _apply(
        self,
        index_version: int,
        batch: Sequence[ObjectRecord],
        persist: bool,
    ) -> None:
        if index_version <= self._version:
            raise StaleVersionError("索引版本必须严格单调递增")
        if persist:
            entry = {
                "index_version": index_version,
                "records": [record.to_dict() for record in batch],
            }
            self._log.append(entry)
        for record in batch:
            committed = ObjectRecord(
                key=record.key,
                version=str(record.version),
                size=record.size,
                etag=record.etag,
                deleted=record.deleted,
                shard_id=record.shard_id,
                index_version=index_version,
            )
            event_id = len(self._events)
            self._events.append(committed.to_dict())
            chain = self._chains.setdefault(committed.key, [])
            if not chain:
                insert_at = bisect.bisect_left(self._sorted_keys, committed.key)
                self._sorted_keys.insert(insert_at, committed.key)
            chain.append(event_id)
        self._version = index_version

    def put(
        self,
        key: str,
        version: str,
        size: int,
        etag: Optional[str] = None,
        shard_id: Optional[str] = None,
    ) -> ObjectRecord:
        """构造一条存活记录，shard_id 缺省时按布局计算。"""
        return ObjectRecord(
            key=key,
            version=str(version),
            size=size,
            etag=etag,
            deleted=False,
            shard_id=shard_id or self.layout.shard_for_key(key),
        )

    def tombstone(
        self,
        key: str,
        version: str,
        shard_id: Optional[str] = None,
    ) -> ObjectRecord:
        """构造一条删除墓碑记录。"""
        return ObjectRecord(
            key=key,
            version=str(version),
            size=0,
            etag=None,
            deleted=True,
            shard_id=shard_id or self.layout.shard_for_key(key),
        )

    def ingest(
        self,
        key: str,
        version: str,
        size: int,
        etag: Optional[str] = None,
        deleted: bool = False,
        shard_id: Optional[str] = None,
    ) -> int:
        """单条记录的便捷提交。"""
        if deleted:
            return self.commit([self.tombstone(key, version, shard_id)])
        return self.commit(
            [self.put(key, version, size, etag, shard_id)]
        )

    def snapshot(self, index_version: Optional[int] = None) -> Snapshot:
        """获取固定版本的只读快照，缺省为当前最新版本。"""
        if index_version is None:
            index_version = self._version
        if index_version < 0 or index_version > self._version:
            raise KeyNotFoundError(f"索引版本 {index_version} 不存在")
        return Snapshot(self, index_version)

    def _latest_event_id_at(self, index_version: int, key: str) -> Optional[int]:
        chain = self._chains.get(key)
        if not chain:
            return None
        lo, hi = 0, len(chain)
        while lo < hi:
            mid = (lo + hi) // 2
            event = ObjectRecord.from_dict(self._events[chain[mid]])
            if event.index_version <= index_version:
                lo = mid + 1
            else:
                hi = mid
        if lo == 0:
            return None
        return chain[lo - 1]

    def _find_at(self, index_version: int, key: str) -> Optional[ObjectRecord]:
        event_id = self._latest_event_id_at(index_version, key)
        if event_id is None:
            return None
        record = ObjectRecord.from_dict(self._events[event_id])
        if record.deleted:
            return None
        return record

    def find(self, key: str) -> Optional[ObjectRecord]:
        return self._find_at(self._version, key)

    def get(self, key: str) -> ObjectRecord:
        return self.snapshot().get(key)

    def scan(
        self,
        prefix: str = "",
        limit: Optional[int] = None,
        cursor: Optional[str] = None,
    ) -> ScanPage:
        return self._scan(self._version, prefix, limit, cursor)

    def scan_all(self, prefix: str = "") -> List[ObjectRecord]:
        return self.snapshot().scan_all(prefix)

    def _validate_cursor(self, prefix: str, cursor: str) -> None:
        upper = prefix_upper_bound(prefix)
        if prefix and not (cursor.startswith(prefix) or cursor >= upper):
            raise InvalidCursorError(
                f"游标 {cursor!r} 不在前缀 {prefix!r} 范围内"
            )

    def _scan(
        self,
        index_version: int,
        prefix: str,
        limit: Optional[int],
        cursor: Optional[str],
    ) -> ScanPage:
        if limit is not None and limit <= 0:
            raise ValueError("limit 必须为正整数")
        if cursor is not None:
            self._validate_cursor(prefix, cursor)
        candidates = self.layout.candidate_shards(prefix)
        upper = prefix_upper_bound(prefix)
        start_key = cursor or prefix
        if cursor is None:
            start = bisect.bisect_left(self._sorted_keys, start_key)
        else:
            start = bisect.bisect_right(self._sorted_keys, start_key)

        matched: List[ObjectRecord] = []
        shards_hit = set()
        live = 0
        tombstones = 0
        visible: List[ObjectRecord] = []
        for key in self._sorted_keys[start:]:
            if not key.startswith(prefix) or key >= upper:
                break
            event_id = self._latest_event_id_at(index_version, key)
            if event_id is None:
                continue
            record = ObjectRecord.from_dict(self._events[event_id])
            shards_hit.add(record.shard_id)
            if record.deleted:
                tombstones += 1
            else:
                live += 1
            matched.append(record)
            if not record.deleted:
                visible.append(record)

        page_records = visible if limit is None else visible[:limit]
        finished = limit is None or len(visible) <= limit
        next_cursor = None if finished else visible[limit - 1].key
        summary = ScanSummary(
            index_version=index_version,
            prefix=prefix,
            shards_hit=tuple(sorted(shards_hit)),
            candidates=candidates,
            shards_scanned=len(candidates),
            total_matched=len(matched),
            returned=len(page_records),
            live=live,
            tombstones=tombstones,
            finished=finished,
        )
        return ScanPage(
            records=tuple(page_records),
            next_cursor=next_cursor,
            finished=finished,
            summary=summary,
        )
