"""索引中流转的数据模型。"""

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


@dataclass(frozen=True)
class ObjectRecord:
    """某个 key 在一次提交中的元数据状态。

    ``version`` 是对象自身的版本号（远端对象版本，允许是字符串或整数），
    ``index_version`` 是该状态被提交到本地索引时获得的单调递增索引版本。
    ``deleted=True`` 表示一条删除墓碑。
    """

    key: str
    version: str
    size: int
    etag: Optional[str]
    deleted: bool
    shard_id: str
    index_version: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "version": self.version,
            "size": self.size,
            "etag": self.etag,
            "deleted": self.deleted,
            "shard_id": self.shard_id,
            "index_version": self.index_version,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ObjectRecord":
        return cls(
            key=data["key"],
            version=str(data["version"]),
            size=int(data["size"]),
            etag=data.get("etag"),
            deleted=bool(data["deleted"]),
            shard_id=data["shard_id"],
            index_version=int(data.get("index_version", 0)),
        )


@dataclass(frozen=True)
class ScanSummary:
    """一次扫描（或一页扫描）的结果摘要。

    计数口径针对整个前缀结果集，翻页时保持稳定；``returned`` 与
    ``finished`` 描述当前页。
    """

    index_version: int
    prefix: str
    shards_hit: Tuple[str, ...]
    candidates: Tuple[str, ...]
    shards_scanned: int
    total_matched: int
    returned: int
    live: int
    tombstones: int
    finished: bool

    def to_dict(self) -> Dict[str, Any]:
        return {
            "index_version": self.index_version,
            "prefix": self.prefix,
            "shards_hit": list(self.shards_hit),
            "candidates": list(self.candidates),
            "shards_scanned": self.shards_scanned,
            "total_matched": self.total_matched,
            "returned": self.returned,
            "live": self.live,
            "tombstones": self.tombstones,
            "finished": self.finished,
        }


@dataclass(frozen=True)
class ScanPage:
    """前缀扫描的一页结果，``next_cursor`` 可用于断点续扫。"""

    records: Tuple[ObjectRecord, ...]
    next_cursor: Optional[str]
    finished: bool
    summary: ScanSummary
