"""本地对象元数据索引。

提供基于固定索引版本的 get、前缀扫描与快照查询，支持删除墓碑、
分片迁移、扫描断点续扫以及基于追加日志的缓存恢复。
"""

from .index import (
    DuplicateKeyError,
    InvalidCursorError,
    KeyNotFoundError,
    LayoutMismatchError,
    MetadataIndex,
    Snapshot,
    StaleVersionError,
)
from .model import ObjectRecord, ScanPage, ScanSummary
from .persistence import CorruptLogError
from .sharding import ShardLayout

__all__ = [
    "CorruptLogError",
    "DuplicateKeyError",
    "InvalidCursorError",
    "KeyNotFoundError",
    "LayoutMismatchError",
    "MetadataIndex",
    "ObjectRecord",
    "ScanPage",
    "ScanSummary",
    "ShardLayout",
    "Snapshot",
    "StaleVersionError",
]
