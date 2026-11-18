"""分片布局：哈希分片与按 key 字典序的范围分片。"""

import bisect
import hashlib
from dataclasses import dataclass
from typing import Sequence, Tuple


def prefix_upper_bound(prefix: str) -> str:
    """返回字典序上严格大于所有以 ``prefix`` 开头字符串的最小串。"""
    chars = list(prefix)
    for pos in range(len(chars) - 1, -1, -1):
        code = ord(chars[pos])
        if code < 0x10FFFF:
            chars[pos] = chr(code + 1)
            return "".join(chars[: pos + 1])
    return "\U0010FFFF" + "0"


@dataclass(frozen=True)
class ShardLayout:
    """不可变分片布局。

    哈希布局（``kind="hash"``）：shard id 为 ``hash-0`` .. ``hash-N-1``，
    路由为 ``blake2b(key) mod N``，任何前缀都可能落在任意分片。

    范围布局（``kind="range"``）：``boundaries`` 为升序边界 B0..Bk-1，
    shard id 为 ``range-0`` .. ``range-k``，分片 i 覆盖
    ``B(i-1) <= key < Bi``（边界上界排他，首片下界无限小）。
    """

    kind: str
    shard_count: int
    boundaries: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.kind == "hash":
            if self.shard_count <= 0:
                raise ValueError("hash 布局 shard_count 必须为正数")
            if self.boundaries:
                raise ValueError("hash 布局不接受 boundaries")
        elif self.kind == "range":
            if list(self.boundaries) != sorted(self.boundaries) or len(
                set(self.boundaries)
            ) != len(self.boundaries):
                raise ValueError("range 布局 boundaries 必须严格升序")
            if self.shard_count != len(self.boundaries) + 1:
                raise ValueError("range 分片数必须等于边界数 + 1")
        else:
            raise ValueError(f"未知分片布局类型: {self.kind!r}")

    @classmethod
    def hash_layout(cls, shard_count: int) -> "ShardLayout":
        return cls(kind="hash", shard_count=shard_count, boundaries=())

    @classmethod
    def range_layout(cls, boundaries: Sequence[str]) -> "ShardLayout":
        return cls(
            kind="range",
            shard_count=len(boundaries) + 1,
            boundaries=tuple(boundaries),
        )

    def all_shards(self) -> Tuple[str, ...]:
        if self.kind == "hash":
            return tuple(f"hash-{i}" for i in range(self.shard_count))
        return tuple(f"range-{i}" for i in range(self.shard_count))

    def shard_for_key(self, key: str) -> str:
        """按布局计算 key 的归属分片。"""
        if self.kind == "hash":
            digest = hashlib.blake2b(key.encode("utf-8"), digest_size=8).digest()
            return f"hash-{int.from_bytes(digest, 'big') % self.shard_count}"
        index = bisect.bisect_left(self.boundaries, key)
        return f"range-{index}"

    def candidate_shards(self, prefix: str = "") -> Tuple[str, ...]:
        """返回可能包含该前缀 key 的分片，用于扫描剪枝。"""
        if self.kind == "hash":
            return self.all_shards()
        lower = bisect.bisect_left(self.boundaries, prefix)
        upper = bisect.bisect_left(self.boundaries, prefix_upper_bound(prefix))
        return tuple(f"range-{i}" for i in range(lower, upper + 1))

    def to_dict(self) -> dict:
        return {
            "kind": self.kind,
            "shard_count": self.shard_count,
            "boundaries": list(self.boundaries),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ShardLayout":
        return cls(
            kind=data["kind"],
            shard_count=int(data["shard_count"]),
            boundaries=tuple(data.get("boundaries", ())),
        )
