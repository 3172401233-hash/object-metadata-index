# object-metadata-index
Python 对象元数据索引库，处理前缀查询、版本快照、分片迁移和缓存一致性

## 解决的问题

对象存储在百万级 key 规模下，每次前缀列举都扫描远端索引，延迟高且不稳定。
本库把元数据缓存到本地，同时用追加日志 + 多版本并发控制（MVCC）解决本地缓存
的两个典型问题：

- 新对象 / 删除标记不能及时反映：通过显式 `commit` 批次推进索引版本，
  每次提交原子可见。
- 前缀查询在版本之间跳变：每次扫描固定在一个不可变索引版本上，扫描期间
  即使有新提交，结果集也不会变化。

## 快速开始

```python
from object_index import MetadataIndex, ShardLayout

layout = ShardLayout.range_layout(["img/2024/", "img/2025/"])
index = MetadataIndex("./cache", layout)

index.commit([
    index.put("img/2024/a.png", version="obj-v1", size=1024, etag="e1"),
    index.put("img/2024/b.png", version="obj-v1", size=2048, etag="e2"),
])
v1 = index.index_version

# 删除：写墓碑而不是物理擦除
index.commit([index.tombstone("img/2024/b.png", version="obj-v2")])

# 普通查询看不到已删除对象
index.find("img/2024/b.png")          # None
index.snapshot(v1).get("img/2024/b.png")  # 旧快照仍可读

# 前缀扫描：固定版本、可翻页、可中断续扫
snapshot = index.snapshot(v1)
page = snapshot.scan(prefix="img/2024/", limit=100)
print(page.summary.to_dict())         # 命中分片、版本与结果摘要
while not page.finished:
    page = snapshot.scan(prefix="img/2024/", limit=100, cursor=page.next_cursor)

index.close()

# 进程重启后从 WAL 恢复
recovered = MetadataIndex.open("./cache")
```

## 数据模型

- `ObjectRecord`：`key`、对象 `version`（远端对象版本，字符串/整数均可）、
  `size`、`etag`、`deleted`（墓碑标记）、`shard_id`、`index_version`
  （提交时分配的本地索引版本）。
- `ScanPage`：本页 `records`、排他语义的 `next_cursor`、`finished` 和
  `ScanSummary`。
- `ScanSummary`：输出 `index_version`、`prefix`、`shards_hit`（实际命中
  分片）、`candidates`（候选分片）、`shards_scanned`、`total_matched`、
  `returned`、`live`、`tombstones`、`finished`。

## 一致性模型

1. **索引版本单调递增**。每次 `commit` 原子应用一批记录并产生
   `index_version = 旧版本 + 1`；显式传入非后继版本会抛 `StaleVersionError`。
2. **快照隔离（固定版本读取）**。`snapshot(v)` 返回版本 `v` 的不可变视图，
   `get` 与 `scan` 都只读取该版本及之前已提交的状态。扫描中途有新提交不影响
   进行中的扫描，因此前缀结果不会跨版本跳变。
3. **对象版本与索引版本分离**。对象重写产生新的对象 `version`，但可见性由
   索引版本决定；同一 key 的历史记录按索引版本串成版本链，旧快照可回溯任意
   历史状态。
4. **删除墓碑**。删除通过追加 `deleted=True` 的墓碑实现：最新版本上的普通
   `get`/`find`/`scan` 不可见该 key（`find` 返回 `None`，`get` 抛
   `KeyNotFoundError`），但墓碑之前的旧快照仍能读到原对象。墓碑计入摘要的
   `tombstones`，但不出现在 `records` 里。
5. **扫描是固定版本 + 断点续扫**。`next_cursor` 为排他游标，恢复时从游标
   之后继续；游标必须位于所扫前缀的字典序范围内，否则抛
   `InvalidCursorError`。翻页时 `total_matched/live/tombstones` 针对完整
   结果集保持稳定，`returned` 只对当前页计数。
6. **单进程追加日志**。持久化采用 JSONL WAL（`index.log`）加清单
   （`manifest.json`），每条提交一行并 `fsync`。`MetadataIndex.open` 重放
   日志恢复全部历史版本；崩溃留下的无换行半行会在打开时安全截断，而已经完整
   落盘的损坏行抛 `CorruptLogError`，避免静默丢数据。

## 分片边界

支持两种不可变布局，由 `ShardLayout` 定义并写入 `manifest.json`：

- **哈希分片** `ShardLayout.hash_layout(n)`：分片 id 为 `hash-0` …
  `hash-(n-1)`，路由为 `blake2b(key) mod n`。前缀与分片没有字典序对应关系，
  因此任何前缀扫描的候选分片都是全部分片，`shards_hit` 只报告实际命中的分片。
- **范围分片** `ShardLayout.range_layout(boundaries)`：边界严格升序
  `B0 … B(k-1)`，分片 id 为 `range-0` … `range-k`，分片 `i` 覆盖
  `B(i-1) <= key < B(i)`——**边界上界排他、首片下界无限小**。前缀扫描用前缀
  字典序区间在边界表上二分，只把可能命中的分片列为候选。

分片归属在提交时校验：记录的 `shard_id` 必须等于布局对该 key 的计算结果，
否则抛 `LayoutMismatchError`。

**分片迁移**通过新建目标布局的索引目录完成：迁移批次中的记录按新布局携带新
`shard_id`，提交到新索引；旧索引与旧快照保持原分片信息不变，因此迁移期间的
历史读取仍然一致。同一目录内布局不可变更（`manifest.json` 校验）。

## 测试

```bash
python -m unittest
```

覆盖场景：前缀嵌套重叠、哈希/范围两种分片剪枝、分片迁移、对象重写历史、
删除墓碑与对象复活、扫描中断与游标续扫、非法游标、WAL 缓存恢复、崩溃半行
截断与完整损坏行报错。
