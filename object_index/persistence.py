"""追加式 WAL 日志与清单文件，支撑本地缓存持久化与恢复。"""

import json
import os
import tempfile
from typing import Any, Dict, Iterator, Optional


class CorruptLogError(RuntimeError):
    """日志中出现无法修复的损坏行（非崩溃导致的半行）。"""


def _atomic_write(path: str, payload: str) -> None:
    directory = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp-", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
        dir_fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except BaseException:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise


class LogStore:
    """单个 JSONL 追加日志。

    每条记录占一行，以 ``\\n`` 结尾。进程崩溃可能留下没有换行符的半行，
    打开时自动截断；已完整落盘的行若 JSON 非法则抛 :class:`CorruptLogError`。
    """

    def __init__(self, path: str, fsync: bool = True) -> None:
        self.path = path
        self.fsync = fsync
        directory = os.path.dirname(os.path.abspath(path))
        os.makedirs(directory, exist_ok=True)
        self._handle = open(path, "a+", encoding="utf-8")
        self._truncate_torn_tail()
        self._handle.seek(0, os.SEEK_END)

    def _truncate_torn_tail(self) -> None:
        self._handle.seek(0, os.SEEK_END)
        size = self._handle.tell()
        if size == 0:
            return
        self._handle.seek(size - 1)
        if self._handle.read(1) == "\n":
            return
        with open(self.path, "rb") as reader:
            data = reader.read()
        last_nl = data.rfind(b"\n")
        cut = last_nl + 1
        with open(self.path, "r+b") as writer:
            writer.truncate(cut)
        self._handle.seek(0, os.SEEK_END)

    def append(self, entry: Dict[str, Any]) -> int:
        line = json.dumps(entry, ensure_ascii=False, sort_keys=True)
        self._handle.write(line + "\n")
        self._handle.flush()
        if self.fsync:
            os.fsync(self._handle.fileno())
        return self._handle.tell()

    def replay(self) -> Iterator[Dict[str, Any]]:
        self._handle.seek(0)
        for line_no, line in enumerate(self._handle, start=1):
            if not line.strip():
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:
                raise CorruptLogError(
                    f"{self.path} 第 {line_no} 行损坏: {exc}"
                ) from exc
        self._handle.seek(0, os.SEEK_END)

    def close(self) -> None:
        self._handle.close()


class Manifest:
    """保存分片布局等需要随缓存一起恢复的静态元数据。"""

    def __init__(self, path: str) -> None:
        self.path = path

    def exists(self) -> bool:
        return os.path.exists(self.path)

    def write(self, payload: Dict[str, Any]) -> None:
        _atomic_write(self.path, json.dumps(payload, ensure_ascii=False, indent=2))

    def read(self) -> Optional[Dict[str, Any]]:
        if not self.exists():
            return None
        with open(self.path, "r", encoding="utf-8") as handle:
            return json.load(handle)
