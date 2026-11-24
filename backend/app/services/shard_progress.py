"""新版分片进度记录领域服务：以分片为最小恢复边界，统一传输与断点续传的进度真相。

背景：历史批次只在 ``data/checkpoints`` 下留下记录级水位检查点（``batch_*``、
``resume_*``、``staging_*``），新版上线后需要把这些历史检查点迁移为分片进度文档，
让旧批次在升级前后都能按同一套可解释的分片边界恢复，且不重发任何已交付分片。

设计约定：

* 新版进度文档存于 ``data/shard_progress/<批次编号>.json``，写入采用临时文件 +
  ``os.replace`` 原子替换，并发更新通过同目录 ``.lock`` 文件上的 ``fcntl`` 互斥锁串行化。
* 旧检查点文件永不删除、永不原地改写：迁移只读取并翻译，传输/续传仍对旧检查点做双写，
  升级失败或回滚后旧版本可以继续读取存量检查点。
* 迁移可重复执行并最终收敛：旧批次的迟到持久化（迁移后才完成的旧检查点写入）会在下次
  解析边界时被重新读取，结合批次产物文件中的实际落地记录对账，不会丢失也不会重复计数。
* 一个分片只有被水位完整覆盖才标记为已交付；尾部半片保留记录级 ``partial_watermark``，
  续传时按产物中已存在的记录主键去重，做到分片级调度、记录级不重发。
"""
from __future__ import annotations

import contextlib
import fcntl
import json
import math
import os
import threading
import time
from pathlib import Path
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 与传输批次种子数据中的分片大小保持一致，历史检查点未携带分片大小时使用该默认值。
DEFAULT_CHUNK_SIZE = 1000
SCHEMA_VERSION = 2

# 线程内可重入锁的持有状态：同线程对同批次的嵌套调用（迁移→对账→投递）直接放行，
# 不同线程/进程仍由 fcntl 互斥锁串行，避免自锁死锁。
_lock_state = threading.local()


class ShardProgressService(BaseModuleService):
    """分片进度记录服务：迁移、对账、边界解析与传输/续传共享投递。"""

    MODULE = "shard_progress"
    ENTITY = "分片进度"
    OPERATION_TERM = "水位"
    LIST_FIELDS = [
        "批次编号", "分片大小", "分片总数", "记录总数", "已交付分片", "待传分片",
        "续传水位", "进度来源", "边界说明", "模式版本",
    ]
    REQUIRED_FIELDS = ["批次编号", "分片大小"]
    STATUS_ORDER = ["登记", "传输中", "已完成"]
    ACTION_RULES = {"推进分片": "传输中", "确认完成": "已完成"}

    # ------------------------------------------------------------------
    # 存储原语：目录、互斥锁、原子读写
    # ------------------------------------------------------------------

    def _progress_dir(self) -> Path:
        return self._data_dir("shard_progress")

    def _doc_path(self, batch_code: str) -> Path:
        return self._progress_dir() / f"{batch_code}.json"

    def _lock_path(self, batch_code: str) -> Path:
        return self._progress_dir() / f"{batch_code}.lock"

    @contextlib.contextmanager
    def _locked(self, batch_code: str):
        """按批次加互斥锁，保证同批次的迁移、投递与对账在线程/进程间串行且可重入。"""
        held = getattr(_lock_state, "held", None)
        if held is None:
            held = {}
            _lock_state.held = held
        if batch_code in held:
            held[batch_code][1] += 1
            try:
                yield
            finally:
                held[batch_code][1] -= 1
            return
        lock_file = open(self._lock_path(batch_code), "a+", encoding="utf-8")
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            held[batch_code] = [lock_file, 1]
            yield
        finally:
            held.pop(batch_code, None)
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            lock_file.close()

    def _load_doc(self, batch_code: str) -> dict[str, Any] | None:
        path = self._doc_path(batch_code)
        if not path.exists():
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                doc = json.load(f)
        except (json.JSONDecodeError, OSError):
            # 升级中断可能留下不可读文档：视为不存在，回退到历史检查点重建。
            return None
        if not isinstance(doc, dict) or doc.get("batch_code") != batch_code:
            return None
        return doc

    def _store_doc(self, doc: dict[str, Any]) -> Path:
        doc["updated_at"] = time.time()
        path = self._doc_path(doc["batch_code"])
        tmp = path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        return path

    # ------------------------------------------------------------------
    # 进度文档与 store 表的初始化 / 同步
    # ------------------------------------------------------------------

    def _new_doc(self, batch_code: str, chunk_size: int, total_records: int,
                 source: str) -> dict[str, Any]:
        now = time.time()
        total_chunks = math.ceil(total_records / chunk_size) if total_records else 0
        return {
            "schema_version": SCHEMA_VERSION,
            "batch_code": batch_code,
            "chunk_size": chunk_size,
            "total_records": total_records,
            "total_chunks": total_chunks,
            "watermark": 0,                # 已完整交付分片覆盖的记录数
            "partial_watermark": 0,        # 产物中实际落地的记录数（可能落在半片内）
            "delivered_chunks": [],
            "shards": [
                {
                    "chunk_index": idx,
                    "record_start": idx * chunk_size,
                    "record_end": min((idx + 1) * chunk_size, total_records),
                    "record_count": min(chunk_size, total_records - idx * chunk_size),
                    "status": "pending",
                    "delivered_at": None,
                    "source": None,
                }
                for idx in range(total_chunks)
            ],
            "sources": [source],
            "migrated_from": [],
            "created_at": now,
            "updated_at": now,
        }

    def _sync_store_entry(self, doc: dict[str, Any]) -> dict[str, Any]:
        """把分片进度文档同步为 store 中的可查询条目（通用资源接口可见）。"""
        rows = store.rows(self.MODULE)
        entry = next((r for r in rows if r.get("批次编号") == doc["batch_code"]), None)
        pending = [s["chunk_index"] for s in doc["shards"] if s["status"] == "pending"]
        boundary = self._boundary_text(doc)
        values = {
            "批次编号": doc["batch_code"],
            "分片大小": str(doc["chunk_size"]),
            "分片总数": str(doc["total_chunks"]),
            "记录总数": str(doc["total_records"]),
            "已交付分片": ",".join(str(i) for i in doc["delivered_chunks"]) or "-",
            "待传分片": ",".join(str(i) for i in pending) or "-",
            "续传水位": str(doc["watermark"]),
            "进度来源": "|".join(doc["sources"]),
            "边界说明": boundary,
            "模式版本": f"v{SCHEMA_VERSION}",
        }
        if entry is None:
            entry, missing = self.create_entry(values)
            if entry is None:  # pragma: no cover - 批次编号与分片大小必填，理论不会触发
                raise ValueError(f"分片进度登记失败: {missing}")
        else:
            entry.update(values)
        entry["status"] = "已完成" if not pending and doc["total_chunks"] > 0 else (
            "传输中" if doc["delivered_chunks"] else "登记")
        entry["pending"] = bool(pending)
        entry["abnormal"] = False
        return entry

    @staticmethod
    def _boundary_text(doc: dict[str, Any]) -> str:
        """生成可向运维解释的分片边界文字：每个分片的记录区间与状态。"""
        parts = [
            f"分片{s['chunk_index']}[{s['record_start']},{s['record_end']})={s['status']}"
            for s in doc["shards"]
        ]
        return ";".join(parts)

    def ensure_progress(self, batch_code: str, *, total_records: int | None = None,
                        chunk_size: int | None = None, source: str = "new") -> dict[str, Any]:
        """确保批次存在新版分片进度文档；已存在时补全总量信息但不覆盖既有分片大小。"""
        with self._locked(batch_code):
            doc = self._load_doc(batch_code)
            if doc is None:
                cs = int(chunk_size or DEFAULT_CHUNK_SIZE)
                total = int(total_records or 0)
                doc = self._new_doc(batch_code, cs, total, source)
            else:
                if chunk_size and int(chunk_size) != doc["chunk_size"]:
                    # 分片大小属于恢复契约，首次确定后不允许漂移，避免边界重算导致重发。
                    raise ValueError(
                        f"批次 {batch_code} 分片大小已固定为 {doc['chunk_size']}，"
                        f"拒绝改写为 {chunk_size}")
                if total_records and int(total_records) > doc["total_records"]:
                    doc["total_records"] = int(total_records)
                    doc["total_chunks"] = math.ceil(doc["total_records"] / doc["chunk_size"])
                    existing = {s["chunk_index"] for s in doc["shards"]}
                    for idx in range(doc["total_chunks"]):
                        if idx not in existing:
                            doc["shards"].append({
                                "chunk_index": idx,
                                "record_start": idx * doc["chunk_size"],
                                "record_end": min((idx + 1) * doc["chunk_size"],
                                                  doc["total_records"]),
                                "record_count": min(doc["chunk_size"],
                                                    doc["total_records"] - idx * doc["chunk_size"]),
                                "status": "pending",
                                "delivered_at": None,
                                "source": None,
                            })
                if source not in doc["sources"]:
                    doc["sources"].append(source)
            self._store_doc(doc)
            self._sync_store_entry(doc)
            return json.loads(json.dumps(doc, default=str))

    # ------------------------------------------------------------------
    # 分片投递与实际落地对账
    # ------------------------------------------------------------------

    def mark_chunk_delivered(self, batch_code: str, chunk_index: int, *,
                             source: str = "transfer") -> dict[str, Any]:
        """幂等标记一个分片已交付；重复标记不重复推进水位。"""
        with self._locked(batch_code):
            doc = self._load_doc(batch_code)
            if doc is None:
                raise ValueError(f"批次 {batch_code} 尚无分片进度文档，请先登记或迁移")
            shard = next((s for s in doc["shards"] if s["chunk_index"] == chunk_index), None)
            if shard is None:
                raise ValueError(f"批次 {batch_code} 不存在分片 {chunk_index}")
            already = shard["status"] == "delivered"
            if not already:
                shard["status"] = "delivered"
                shard["delivered_at"] = time.time()
                shard["source"] = source
            doc["delivered_chunks"] = [
                s["chunk_index"] for s in doc["shards"] if s["status"] == "delivered"]
            doc["watermark"] = sum(
                s["record_count"] for s in doc["shards"] if s["status"] == "delivered")
            doc["partial_watermark"] = max(doc["partial_watermark"], doc["watermark"])
            if source not in doc["sources"]:
                doc["sources"].append(source)
            self._store_doc(doc)
            self._sync_store_entry(doc)
            self._write_jsonl(
                self._data_dir("events") / "shard_progress_log.jsonl",
                [{"action": "mark_delivered", "batch_code": batch_code,
                  "chunk_index": chunk_index, "idempotent_hit": already,
                  "watermark": doc["watermark"], "source": source, "at": time.time()}],
            )
            return {"batch_code": batch_code, "chunk_index": chunk_index,
                    "idempotent_hit": already, "watermark": doc["watermark"]}

    def _batch_artifact(self, batch_code: str) -> Path:
        return self._data_dir("batches") / f"{batch_code}.jsonl"

    def _source_artifact(self, batch_code: str, source_file: str | Path | None) -> Path:
        if source_file is not None:
            return Path(source_file)
        companion = self._data_dir("batches") / f"{batch_code}_source.jsonl"
        if companion.exists():
            return companion
        return self._batch_artifact(batch_code)

    def _actual_landed_count(self, batch_code: str) -> int:
        """统计批次产物中实际落地的记录数（恢复真相以实际落地为准）。"""
        return len(self._read_jsonl(self._batch_artifact(batch_code)))

    def reconcile_landed(self, batch_code: str, landed_records: int, *,
                         chunk_size: int | None = None, source: str = "staging") -> dict[str, Any]:
        """按实际落地记录数对账分片进度：完整覆盖的分片置为已交付，半片只记水位。"""
        with self._locked(batch_code):
            doc = self._load_doc(batch_code)
            if doc is None:
                cs = int(chunk_size or DEFAULT_CHUNK_SIZE)
                total = max(int(landed_records), 0)
                doc = self._new_doc(batch_code, cs, total, source)
            else:
                if chunk_size and int(chunk_size) != doc["chunk_size"]:
                    raise ValueError(
                        f"批次 {batch_code} 分片大小已固定为 {doc['chunk_size']}，"
                        f"拒绝改写为 {chunk_size}")
                # 防御：登记时未知总量的空文档，在首次对账时按实际落地补建分片。
                if doc["total_records"] == 0 and int(landed_records) > 0:
                    doc["total_records"] = int(landed_records)
                    doc["total_chunks"] = math.ceil(doc["total_records"] / doc["chunk_size"])
                    doc["shards"] = [
                        {
                            "chunk_index": idx,
                            "record_start": idx * doc["chunk_size"],
                            "record_end": min((idx + 1) * doc["chunk_size"],
                                              doc["total_records"]),
                            "record_count": min(doc["chunk_size"],
                                                doc["total_records"] - idx * doc["chunk_size"]),
                            "status": "pending", "delivered_at": None, "source": None,
                        }
                        for idx in range(doc["total_chunks"])
                    ]
            landed = min(int(landed_records), doc["total_records"] or int(landed_records))
            for shard in doc["shards"]:
                if shard["status"] == "delivered":
                    continue
                if landed >= shard["record_end"]:
                    shard["status"] = "delivered"
                    shard["delivered_at"] = time.time()
                    shard["source"] = source
            doc["delivered_chunks"] = [
                s["chunk_index"] for s in doc["shards"] if s["status"] == "delivered"]
            doc["watermark"] = sum(
                s["record_count"] for s in doc["shards"] if s["status"] == "delivered")
            doc["partial_watermark"] = max(landed, doc["watermark"])
            if source not in doc["sources"]:
                doc["sources"].append(source)
            self._store_doc(doc)
            self._sync_store_entry(doc)
            self._write_jsonl(
                self._data_dir("events") / "shard_progress_log.jsonl",
                [{"action": "reconcile_landed", "batch_code": batch_code,
                  "landed_records": landed, "watermark": doc["watermark"],
                  "source": source, "at": time.time()}],
            )
            return {"batch_code": batch_code, "landed_records": landed,
                    "watermark": doc["watermark"],
                    "delivered_chunks": list(doc["delivered_chunks"])}

    # ------------------------------------------------------------------
    # 历史检查点迁移
    # ------------------------------------------------------------------

    def _legacy_candidates(self, batch_code: str) -> list[tuple[str, dict[str, Any]]]:
        """发现批次名下全部有效历史检查点（旧文件名约定，含传输/续传/暂存三种）。"""
        cp_dir = self._data_dir("checkpoints")
        names = [f"batch_{batch_code}.json", f"resume_{batch_code}.json",
                 f"staging_{batch_code}.json"]
        found: list[tuple[str, dict[str, Any]]] = []
        for name in names:
            path = cp_dir / name
            if not path.exists():
                continue
            try:
                with open(path, "r", encoding="utf-8") as f:
                    state = json.load(f)
            except (json.JSONDecodeError, OSError):
                continue
            if isinstance(state, dict):
                found.append((name.removesuffix(".json"), state))
        return found

    @staticmethod
    def _legacy_counter(name: str, state: dict[str, Any]) -> int:
        """从不同类型的旧检查点中提取记录级水位计数。"""
        if name.startswith("batch_"):
            return int(state.get("records_transferred", state.get("watermark", 0)) or 0)
        if name.startswith("resume_"):
            return int(state.get("resume_position", state.get("records_written", 0)) or 0)
        if name.startswith("staging_"):
            return int(state.get("records_landed", 0) or 0)
        return 0

    def migrate_legacy_checkpoint(self, batch_code: str, *, chunk_size: int | None = None,
                                  total_records: int | None = None,
                                  source_file: str | Path | None = None) -> dict[str, Any]:
        """把批次的历史检查点翻译为新版分片进度文档，可重复调用并对迟到写入收敛。

        水位取值规则：先取各类旧检查点中的最大记录水位，再与批次产物中的实际落地
        记录数取较大者（实际落地优先），最后按分片边界翻译为已交付/待传分片集合。
        未显式给出记录总量时，先从源产物读取真实总量，避免按水位建出错误的小分片。
        """
        with self._locked(batch_code):
            candidates = self._legacy_candidates(batch_code)
            actual = self._actual_landed_count(batch_code)
            legacy_watermark = 0
            for name, state in candidates:
                legacy_watermark = max(legacy_watermark, self._legacy_counter(name, state))
            effective = max(legacy_watermark, actual)

            if total_records is None:
                src = self._source_artifact(batch_code, source_file)
                if src.exists():
                    total_records = len(self._read_jsonl(src))
                else:
                    total_records = effective

            doc = self._load_doc(batch_code)
            if doc is None:
                cs = int(chunk_size or DEFAULT_CHUNK_SIZE)
                total = int(total_records or effective or 0)
                doc = self._new_doc(batch_code, cs, total, "legacy_checkpoint")
            else:
                if chunk_size and int(chunk_size) != doc["chunk_size"]:
                    raise ValueError(
                        f"批次 {batch_code} 分片大小已固定为 {doc['chunk_size']}，"
                        f"拒绝改写为 {chunk_size}")
            if total_records and int(total_records) > doc["total_records"]:
                doc["total_records"] = int(total_records)
                doc["total_chunks"] = math.ceil(doc["total_records"] / doc["chunk_size"])
                known = {s["chunk_index"] for s in doc["shards"]}
                for idx in range(doc["total_chunks"]):
                    if idx not in known:
                        doc["shards"].append({
                            "chunk_index": idx,
                            "record_start": idx * doc["chunk_size"],
                            "record_end": min((idx + 1) * doc["chunk_size"],
                                              doc["total_records"]),
                            "record_count": min(doc["chunk_size"],
                                                doc["total_records"] - idx * doc["chunk_size"]),
                            "status": "pending", "delivered_at": None, "source": None,
                        })

            for name, _ in candidates:
                if name not in doc["migrated_from"]:
                    doc["migrated_from"].append(name)
            if "legacy_checkpoint" not in doc["sources"]:
                doc["sources"].append("legacy_checkpoint")

            # 按有效水位翻译分片：只有被完整覆盖的分片才置已交付，半片保留 partial。
            for shard in doc["shards"]:
                if shard["status"] == "delivered":
                    continue
                if effective >= shard["record_end"]:
                    shard["status"] = "delivered"
                    shard["delivered_at"] = time.time()
                    shard["source"] = "legacy_checkpoint"
            doc["delivered_chunks"] = [
                s["chunk_index"] for s in doc["shards"] if s["status"] == "delivered"]
            doc["watermark"] = sum(
                s["record_count"] for s in doc["shards"] if s["status"] == "delivered")
            doc["partial_watermark"] = max(effective, doc["watermark"])

            self._store_doc(doc)
            self._sync_store_entry(doc)
            self._write_jsonl(
                self._data_dir("events") / "shard_progress_log.jsonl",
                [{"action": "migrate_legacy", "batch_code": batch_code,
                  "legacy_checkpoints": [name for name, _ in candidates],
                  "legacy_watermark": legacy_watermark, "actual_landed": actual,
                  "effective_watermark": effective,
                  "delivered_chunks": list(doc["delivered_chunks"]),
                  "at": time.time()}],
            )
            return {
                "batch_code": batch_code,
                "legacy_checkpoints": [name for name, _ in candidates],
                "legacy_watermark": legacy_watermark,
                "actual_landed": actual,
                "effective_watermark": effective,
                "chunk_size": doc["chunk_size"],
                "delivered_chunks": list(doc["delivered_chunks"]),
                "pending_chunks": [
                    s["chunk_index"] for s in doc["shards"] if s["status"] == "pending"],
                "doc_path": str(self._doc_path(batch_code)),
            }

    def migrate_all_legacy(self, chunk_size: int | None = None) -> list[dict[str, Any]]:
        """扫描检查点目录，迁移全部能识别出批次编号的历史检查点。

        单个批次迁移失败（例如既有新版文档的分片大小与传入参数冲突）时跳过并在
        返回项中标注错误，保证批量迁移不会被个别坏批次整体打断。
        """
        cp_dir = self._data_dir("checkpoints")
        batch_codes: set[str] = set()
        for path in cp_dir.glob("*.json"):
            stem = path.stem
            for prefix in ("batch_", "resume_", "staging_"):
                if stem.startswith(prefix) and not stem.endswith("_cleared"):
                    batch_codes.add(stem[len(prefix):])
        results: list[dict[str, Any]] = []
        for code in sorted(batch_codes):
            try:
                results.append(self.migrate_legacy_checkpoint(code, chunk_size=chunk_size))
            except ValueError as exc:
                results.append({"batch_code": code, "skipped": True, "error": str(exc)})
        return results

    # ------------------------------------------------------------------
    # 恢复边界解析（新版优先，旧检查点兜底并即时迁移）
    # ------------------------------------------------------------------

    def resolve_resume_boundary(self, batch_code: str, *, chunk_size: int | None = None,
                                source_file: str | Path | None = None) -> dict[str, Any] | None:
        """解析批次当前可解释的恢复边界。

        优先读取新版分片进度文档；文档缺失（升级失败/回滚/尚未迁移）时自动从有效
        历史检查点即时迁移。返回前再与批次产物的实际落地记录对账，确保旧入口拿到的
        待传分片与真实落地一致。
        """
        with self._locked(batch_code):
            doc = self._load_doc(batch_code)
            migrated = False
            if doc is None:
                candidates = self._legacy_candidates(batch_code)
                if not candidates and not self._batch_artifact(batch_code).exists():
                    return None
                result = self.migrate_legacy_checkpoint(
                    batch_code, chunk_size=chunk_size, source_file=source_file)
                migrated = True
                doc = self._load_doc(batch_code)
                source = "legacy:" + "|".join(result["legacy_checkpoints"]) if result[
                    "legacy_checkpoints"] else "artifact"
            else:
                # 迁移后的迟到旧检查点写入 / 外部落地：重新对账后再给边界。
                actual = self._actual_landed_count(batch_code)
                if actual > doc.get("partial_watermark", 0):
                    self.reconcile_landed(batch_code, actual, source="artifact_reconcile")
                    doc = self._load_doc(batch_code)
                source = "v2:shard_progress"

            # 历史检查点不携带记录总量：从源产物发现真实总量并扩展待传分片，
            # 否则迁移文档只会包含已交付部分，无法定位未完成分片。
            src = self._source_artifact(batch_code, source_file)
            if src.exists():
                source_total = len(self._read_jsonl(src))
                if source_total > int(doc["total_records"]):
                    self.ensure_progress(batch_code, total_records=source_total)
                    doc = self._load_doc(batch_code)

            pending = [s for s in doc["shards"] if s["status"] == "pending"]
            boundary = [
                {
                    "chunk_index": s["chunk_index"],
                    "record_start": s["record_start"],
                    "record_end": s["record_end"],
                    "record_count": s["record_count"],
                    "status": s["status"],
                }
                for s in doc["shards"]
            ]
            return {
                "batch_code": batch_code,
                "schema_version": SCHEMA_VERSION,
                "chunk_size": doc["chunk_size"],
                "total_records": doc["total_records"],
                "total_chunks": doc["total_chunks"],
                "watermark": doc["watermark"],
                "partial_watermark": doc["partial_watermark"],
                "delivered_chunks": list(doc["delivered_chunks"]),
                "pending_chunks": [s["chunk_index"] for s in pending],
                "boundary": boundary,
                "explanation": self._boundary_text(doc),
                "source": source,
                "migrated_on_demand": migrated,
                "migrated_from": list(doc["migrated_from"]),
            }

    # ------------------------------------------------------------------
    # 传输 / 断点续传共享的分片投递
    # ------------------------------------------------------------------

    def transfer_pending_chunks(self, batch_code: str, *,
                                source_file: str | Path | None = None,
                                max_chunks: int | None = None) -> dict[str, Any]:
        """按新版分片进度投递待传分片，传输与断点续传共用本入口。

        分片级调度（只安排待传分片）+ 记录级去重（产物中已存在的 line_no 跳过），
        因此即使历史水位落在分片中间，也不会重发任何已交付记录。``max_chunks`` 可
        模拟切换中断：只投递指定数量的待传分片，再次调用即从断点继续。
        """
        with self._locked(batch_code):
            boundary = self.resolve_resume_boundary(
                batch_code, source_file=source_file)
            if boundary is None:
                return {"error": f"批次 {batch_code} 无可用分片进度或历史检查点"}
            src = self._source_artifact(batch_code, source_file)
            source_records = self._read_jsonl(src)
            if not source_records:
                # 无真实产物时保留旧版模拟续传语义，保证只有历史检查点的老批次也能恢复。
                fallback_count = boundary["total_records"] or 5
                source_records = self._simulate_records(
                    batch_code, count=fallback_count, field_prefix="resume")

            output = self._batch_artifact(batch_code)
            existing = self._read_jsonl(output)
            existing_keys = {r.get("line_no") for r in existing if "line_no" in r}

            chunk_size = boundary["chunk_size"]
            total = boundary["total_records"] or len(source_records)
            todo = boundary["pending_chunks"]
            if max_chunks is not None:
                todo = todo[:max(1, int(max_chunks))]

            appended: list[dict[str, Any]] = []
            done_chunks: list[int] = []
            for chunk_index in todo:
                start = chunk_index * chunk_size
                end = min(start + chunk_size, total)
                chunk_records = source_records[start:end]
                missing = [r for r in chunk_records if r.get("line_no") not in existing_keys]
                for rec in missing:
                    rec = dict(rec)
                    rec.setdefault("batch_code", batch_code)
                    rec["chunk_index"] = chunk_index
                    rec["transferred_at"] = time.time()
                    appended.append(rec)
                    if "line_no" in rec:
                        existing_keys.add(rec["line_no"])
                # 分片内记录已全部在产物中（本次写入或此前已落地）方可确认交付。
                landed_keys = existing_keys
                want_keys = {r.get("line_no") for r in chunk_records}
                if want_keys and want_keys <= landed_keys:
                    self.mark_chunk_delivered(batch_code, chunk_index, source="shared_transfer")
                    done_chunks.append(chunk_index)

            if appended:
                self._write_jsonl(output, appended)

            doc = self._load_doc(batch_code)
            actual = self._actual_landed_count(batch_code)
            doc["partial_watermark"] = max(doc["partial_watermark"], actual)
            self._store_doc(doc)
            self._sync_store_entry(doc)

            # 双写旧版传输检查点：回滚后旧版本仍能读到存量进度。
            self._write_checkpoint(f"batch_{batch_code}", {
                "batch_code": batch_code,
                "records_transferred": actual,
                "watermark": str(doc["watermark"]),
                "chunk_count": doc["total_chunks"],
                "chunk_size": doc["chunk_size"],
                "delivered_chunks": list(doc["delivered_chunks"]),
                "last_transfer_at": time.time(),
            })
            self._write_jsonl(
                self._data_dir("events") / "shard_progress_log.jsonl",
                [{"action": "transfer_pending", "batch_code": batch_code,
                  "appended": len(appended), "chunks_done": done_chunks,
                  "interrupted": max_chunks is not None and len(todo) == int(max_chunks or 0)
                  and len(boundary["pending_chunks"]) > len(done_chunks),
                  "at": time.time()}],
            )
            return {
                "batch_code": batch_code,
                "appended_records": len(appended),
                "appended_line_nos": [r.get("line_no") for r in appended],
                "chunks_done": done_chunks,
                "remaining_chunks": [
                    s["chunk_index"] for s in doc["shards"] if s["status"] == "pending"],
                "actual_landed": actual,
                "completed": not any(s["status"] == "pending" for s in doc["shards"]),
                "artifact_path": str(output),
            }

    def get_progress(self, batch_code: str) -> dict[str, Any] | None:
        """读取新版分片进度文档（不存在返回 None）。"""
        doc = self._load_doc(batch_code)
        return json.loads(json.dumps(doc, default=str)) if doc else None
