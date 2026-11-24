"""验收测试：历史批次检查点迁移到新版分片进度记录的完整场景。

覆盖题面全部约束：

* 两个不同恢复边界的老批次（边界落在分片内 vs 分片整界），迁移与批次 A 的迟到
  旧检查点写入交错，再分别恢复两批；
* 暂存实际记录、旧检查点与新版分片文档之间的对应关系；
* 升级失败/回滚后存量旧检查点仍可读取，缺失新版文档时旧入口即时迁移；
* 切换中断后续做只投递未完成分片，任何已交付分片/记录不重发；
* 传输与断点续传共享新版分片进度记录，旧入口可从历史检查点定位未完成分片。
"""
from __future__ import annotations

import json
import threading
import uuid
from pathlib import Path

import pytest

from app.services.base import DATA_ROOT
from app.services import service_for
from app.services.shard_progress import ShardProgressService
from app.services.resume_point import ResumePointService
from app.services.staging_area import StagingAreaService
from app.services.transfer_batch import TransferBatchService
from app.store import store


@pytest.fixture(autouse=True)
def reset_store():
    """每个用例前重置内存表，文件产物用唯一批次编号隔离并在结束后清理。"""
    from app.seed import SEED_ROWS
    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
    CREATED_FILES.clear()
    yield
    for path in CREATED_FILES:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
    CREATED_FILES.clear()


CREATED_FILES: list[Path] = []


def _unique_batch(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def _write_source(batch_code: str, count: int) -> Path:
    """生成批次源产物（带稳定 line_no，供记录级去重核对）。"""
    sp = ShardProgressService()
    path = sp._data_dir("batches") / f"{batch_code}_source.jsonl"
    records = [
        {"batch_key": batch_code, "line_no": i + 1, "trans_a": f"v{i}", "trans_b": i * 7}
        for i in range(count)
    ]
    sp._write_jsonl(path, records)
    CREATED_FILES.append(path)
    return path


def _land_artifact(batch_code: str, line_nos: list[int]) -> Path:
    """模拟旧版本已实际落地到批次产物的记录（恢复真相以实际落地为准）。"""
    sp = ShardProgressService()
    path = sp._batch_artifact(batch_code)
    sp._write_jsonl(path, [
        {"batch_key": batch_code, "line_no": n, "trans_a": f"v{n - 1}",
         "trans_b": (n - 1) * 7, "chunk_index": (n - 1) // 1000}
        for n in line_nos
    ])
    CREATED_FILES.append(path)
    return path


def _write_legacy_checkpoint(batch_code: str, kind: str, state: dict) -> Path:
    sp = ShardProgressService()
    path = sp._write_checkpoint(f"{kind}_{batch_code}", state)
    CREATED_FILES.append(path)
    return path


def _migration_events(batch_code: str) -> list[dict]:
    sp = ShardProgressService()
    return [
        e for e in sp._read_jsonl(
            sp._data_dir("events") / "shard_progress_log.jsonl")
        if e.get("batch_code") == batch_code and e.get("action") == "migrate_legacy"
    ]


def _line_nos(batch_code: str) -> list[int]:
    sp = ShardProgressService()
    return [r["line_no"] for r in sp._read_jsonl(sp._batch_artifact(batch_code))]


class TestTwoLegacyBatchesInterleavedMigration:
    """场景一：两个不同恢复边界的老批次，迁移与 A 的迟到写入交错，分别恢复。"""

    def test_migration_interleaves_with_late_write_and_resume_each(self, reset_store):
        sp = ShardProgressService()
        # 批次 A：分片大小 3，10 条，旧水位 4 -> 边界落在分片 1 内部（已交付分片 0，半片 1）。
        batch_a = _unique_batch("LEG-A")
        src_a = _write_source(batch_a, 10)
        _land_artifact(batch_a, [1, 2, 3, 4])
        _write_legacy_checkpoint(batch_a, "batch", {
            "batch_code": batch_a, "records_transferred": 4, "watermark": "4",
            "chunk_count": 4, "last_transfer_at": 1.0,
        })
        # 批次 B：分片大小 5，10 条，旧水位 5 -> 整界（分片 0 完整，分片 1 待传）。
        batch_b = _unique_batch("LEG-B")
        src_b = _write_source(batch_b, 10)
        _land_artifact(batch_b, [1, 2, 3, 4, 5])
        _write_legacy_checkpoint(batch_b, "batch", {
            "batch_code": batch_b, "records_transferred": 5, "watermark": "5",
            "chunk_count": 2, "last_transfer_at": 1.0,
        })
        _write_legacy_checkpoint(batch_b, "resume", {
            "batch_code": batch_b, "retry_count": 1, "resume_position": 5,
            "records_written": 0, "last_resumed_at": 1.0,
        })

        # 先迁移 A（此时 A 的旧持久化“还没完成”：只落地到水位 4 的检查点）。
        first_a = sp.migrate_legacy_checkpoint(batch_a, chunk_size=3, total_records=10)
        assert first_a["effective_watermark"] == 4
        assert first_a["delivered_chunks"] == [0]
        assert first_a["pending_chunks"] == [1, 2, 3]

        # A 的迟到旧检查点写入完成：旧批次 A 的持久化请求此刻才落盘，水位推进到 6，
        # 实际产物也由旧版本追加到 6（模拟交错期间 A 的写入仍在继续）。
        _land_artifact(batch_a, [5, 6])
        _write_legacy_checkpoint(batch_a, "batch", {
            "batch_code": batch_a, "records_transferred": 6, "watermark": "6",
            "chunk_count": 4, "last_transfer_at": 2.0,
        })

        # 交错期间迁移 B，并恢复 B。
        mig_b = sp.migrate_legacy_checkpoint(batch_b, chunk_size=5, total_records=10)
        assert mig_b["delivered_chunks"] == [0]
        assert mig_b["pending_chunks"] == [1]
        resume_b = sp.transfer_pending_chunks(batch_b, source_file=src_b)
        assert resume_b["chunks_done"] == [1]
        assert resume_b["completed"] is True
        assert _line_nos(batch_b) == list(range(1, 11))  # 1..10 各一次，无重发

        # 再次迁移 A：迟到写入必须被收敛，且不重发已交付分片。
        second_a = sp.migrate_legacy_checkpoint(batch_a, chunk_size=3, total_records=10)
        assert second_a["effective_watermark"] == 6
        assert second_a["delivered_chunks"] == [0, 1]
        doc_a = sp.get_progress(batch_a)
        assert [s["status"] for s in doc_a["shards"]] == [
            "delivered", "delivered", "pending", "pending"]
        # 半片信息保留在 partial_watermark，分片 1 已完整覆盖才标记交付。
        assert doc_a["partial_watermark"] == 6
        assert doc_a["watermark"] == 6

        # 恢复 A：分片级调度 + 记录级去重，只补 line_no 7..10。
        resume_a = sp.transfer_pending_chunks(batch_a, source_file=src_a)
        assert resume_a["chunks_done"] == [2, 3]
        assert resume_a["appended_line_nos"] == [7, 8, 9, 10]
        assert resume_a["completed"] is True
        assert _line_nos(batch_a) == list(range(1, 11))

        # 两个批次恢复边界不同且都可解释。
        bnd_a = sp.resolve_resume_boundary(batch_a)
        bnd_b = sp.resolve_resume_boundary(batch_b)
        assert bnd_a["chunk_size"] == 3 and bnd_b["chunk_size"] == 5
        assert bnd_a["pending_chunks"] == [] and bnd_b["pending_chunks"] == []
        assert "分片0[0,3)=delivered" in bnd_a["explanation"]
        assert "分片1[5,10)=delivered" in bnd_b["explanation"]

    def test_repeated_migration_is_convergent_and_idempotent(self, reset_store):
        sp = ShardProgressService()
        batch = _unique_batch("LEG-IDEM")
        _write_source(batch, 7)
        _land_artifact(batch, [1, 2, 3])
        _write_legacy_checkpoint(batch, "batch", {
            "batch_code": batch, "records_transferred": 3, "watermark": "3"})
        first = sp.migrate_legacy_checkpoint(batch, chunk_size=3, total_records=7)
        second = sp.migrate_legacy_checkpoint(batch, chunk_size=3, total_records=7)
        assert first["delivered_chunks"] == second["delivered_chunks"] == [0]
        doc = sp.get_progress(batch)
        assert doc["migrated_from"] == [f"batch_{batch}"]  # 来源不重复登记
        # 幂等重放投递不产生重复记录。
        sp.transfer_pending_chunks(batch)
        sp.transfer_pending_chunks(batch)
        assert _line_nos(batch) == list(range(1, 8))


class TestLegacyResumeEntryLocatesPendingShards:
    """场景二：旧批次 B 已登记恢复点但尚未执行，旧入口从历史检查点定位未完成分片。"""

    def test_resume_point_entry_triggers_on_demand_migration(self, reset_store):
        sp = ShardProgressService()
        rp = ResumePointService()
        batch = _unique_batch("LEG-RESU")
        src = _write_source(batch, 8)
        _land_artifact(batch, [1, 2])
        # 升级前留下的传输检查点（已交付 2/8，分片大小 4 -> 分片 0 内半片）。
        _write_legacy_checkpoint(batch, "batch", {
            "batch_code": batch, "records_transferred": 2, "watermark": "2"})

        # 新版上线后通过旧入口登记恢复点：只写旧 resume 检查点，不预建新版文档。
        registered = rp.create_resume_point(batch, completed_chunks=2, pending_chunks=6)
        assert sp.get_progress(batch) is None
        legacy_resume = sp._read_checkpoint(f"resume_{batch}")
        assert legacy_resume["resume_position"] == 2

        # 执行续传：新版文档缺失 -> 从有效历史检查点即时迁移后按分片恢复。
        result = rp.resume_transfer(registered["resume_id"], chunk_size=4)
        assert "error" not in result
        boundary = result["resume_boundary"]
        assert boundary["migrated_on_demand"] is True
        assert boundary["source"].startswith("legacy:")
        # 水位 2 落在分片 0 内部：分片 0 仍按待传调度，但记录级去重使已落地的
        # line_no 1-2 不重发，本次只追加 3-4；分片 1（5-8）随后一并投递。
        assert result["records_resumed"] == 6
        assert _line_nos(batch) == list(range(1, 9))
        assert len(set(_line_nos(batch))) == 8  # 无任何重复交付
        # 半片信息保留在 partial_watermark，全部分片最终交付。
        doc = sp.get_progress(batch)
        assert doc["delivered_chunks"] == [0, 1]
        assert doc["watermark"] == 8
        assert doc["partial_watermark"] == 8

        doc = sp.get_progress(batch)
        assert doc["migrated_from"] == [f"batch_{batch}", f"resume_{batch}"]
        # 旧检查点升级后仍可读取（回滚兼容）；迁移时的原始水位记录在迁移事件日志中。
        assert sp._read_checkpoint(f"batch_{batch}") is not None
        assert sp._read_checkpoint(f"resume_{batch}")["shard_schema_version"] == 2
        events = _migration_events(batch)
        assert events[0]["legacy_watermark"] == 2
        assert events[0]["actual_landed"] == 2

    def test_resume_with_interrupted_max_chunks_then_continue(self, reset_store):
        sp = ShardProgressService()
        rp = ResumePointService()
        batch = _unique_batch("LEG-CUT")
        src = _write_source(batch, 12)
        _land_artifact(batch, [1, 2, 3])
        _write_legacy_checkpoint(batch, "batch", {
            "batch_code": batch, "records_transferred": 3, "watermark": "3"})
        registered = rp.create_resume_point(batch, 3, 9)

        # 第一次续传只允许推进 1 个待传分片，模拟切换中断。
        first = rp.resume_transfer(registered["resume_id"], max_chunks=1, chunk_size=3)
        assert first["resume_boundary"]["chunks_done_this_time"] == [1]
        assert first["resume_boundary"]["pending_chunks"] == [2, 3]
        mid_line_nos = _line_nos(batch)
        assert mid_line_nos == list(range(1, 7))  # 3 旧 + 分片 1 的 3 条

        # 再次续传做完剩余分片。
        second = rp.resume_transfer(registered["resume_id"], chunk_size=3)
        assert second["resume_boundary"]["pending_chunks"] == []
        assert _line_nos(batch) == list(range(1, 13))
        # 中断前后无重复 line_no。
        assert len(set(_line_nos(batch))) == 12


class TestRollbackKeepsLegacyCheckpointsReadable:
    """场景三：升级失败/回滚后，存量检查点仍能读取并重建分片边界。"""

    def test_corrupted_or_missing_v2_doc_falls_back_to_legacy(self, reset_store):
        sp = ShardProgressService()
        batch = _unique_batch("LEG-ROLL")
        _write_source(batch, 6)
        _land_artifact(batch, [1, 2, 3])
        _write_legacy_checkpoint(batch, "batch", {
            "batch_code": batch, "records_transferred": 3, "watermark": "3"})
        sp.migrate_legacy_checkpoint(batch, chunk_size=3, total_records=6)
        assert sp.get_progress(batch) is not None

        # 模拟回滚：删除新版分片文档与锁文件，旧检查点原封不动。
        sp._doc_path(batch).unlink()
        sp._lock_path(batch).unlink(missing_ok=True)
        assert sp.get_progress(batch) is None
        legacy = sp._read_checkpoint(f"batch_{batch}")
        assert legacy["records_transferred"] == 3

        # 旧入口解析边界时即时重建，恢复结果与升级前一致。
        boundary = sp.resolve_resume_boundary(batch, chunk_size=3)
        assert boundary["migrated_on_demand"] is True
        assert boundary["delivered_chunks"] == [0]
        assert boundary["pending_chunks"] == [1]
        result = sp.transfer_pending_chunks(batch)
        assert result["appended_line_nos"] == [4, 5, 6]
        assert _line_nos(batch) == list(range(1, 7))

    def test_corrupted_json_doc_is_rebuilt_not_fatal(self, reset_store):
        sp = ShardProgressService()
        batch = _unique_batch("LEG-BADJSON")
        _write_source(batch, 4)
        _land_artifact(batch, [1, 2])
        _write_legacy_checkpoint(batch, "batch", {
            "batch_code": batch, "records_transferred": 2, "watermark": "2"})
        sp._doc_path(batch).parent.mkdir(parents=True, exist_ok=True)
        sp._doc_path(batch).write_text("{ broken json", encoding="utf-8")
        boundary = sp.resolve_resume_boundary(batch, chunk_size=2)
        assert boundary is not None
        assert boundary["delivered_chunks"] == [0]


class TestStagingActualRecordsCorrespondence:
    """场景四：暂存实际记录与两种检查点、新版分片文档的对应关系。"""

    def test_staging_landing_reconciles_shard_progress(self, reset_store):
        tb = TransferBatchService()
        sa = StagingAreaService()
        sp = ShardProgressService()
        batch_info = tb.create_batch("EXTR-SHARD-CORR", 9, "TARG-0001")
        transfer = tb.start_transfer(batch_info["batch_id"], chunk_size=3)
        batch_code = batch_info["batch_code"]

        doc = sp.get_progress(batch_code)
        assert doc["total_chunks"] == 3
        assert doc["delivered_chunks"] == [0, 1, 2]

        # 暂存落地：实际记录数对账分片进度，并产生三类可互相印证的产物。
        staging = sa.land_batch(batch_code, 9)
        assert staging["records_landed"] == 9
        assert staging["shard_reconcile"]["watermark"] == 9

        # 1) 暂存实际记录
        staged = sa._read_jsonl(
            sa._data_dir("staging") / f"{staging['staging_code']}.jsonl")
        assert len(staged) == 9
        # 2) 旧检查点（传输 + 暂存）与 3) 新版分片文档指向同一水位。
        assert sa._read_checkpoint(f"batch_{batch_code}")["records_transferred"] == 9
        assert sa._read_checkpoint(
            f"staging_{staging['staging_code']}")["records_landed"] == 9
        assert sp.get_progress(batch_code)["watermark"] == 9

        # 通用资源接口可查询新版分片进度条目。
        items, total = sp.list_entries(filters={"批次编号": batch_code})
        assert total == 1 and items[0]["分片总数"] == "3"
        assert items[0]["已交付分片"] == "0,1,2"
        assert "分片0[0,3)=delivered" in items[0]["边界说明"]
        assert transfer["records_transferred"] == 9


class TestSwitchInterruptionAndContinue:
    """场景五：传输过程中切换中断，续做不重发已交付分片。"""

    def test_batch_continue_transfer_after_interruption(self, reset_store):
        tb = TransferBatchService()
        sp = ShardProgressService()
        batch_info = tb.create_batch("EXTR-SHARD-CUT", 10, "TARG-0001")
        batch_code = batch_info["batch_code"]
        # 准备源产物，令传输按分片中断可控：先建分片文档再逐片投递。
        src = _write_source(batch_code, 10)
        sp.ensure_progress(batch_code, total_records=10, chunk_size=4, source="test")

        first = sp.transfer_pending_chunks(batch_code, source_file=src, max_chunks=1)
        assert first["chunks_done"] == [0]
        assert first["remaining_chunks"] == [1, 2]
        assert first["appended_line_nos"] == [1, 2, 3, 4]

        # 通过传输批次服务的续做入口再推一片（切换中断后的正式续做通道）。
        second = tb.continue_transfer(batch_info["batch_id"], max_chunks=1)
        assert second["chunks_done"] == [1]
        assert second["appended_line_nos"] == [5, 6, 7, 8]

        third = tb.continue_transfer(batch_info["batch_id"])
        assert third["completed"] is True
        assert third["appended_line_nos"] == [9, 10]
        assert _line_nos(batch_code) == list(range(1, 11))

        # 双写的旧检查点水位与新版文档一致，回滚后可读。
        legacy = sp._read_checkpoint(f"batch_{batch_code}")
        assert legacy["records_transferred"] == 10
        assert legacy["delivered_chunks"] == [0, 1, 2]
        assert sp.get_progress(batch_code)["watermark"] == 10

    def test_chunk_size_is_frozen_after_first_commit(self, reset_store):
        sp = ShardProgressService()
        batch = _unique_batch("LEG-FROZEN")
        _write_source(batch, 10)
        sp.ensure_progress(batch, total_records=10, chunk_size=5, source="test")
        with pytest.raises(ValueError):
            sp.ensure_progress(batch, total_records=10, chunk_size=4)
        with pytest.raises(ValueError):
            sp.migrate_legacy_checkpoint(batch, chunk_size=4)


class TestMigrateAllLegacyScan:
    """场景七：批量扫描检查点目录，按批次编号迁移全部历史检查点。"""

    def test_migrate_all_legacy_discovers_batches_and_skips_cleared(self, reset_store):
        sp = ShardProgressService()
        b1 = _unique_batch("LEG-SCAN1")
        b2 = _unique_batch("LEG-SCAN2")
        _write_source(b1, 6)
        _land_artifact(b1, [1, 2, 3])
        _write_legacy_checkpoint(b1, "batch", {
            "batch_code": b1, "records_transferred": 3, "watermark": "3"})
        _write_source(b2, 4)
        _land_artifact(b2, [1, 2])
        _write_legacy_checkpoint(b2, "resume", {
            "batch_code": b2, "retry_count": 1, "resume_position": 2})
        # 清空标记类检查点不应被识别为批次。
        cleared = sp._write_checkpoint("staging_STAG-GHOST_cleared", {"status": "cleared"})
        CREATED_FILES.append(cleared)

        results = sp.migrate_all_legacy(chunk_size=2)
        by_batch = {r["batch_code"]: r for r in results}
        assert b1 in by_batch and b2 in by_batch
        assert all("GHOST" not in r["batch_code"] for r in results)
        assert by_batch[b1]["delivered_chunks"] == [0]
        assert by_batch[b2]["legacy_watermark"] == 2
        assert sp.get_progress(b2)["total_chunks"] == 2  # 源产物 4 条 / 分片 2
        assert all(not r.get("skipped") for r in results if r["batch_code"] in {b1, b2})
        # store 中可列出两个分片进度条目。
        items, total = sp.list_entries(page=1, size=200)
        codes = {it["批次编号"] for it in items}
        assert {b1, b2} <= codes

    def test_migrate_all_legacy_skips_conflicting_batch_without_aborting(self, reset_store):
        sp = ShardProgressService()
        ok_batch = _unique_batch("LEG-SCANOK")
        conflict_batch = _unique_batch("LEG-SCANCONFLICT")
        _write_source(ok_batch, 4)
        _land_artifact(ok_batch, [1, 2])
        _write_legacy_checkpoint(ok_batch, "batch", {
            "batch_code": ok_batch, "records_transferred": 2})
        _write_source(conflict_batch, 4)
        _land_artifact(conflict_batch, [1, 2])
        _write_legacy_checkpoint(conflict_batch, "batch", {
            "batch_code": conflict_batch, "records_transferred": 2})
        # 先以分片大小 3 固定冲突批次的新版文档。
        sp.migrate_legacy_checkpoint(conflict_batch, chunk_size=3)

        results = sp.migrate_all_legacy(chunk_size=2)
        flags = {r["batch_code"]: r for r in results}
        assert flags[conflict_batch]["skipped"] is True
        assert "分片大小已固定" in flags[conflict_batch]["error"]
        assert not flags[ok_batch].get("skipped")


class TestConcurrentMarkingAndLocking:
    """场景六：并发投递同一分片时锁与幂等保证只生效一次。"""

    def test_concurrent_mark_delivered_is_idempotent(self, reset_store):
        sp = ShardProgressService()
        batch = _unique_batch("LEG-LOCK")
        sp.ensure_progress(batch, total_records=6, chunk_size=2, source="test")
        errors: list[Exception] = []

        def mark():
            try:
                sp.mark_chunk_delivered(batch, 1, source="thread")
            except Exception as exc:  # pragma: no cover - 并发异常需显式暴露
                errors.append(exc)

        threads = [threading.Thread(target=mark) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        assert not errors
        doc = sp.get_progress(batch)
        assert doc["delivered_chunks"] == [1]
        assert doc["watermark"] == 2  # 只含分片 1 的 2 条，未重复计数
        assert sum(1 for s in doc["shards"] if s["status"] == "delivered") == 1
