"""传输批次领域服务：维护传输批次的分片组织、记录条数与字节总量登记及批次水位推进。

本模块读取抽取窗口产物文件后创建传输批次，分片写入传输数据文件，
传输完成后自动驱动暂存落地和校验核对，形成窗口抽取→批次传输→落地校验的贯通链路。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class TransferBatchService(BaseModuleService):
    MODULE = "transfer_batch"
    ENTITY = "传输批次"
    OPERATION_TERM = "水位"
    LIST_FIELDS = ["批次编号", "窗口编号", "目标编号", "记录条数", "字节总量", "分片大小", "传输状态", "批次水位"]
    REQUIRED_FIELDS = ["批次编号", "窗口编号", "目标编号"]
    STATUS_ORDER = ["草稿", "待传输", "传输中", "已完成", "已失败"]
    ACTION_RULES = {"启动传输": "传输中", "标记完成": "已完成", "标记失败": "已失败"}
    NEGATIVE_ACTIONS = ["标记失败"]

    def create_batch(self, window_code: str, record_count: int, target_code: str = "TARG-0001") -> dict[str, Any]:
        """基于抽取窗口创建传输批次，传入产物文件的 record_count 并写入批次事件日志。"""
        batch_code = f"TRAN-{window_code}-{time.time_ns()}"
        values = {
            "批次编号": batch_code,
            "窗口编号": window_code,
            "目标编号": target_code,
            "记录条数": str(record_count),
            "字节总量": "0",
            "分片大小": "1000",
            "传输状态": "草稿",
            "批次水位": "0",
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建传输批次失败: {missing}")
        # 写入批次创建事件
        self._write_jsonl(
            self._data_dir("events") / "batch_create_log.jsonl",
            [{"action": "create_batch", "batch_code": batch_code, "window_code": window_code,
              "record_count": record_count, "target": target_code, "at": time.time()}]
        )
        return {"batch_code": batch_code, "batch_id": entry["id"], "window_code": window_code,
                "record_count": record_count, "target_code": target_code}

    def start_transfer(self, batch_id: int, chunk_size: int = 3) -> dict[str, Any]:
        """启动传输批次执行：读取抽取窗口产物文件，按分片大小写入传输产物文件并推进水位。

        本方法执行实际的批量数据传输：从抽取产物文件读取数据，切分为分片，
        写入传输产物文件（jsonl），计算字节总量，然后自动触发暂存落地和校验核对。
        """
        batch_entry = self.get_entry(batch_id)
        if batch_entry is None:
            return {"error": f"传输批次 {batch_id} 不存在"}

        entry, msg = self.run_action(batch_id, "启动传输")
        if entry is None:
            return {"error": msg, "batch_id": batch_id}

        batch_code = batch_entry["批次编号"]
        window_code = batch_entry.get("窗口编号", "unknown")

        # 读取上游抽取窗口的产物文件
        artifact_dir = self._data_dir("extracted")
        source_file = artifact_dir / f"{window_code}.jsonl" if window_code != "unknown" else None

        records = []
        if source_file and source_file.exists():
            records = self._read_jsonl(source_file)
        else:
            # 如果上游产物文件不存在，生成模拟数据
            records = self._simulate_records(batch_code, count=int(batch_entry.get("记录条数", 10)),
                                              field_prefix="trans")

        # 分片写入传输产物文件
        output_file = self._data_dir("batches") / f"{batch_code}.jsonl"
        total_bytes = 0
        total_written = 0
        chunks = []

        # 登记新版分片进度文档：传输与断点续传共享同一份分片边界真相。
        sp_svc = self._safe_import("app.services.shard_progress", "ShardProgressService")()
        sp_svc.ensure_progress(
            batch_code, total_records=len(records), chunk_size=chunk_size,
            source="transfer_batch")

        for i in range(0, len(records), chunk_size):
            chunk_index = i // chunk_size
            chunk = records[i:i + chunk_size]
            for rec in chunk:
                rec["batch_code"] = batch_code
                rec["chunk_index"] = chunk_index
                rec["transferred_at"] = time.time()
            self._write_jsonl(output_file, chunk)
            chunk_bytes = sum(len(str(r).encode("utf-8")) for r in chunk)
            chunks.append({"chunk_index": chunk_index, "record_count": len(chunk),
                           "bytes": chunk_bytes})
            total_bytes += chunk_bytes
            total_written += len(chunk)
            # 每个分片落地后推进新版分片进度（幂等，重跑不会重复计数）。
            sp_svc.mark_chunk_delivered(batch_code, chunk_index, source="transfer_batch")

        # 更新批次水位和字节总量
        entry["字节总量"] = str(total_bytes)
        entry["记录条数"] = str(total_written)
        entry["批次水位"] = str(total_written)

        # 写入传输水位断点（旧版检查点保留双写，回滚后旧版本仍可读取）
        self._write_checkpoint(f"batch_{batch_code}", {
            "batch_code": batch_code,
            "window_code": window_code,
            "records_transferred": total_written,
            "total_bytes": total_bytes,
            "chunk_count": len(chunks),
            "watermark": str(total_written),
            "last_transfer_at": time.time(),
        })
        # 用实际落地记录对账新版分片进度，确保两种检查点指向同一恢复边界。
        sp_svc.reconcile_landed(batch_code, total_written, source="transfer_batch")

        result = {
            "batch_code": batch_code,
            "window_code": window_code,
            "records_transferred": total_written,
            "total_bytes": total_bytes,
            "chunks": len(chunks),
            "artifact_path": str(output_file),
        }

        # 传输完成后自动触发暂存落地
        try:
            sa_svc = self._safe_import("app.services.staging_area", "StagingAreaService")()
            staging_result = sa_svc.land_batch(batch_code, total_written)
            result["auto_staging"] = staging_result
        except Exception as e:
            result["auto_staging"] = {"error": str(e)}

        # 传输完成后自动创建校验核对任务
        try:
            cv_svc = self._safe_import("app.services.checksum_verify", "ChecksumVerifyService")()
            verify_result = cv_svc.create_verification(batch_code, total_written)
            result["auto_verify"] = verify_result
        except Exception as e:
            result["auto_verify"] = {"error": str(e)}

        # 传输完成后自动创建到达对账单
        try:
            ar_svc = self._safe_import("app.services.arrival_reconcile", "ArrivalReconcileService")()
            reconcile_result = ar_svc.create_reconciliation(batch_code, total_written)
            result["auto_reconcile"] = reconcile_result
        except Exception as e:
            result["auto_reconcile"] = {"error": str(e)}

        return result

    def continue_transfer(self, batch_id: int, max_chunks: int | None = None) -> dict[str, Any]:
        """切换中断后的续做入口：按新版分片进度只投递未完成分片，不重发已交付分片。"""
        batch_entry = self.get_entry(batch_id)
        if batch_entry is None:
            return {"error": f"传输批次 {batch_id} 不存在"}
        batch_code = batch_entry["批次编号"]
        sp_svc = self._safe_import("app.services.shard_progress", "ShardProgressService")()
        result = sp_svc.transfer_pending_chunks(batch_code, max_chunks=max_chunks)
        if "error" in result:
            return result
        entry, _ = self.run_action(batch_id, "启动传输")
        if entry is not None:
            entry["批次水位"] = str(result["actual_landed"])
            if result.get("completed"):
                done, _ = self.run_action(batch_id, "标记完成")
                if done is not None:
                    result["batch_status"] = done["status"]
        return result

    def mark_completed(self, batch_id: int) -> dict[str, Any]:
        """标记传输批次已完成，写入完成事件日志并记录批次汇总统计。"""
        entry, msg = self.run_action(batch_id, "标记完成")
        if entry is None:
            return {"error": msg, "batch_id": batch_id}
        batch_code = entry["批次编号"]
        # 从产物文件验证批次完整性
        artifact_file = self._data_dir("batches") / f"{batch_code}.jsonl"
        records = self._read_jsonl(artifact_file)
        checksum = self._compute_checksum(str(records))
        self._write_jsonl(
            self._data_dir("events") / "batch_complete_log.jsonl",
            [{"action": "complete", "batch_code": batch_code, "record_count": len(records),
              "checksum": checksum, "completed_at": time.time()}]
        )
        return {"batch_code": batch_code, "status": "已完成", "record_count": len(records),
                "checksum": checksum}

    def mark_failed(self, batch_id: int, reason: str = "") -> dict[str, Any]:
        """标记传输批次失败，写入失败事件日志并记录中断原因。"""
        entry, msg = self.run_action(batch_id, "标记失败")
        if entry is None:
            return {"error": msg, "batch_id": batch_id}
        batch_code = entry["批次编号"]
        self._write_jsonl(
            self._data_dir("events") / "batch_fail_log.jsonl",
            [{"action": "fail", "batch_code": batch_code, "reason": reason, "failed_at": time.time()}]
        )
        return {"batch_code": batch_code, "status": "已失败", "reason": reason}