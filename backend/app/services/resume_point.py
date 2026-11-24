"""断点续传领域服务：维护传输中断后已完成与未完成分片的断点登记与恢复位置。

本模块在传输批次因分片中断、重试超限等原因失败时，创建断点记录并支持从
恢复位置重新启动传输，保障数据传输的完整性。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ResumePointService(BaseModuleService):
    MODULE = "resume_point"
    ENTITY = "续传断点"
    OPERATION_TERM = "水位"
    LIST_FIELDS = ["断点编号", "批次编号", "已完成分片", "未完成分片", "恢复位置", "重试次数", "断点类型", "续传水位"]
    REQUIRED_FIELDS = ["断点编号", "批次编号", "已完成分片"]
    STATUS_ORDER = ["草稿", "待续传", "续传中", "已恢复"]
    ACTION_RULES = {"启动续传": "续传中", "确认恢复": "已恢复", "标记放弃": "已恢复"}
    NEGATIVE_ACTIONS = ["标记放弃"]

    def create_resume_point(self, batch_code: str, completed_chunks: int, pending_chunks: int) -> dict[str, Any]:
        """为传输中断批次创建断点续传记录，登记已完成分片、未完成分片和恢复位置。"""
        resume_code = f"RESU-{batch_code}-{time.time_ns()}"
        values = {
            "断点编号": resume_code,
            "批次编号": batch_code,
            "已完成分片": str(completed_chunks),
            "未完成分片": str(pending_chunks),
            "恢复位置": str(completed_chunks),
            "重试次数": "0",
            "断点类型": "分片中断",
            "续传水位": str(completed_chunks),
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建断点续传记录失败: {missing}")
        # 写入断点日志
        self._write_jsonl(
            self._data_dir("events") / "resume_point_log.jsonl",
            [{"action": "create_resume_point", "resume_code": resume_code, "batch_code": batch_code,
              "completed_chunks": completed_chunks, "pending_chunks": pending_chunks,
              "at": time.time()}]
        )

        # 旧入口继续写旧版检查点：升级失败或回滚后，存量检查点仍可被旧版本读取。
        self._write_checkpoint(f"resume_{batch_code}", {
            "batch_code": batch_code,
            "retry_count": 0,
            "resume_position": completed_chunks,
            "records_written": 0,
            "registered_at": time.time(),
        })

        # 注意：此处不预建新版分片文档。批次 B「已登记恢复点但尚未执行」时，新版文档
        # 应在首次迁移/恢复时由有效历史检查点翻译生成，避免空文档遮蔽真实分片边界。

        return {"resume_code": resume_code, "resume_id": entry["id"], "batch_code": batch_code,
                "completed_chunks": completed_chunks, "pending_chunks": pending_chunks}

    def resume_transfer(self, resume_id: int, max_chunks: int | None = None,
                        chunk_size: int | None = None) -> dict[str, Any]:
        """执行断点续传：按新版分片进度从未完成分片恢复，并保留旧检查点双写。

        恢复流程：

        1. 通过旧断点条目拿到批次编号，调用新版分片进度服务解析可解释的分片边界；
           新版文档缺失时（升级失败/回滚/未迁移），由分片服务从有效历史检查点即时迁移。
        2. 以分片为最小调度单位，只投递待传分片；产物中已落地的记录按 line_no 去重，
           任何已交付分片都不会被重发。
        3. 续传后同步推进续传水位与重试次数，并继续写旧版 resume 检查点，保证回滚可读。
        """
        resume_entry = self.get_entry(resume_id)
        if resume_entry is None:
            return {"error": f"续传断点 {resume_id} 不存在"}

        entry, msg = self.run_action(resume_id, "启动续传")
        if entry is None:
            return {"error": msg, "resume_id": resume_id}

        batch_code = resume_entry["批次编号"]
        retry_count = int(resume_entry.get("重试次数", 0)) + 1
        legacy_position = int(resume_entry.get("恢复位置", 0))

        sp_svc = self._safe_import("app.services.shard_progress", "ShardProgressService")()
        boundary = sp_svc.resolve_resume_boundary(batch_code, chunk_size=chunk_size)

        if boundary is not None:
            # 新版分片边界可解析：传输与续传共享同一投递入口。
            transfer_result = sp_svc.transfer_pending_chunks(
                batch_code, max_chunks=max_chunks)
            if "error" in transfer_result:
                return {"error": transfer_result["error"], "resume_id": resume_id}

            written = transfer_result["appended_records"]
            new_position = transfer_result["actual_landed"]

            # 续传产物文件（保留原有命名约定，便于旧版工具链识别）：只写本次追加的记录。
            resume_file = self._data_dir("batches") / f"{batch_code}_resume_{retry_count}.jsonl"
            appended_nos = set(transfer_result["appended_line_nos"])
            if appended_nos:
                artifact_records = self._read_jsonl(sp_svc._batch_artifact(batch_code))
                self._write_jsonl(
                    resume_file, [r for r in artifact_records if r.get("line_no") in appended_nos])

            entry["重试次数"] = str(retry_count)
            entry["续传水位"] = str(new_position)
            entry["已完成分片"] = str(len(boundary["delivered_chunks"])
                                      + len(transfer_result["chunks_done"]))

            # 旧版续传检查点双写：回滚后旧版本仍可从记录级水位恢复。
            self._write_checkpoint(f"resume_{batch_code}", {
                "batch_code": batch_code,
                "retry_count": retry_count,
                "resume_position": new_position,
                "records_written": written,
                "delivered_chunks": sp_svc.get_progress(batch_code)["delivered_chunks"],
                "shard_schema_version": 2,
                "last_resumed_at": time.time(),
            })

            if transfer_result.get("completed"):
                final_entry, _ = self.run_action(resume_id, "确认恢复")

            return {
                "resume_code": resume_entry["断点编号"],
                "batch_code": batch_code,
                "retry_count": retry_count,
                "records_resumed": written,
                "new_position": new_position,
                "resume_boundary": {
                    "pending_chunks": transfer_result["remaining_chunks"],
                    "chunks_done_this_time": transfer_result["chunks_done"],
                    "explanation": boundary["explanation"],
                    "source": boundary["source"],
                    "migrated_on_demand": boundary["migrated_on_demand"],
                },
            }

        # 兜底：既无新版文档也无有效历史检查点时，保留旧版模拟续传行为。
        batch_file = self._data_dir("batches") / f"{batch_code}.jsonl"
        existing_records = self._read_jsonl(batch_file)
        pending_records = existing_records[legacy_position:] if legacy_position < len(
            existing_records) else []
        if not pending_records:
            pending_records = self._simulate_records(batch_code, count=5, field_prefix="resume")

        resume_file = self._data_dir("batches") / f"{batch_code}_resume_{retry_count}.jsonl"
        written = self._write_jsonl(resume_file, pending_records)
        new_position = legacy_position + written
        entry["重试次数"] = str(retry_count)
        entry["续传水位"] = str(new_position)

        self._write_checkpoint(f"resume_{batch_code}", {
            "batch_code": batch_code,
            "retry_count": retry_count,
            "resume_position": new_position,
            "records_written": written,
            "last_resumed_at": time.time(),
        })

        if len(pending_records) > 0 and written > 0:
            self.run_action(resume_id, "确认恢复")

        return {
            "resume_code": resume_entry["断点编号"],
            "batch_code": batch_code,
            "retry_count": retry_count,
            "records_resumed": written,
            "new_position": new_position,
            "resume_boundary": None,
        }

    def get_pending_resume_points(self) -> list[dict[str, Any]]:
        """获取所有待续传的断点记录列表。"""
        return [r for r in store.rows(self.MODULE) if r.get("status") == "待续传"]