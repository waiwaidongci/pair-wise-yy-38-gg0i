from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from .audit import make_entry, utc_now
from .domain import ConflictError, NotFoundError
from .queueing import (ALLOCATED, EXECUTED, FROZEN, PLAN_STATUSES, WAITING)
from .rules import ID_PREFIX, STATES


class Repository:
    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self._create_schema()

    def _create_schema(self) -> None:
        statuses = ",".join("'" + s.replace("'", "''") + "'" for s in STATES)
        plan_statuses = ",".join("'" + s + "'" for s in PLAN_STATUSES)
        with self.conn:
            self.conn.executescript(f"""
                CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    quantity REAL NOT NULL DEFAULT 0,
                    threshold REAL NOT NULL DEFAULT 1,
                    status TEXT NOT NULL CHECK(status IN ({statuses})),
                    version INTEGER NOT NULL DEFAULT 1,
                    external_ref TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS ux_items_external_ref
                    ON items(external_ref) WHERE external_ref IS NOT NULL;
                CREATE TABLE IF NOT EXISTS records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                    kind TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open'
                        CHECK(status IN ('open','closed')),
                    external_ref TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(item_id, external_ref)
                );
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    action TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_id INTEGER NOT NULL,
                    actor TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    entry_hash TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS queue_sections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    shared_cap REAL NOT NULL,
                    frozen INTEGER NOT NULL DEFAULT 0,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS queue_plans (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    section_id INTEGER NOT NULL REFERENCES queue_sections(id) ON DELETE CASCADE,
                    seq INTEGER NOT NULL,
                    reservoir TEXT NOT NULL,
                    planned REAL NOT NULL,
                    actual REAL,
                    status TEXT NOT NULL DEFAULT '{WAITING}'
                        CHECK(status IN ({plan_statuses})),
                    executed_at TEXT,
                    executed_by TEXT,
                    reviewed_at TEXT,
                    reviewed_by TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(section_id, seq)
                );
                CREATE INDEX IF NOT EXISTS ix_queue_plans_section
                    ON queue_plans(section_id, seq);
            """)

    @staticmethod
    def _item(row: sqlite3.Row) -> Dict[str, Any]:
        return dict(row)

    def create_item(self, title: str, description: str, severity: str,
                    quantity: float, threshold: float, external_ref: Optional[str],
                    actor: str) -> Dict[str, Any]:
        now = utc_now()
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO items(title, description, severity, quantity, threshold,
                       status, version, external_ref, created_by, created_at, updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                    (title, description, severity, quantity, threshold, STATES[0], 1,
                     external_ref, actor, now, now),
                )
                item_id = int(cur.lastrowid)
        except sqlite3.IntegrityError as exc:
            raise ConflictError("external_ref已存在") from exc
        return self.get_item(item_id)

    def get_item(self, item_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
        if row is None:
            raise NotFoundError("项目不存在")
        return self._item(row)

    def list_items(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM items"
        params: tuple = ()
        if status:
            sql += " WHERE status=?"
            params = (status,)
        sql += " ORDER BY id DESC"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        return [self._item(row) for row in rows]

    def transition_item(self, item_id: int, target: str, expected_version: int,
                        actor: str) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """UPDATE items SET status=?, version=version+1, updated_at=?
                   WHERE id=? AND version=?""",
                (target, now, item_id, expected_version),
            )
            if cur.rowcount == 0:
                exists = self.conn.execute("SELECT 1 FROM items WHERE id=?", (item_id,)).fetchone()
                if exists is None:
                    raise NotFoundError("项目不存在")
                raise ConflictError("版本冲突，请刷新后重试")
        return self.get_item(item_id)

    def add_record(self, item_id: int, kind: str, detail: str, status: str,
                   external_ref: Optional[str], actor: str) -> Dict[str, Any]:
        now = utc_now()
        self.get_item(item_id)
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO records(item_id, kind, detail, status, external_ref,
                       created_by, created_at) VALUES(?,?,?,?,?,?,?)""",
                    (item_id, kind, detail, status, external_ref, actor, now),
                )
                record_id = int(cur.lastrowid)
        except sqlite3.IntegrityError as exc:
            raise ConflictError("记录唯一标识已存在") from exc
        with self._lock:
            row = self.conn.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
        return dict(row)

    def list_records(self, item_id: int) -> List[Dict[str, Any]]:
        self.get_item(item_id)
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM records WHERE item_id=? ORDER BY id", (item_id,)
            ).fetchall()
        return [dict(row) for row in rows]

    def open_record_count(self, item_id: int) -> int:
        with self._lock:
            row = self.conn.execute(
                "SELECT COUNT(*) AS n FROM records WHERE item_id=? AND status='open'",
                (item_id,),
            ).fetchone()
        return int(row["n"])

    def append_audit(self, action: str, entity_type: str, entity_id: int,
                     actor: str, detail: dict) -> Dict[str, Any]:
        with self._lock, self.conn:
            row = self.conn.execute(
                "SELECT entry_hash FROM audit_events ORDER BY id DESC LIMIT 1"
            ).fetchone()
            previous = row["entry_hash"] if row else "GENESIS"
            event = make_entry(action, entity_type, entity_id, actor, detail, previous)
            cur = self.conn.execute(
                """INSERT INTO audit_events(action, entity_type, entity_id, actor, detail,
                   previous_hash, entry_hash, created_at) VALUES(?,?,?,?,?,?,?,?)""",
                (event["action"], event["entity_type"], event["entity_id"], event["actor"],
                 json.dumps(event["detail"], ensure_ascii=False, sort_keys=True),
                 event["previous_hash"], event["entry_hash"], event["created_at"]),
            )
            event_id = int(cur.lastrowid)
        event["id"] = event_id
        return event

    def list_audit(self, entity_id: Optional[int] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM audit_events"
        params: tuple = ()
        if entity_id is not None:
            sql += " WHERE entity_id=?"
            params = (entity_id,)
        sql += " ORDER BY id"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["detail"] = json.loads(item["detail"])
            result.append(item)
        return result

    def verify_audit_chain(self) -> bool:
        from .audit import calculate_hash
        with self._lock:
            rows = self.conn.execute("SELECT * FROM audit_events ORDER BY id").fetchall()
        previous = "GENESIS"
        for row in rows:
            if row["previous_hash"] != previous:
                return False
            payload = {
                "action": row["action"], "entity_type": row["entity_type"],
                "entity_id": row["entity_id"], "actor": row["actor"],
                "detail": json.loads(row["detail"]), "created_at": row["created_at"],
            }
            if calculate_hash(previous, payload) != row["entry_hash"]:
                return False
            previous = row["entry_hash"]
        return True

    # ----- 联调排队 -----

    def create_section(self, name: str, shared_cap: float,
                       actor: str) -> Dict[str, Any]:
        now = utc_now()
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO queue_sections(name, shared_cap, created_by, created_at)
                       VALUES(?,?,?,?)""",
                    (name, shared_cap, actor, now),
                )
                section_id = int(cur.lastrowid)
        except sqlite3.IntegrityError as exc:
            raise ConflictError("控制断面名称已存在") from exc
        return self.get_section(section_id)

    def get_section(self, section_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM queue_sections WHERE id=?", (section_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError("控制断面不存在")
        return dict(row)

    def list_sections(self) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM queue_sections ORDER BY id"
            ).fetchall()
        return [dict(row) for row in rows]

    def create_plan(self, section_id: int, reservoir: str, planned: float,
                    actor: str) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            row = self.conn.execute(
                "SELECT COALESCE(MAX(seq),0) AS last_seq FROM queue_plans WHERE section_id=?",
                (section_id,),
            ).fetchone()
            seq = int(row["last_seq"]) + 1
            cur = self.conn.execute(
                """INSERT INTO queue_plans(section_id, seq, reservoir, planned, status,
                   created_by, created_at) VALUES(?,?,?,?,?,?,?)""",
                (section_id, seq, reservoir, planned, WAITING, actor, now),
            )
            plan_id = int(cur.lastrowid)
        return self.get_plan(plan_id)

    def get_plan(self, plan_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM queue_plans WHERE id=?", (plan_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError("联调计划不存在")
        return dict(row)

    def list_plans(self, section_id: int) -> List[Dict[str, Any]]:
        self.get_section(section_id)
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM queue_plans WHERE section_id=? ORDER BY seq",
                (section_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def update_plan_status(self, plan_id: int, statuses: tuple,
                           status: str) -> None:
        with self._lock, self.conn:
            cur = self.conn.execute(
                "UPDATE queue_plans SET status=? WHERE id=? AND status IN (%s)"
                % ",".join("?" for _ in statuses),
                (status, plan_id, *statuses),
            )
            if cur.rowcount == 0:
                exists = self.conn.execute(
                    "SELECT 1 FROM queue_plans WHERE id=?", (plan_id,)
                ).fetchone()
                if exists is None:
                    raise NotFoundError("联调计划不存在")
                current = self.conn.execute(
                    "SELECT status FROM queue_plans WHERE id=?", (plan_id,)
                ).fetchone()
                raise ConflictError(f"计划当前状态为{current['status']}，不能改为{status}")

    def mark_plan_executed(self, plan_id: int, actual: float,
                           actor: str) -> None:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """UPDATE queue_plans SET status=?, actual=?, executed_at=?, executed_by=?
                   WHERE id=? AND status=?""",
                (EXECUTED, actual, now, actor, plan_id, ALLOCATED),
            )
            if cur.rowcount == 0:
                exists = self.conn.execute(
                    "SELECT 1 FROM queue_plans WHERE id=?", (plan_id,)
                ).fetchone()
                if exists is None:
                    raise NotFoundError("联调计划不存在")
                current = self.conn.execute(
                    "SELECT status FROM queue_plans WHERE id=?", (plan_id,)
                ).fetchone()
                raise ConflictError(
                    f"计划当前状态为{current['status']}，无法回填"
                    if current["status"] == EXECUTED else "只有已分配的计划可以回填执行结果")

    def freeze_later_plans(self, section_id: int, seq: int) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                """UPDATE queue_plans SET status=?
                   WHERE section_id=? AND seq>? AND status IN (?,?)""",
                (FROZEN, section_id, seq, WAITING, ALLOCATED),
            )
            self.conn.execute(
                "UPDATE queue_sections SET frozen=1 WHERE id=?", (section_id,)
            )

    def mark_plans_allocated(self, plan_ids: List[int]) -> None:
        if not plan_ids:
            return
        with self._lock, self.conn:
            self.conn.execute(
                "UPDATE queue_plans SET status=? WHERE id IN (%s)"
                % ",".join("?" for _ in plan_ids),
                (ALLOCATED, *plan_ids),
            )

    def review_plan(self, plan_id: int, status: str, actor: str) -> None:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """UPDATE queue_plans SET status=?, reviewed_at=?, reviewed_by=?
                   WHERE id=? AND status=?""",
                (status, now, actor, plan_id, FROZEN),
            )
            if cur.rowcount == 0:
                exists = self.conn.execute(
                    "SELECT 1 FROM queue_plans WHERE id=?", (plan_id,)
                ).fetchone()
                if exists is None:
                    raise NotFoundError("联调计划不存在")
                current = self.conn.execute(
                    "SELECT status FROM queue_plans WHERE id=?", (plan_id,)
                ).fetchone()
                raise ConflictError(
                    f"计划当前状态为{current['status']}，只有冻结计划可复核")

    def section_has_frozen(self, section_id: int) -> bool:
        with self._lock:
            row = self.conn.execute(
                "SELECT COUNT(*) AS n FROM queue_plans WHERE section_id=? AND status=?",
                (section_id, FROZEN),
            ).fetchone()
        return int(row["n"]) > 0

    def clear_section_frozen(self, section_id: int) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                "UPDATE queue_sections SET frozen=0 WHERE id=?", (section_id,)
            )

    def close(self) -> None:
        with self._lock:
            self.conn.close()