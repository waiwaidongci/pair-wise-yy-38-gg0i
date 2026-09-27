from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from .audit import make_entry, utc_now
from .domain import ConflictError, NotFoundError
from .rules import EPSILON, ID_PREFIX, PLAN_STATES, STATES, quota_fits


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
        plan_statuses = ",".join("'" + s + "'" for s in PLAN_STATES)
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
                CREATE TABLE IF NOT EXISTS sections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    shared_limit REAL NOT NULL,
                    review_required INTEGER NOT NULL DEFAULT 0,
                    version INTEGER NOT NULL DEFAULT 1,
                    external_ref TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS ux_sections_external_ref
                    ON sections(external_ref) WHERE external_ref IS NOT NULL;
                CREATE TABLE IF NOT EXISTS plans (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    section_id INTEGER NOT NULL REFERENCES sections(id) ON DELETE CASCADE,
                    reservoir TEXT NOT NULL,
                    planned REAL NOT NULL,
                    actual REAL,
                    seq INTEGER NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ({plan_statuses})),
                    version INTEGER NOT NULL DEFAULT 1,
                    external_ref TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    executed_by TEXT,
                    executed_at TEXT,
                    UNIQUE(section_id, seq)
                );
                CREATE UNIQUE INDEX IF NOT EXISTS ux_plans_external_ref
                    ON plans(external_ref) WHERE external_ref IS NOT NULL;
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

    def create_section(self, name: str, shared_limit: float,
                       external_ref: Optional[str], actor: str) -> Dict[str, Any]:
        now = utc_now()
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO sections(name, shared_limit, review_required, version,
                       external_ref, created_by, created_at, updated_at)
                       VALUES(?,?,0,1,?,?,?,?)""",
                    (name, shared_limit, external_ref, actor, now, now),
                )
                section_id = int(cur.lastrowid)
        except sqlite3.IntegrityError as exc:
            raise ConflictError("external_ref已存在") from exc
        return self.get_section(section_id)

    def get_section(self, section_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM sections WHERE id=?", (section_id,)).fetchone()
        if row is None:
            raise NotFoundError("控制断面不存在")
        return dict(row)

    def list_sections(self) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute("SELECT * FROM sections ORDER BY id").fetchall()
        return [dict(row) for row in rows]

    def _occupied_locked(self, section_id: int) -> float:
        row = self.conn.execute(
            """SELECT COALESCE(SUM(CASE WHEN status='allocated' THEN planned
                   WHEN status='executed' THEN actual ELSE 0 END),0) AS used
               FROM plans WHERE section_id=?""",
            (section_id,),
        ).fetchone()
        return float(row["used"])

    def section_occupied(self, section_id: int) -> float:
        with self._lock:
            return self._occupied_locked(section_id)

    def create_plan(self, section_id: int, reservoir: str, planned: float,
                    external_ref: Optional[str], actor: str) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            section = self.conn.execute(
                "SELECT * FROM sections WHERE id=?", (section_id,)).fetchone()
            if section is None:
                raise NotFoundError("控制断面不存在")
            row = self.conn.execute(
                "SELECT COALESCE(MAX(seq),0) AS m FROM plans WHERE section_id=?",
                (section_id,)).fetchone()
            seq = int(row["m"]) + 1
            blocked = self.conn.execute(
                """SELECT 1 FROM plans WHERE section_id=? AND status IN ('queued','frozen')
                   LIMIT 1""",
                (section_id,),
            ).fetchone() is not None
            occupied = self._occupied_locked(section_id)
            fits = quota_fits(float(section["shared_limit"]), occupied, planned)
            status = "allocated" if not blocked and fits else "queued"
            try:
                cur = self.conn.execute(
                    """INSERT INTO plans(section_id, reservoir, planned, actual, seq, status,
                       version, external_ref, created_by, created_at)
                       VALUES(?,?,?,NULL,?,?,1,?,?,?)""",
                    (section_id, reservoir, planned, seq, status, external_ref, actor, now),
                )
                plan_id = int(cur.lastrowid)
            except sqlite3.IntegrityError as exc:
                raise ConflictError("external_ref已存在") from exc
            row = self.conn.execute("SELECT * FROM plans WHERE id=?", (plan_id,)).fetchone()
        return dict(row)

    def get_plan(self, plan_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM plans WHERE id=?", (plan_id,)).fetchone()
        if row is None:
            raise NotFoundError("计划不存在")
        return dict(row)

    def list_plans(self, section_id: int) -> List[Dict[str, Any]]:
        self.get_section(section_id)
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM plans WHERE section_id=? ORDER BY seq", (section_id,)
            ).fetchall()
        return [dict(row) for row in rows]

    def _promote_locked(self, section_id: int, shared_limit: float) -> List[int]:
        promoted: List[int] = []
        occupied = self._occupied_locked(section_id)
        rows = self.conn.execute(
            """SELECT * FROM plans WHERE section_id=? AND status='queued' ORDER BY seq""",
            (section_id,),
        ).fetchall()
        for row in rows:
            planned = float(row["planned"])
            if not quota_fits(shared_limit, occupied, planned):
                break
            self.conn.execute(
                "UPDATE plans SET status='allocated', version=version+1 WHERE id=?",
                (row["id"],),
            )
            occupied += planned
            promoted.append(int(row["id"]))
        return promoted

    def execute_plan(self, plan_id: int, actual: float, actor: str) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            row = self.conn.execute("SELECT * FROM plans WHERE id=?", (plan_id,)).fetchone()
            if row is None:
                raise NotFoundError("计划不存在")
            plan = dict(row)
            if plan["status"] == "executed":
                raise ConflictError("计划已执行完成，不能改写")
            if plan["status"] != "allocated":
                raise ConflictError("计划未持有额度，不能回填执行")
            section = self.conn.execute(
                "SELECT * FROM sections WHERE id=?", (plan["section_id"],)).fetchone()
            planned = float(plan["planned"])
            self.conn.execute(
                """UPDATE plans SET status='executed', actual=?, executed_by=?,
                   executed_at=?, version=version+1 WHERE id=?""",
                (actual, actor, now, plan_id),
            )
            promoted: List[int] = []
            frozen: List[int] = []
            if actual > planned + EPSILON:
                self.conn.execute(
                    """UPDATE plans SET status='frozen', version=version+1
                       WHERE section_id=? AND status IN ('allocated','queued')""",
                    (plan["section_id"],),
                )
                frozen = [int(r["id"]) for r in self.conn.execute(
                    """SELECT id FROM plans WHERE section_id=? AND status='frozen'
                       ORDER BY seq""",
                    (plan["section_id"],),
                ).fetchall()]
                self.conn.execute(
                    """UPDATE sections SET review_required=1, version=version+1,
                       updated_at=? WHERE id=?""",
                    (now, plan["section_id"]),
                )
            else:
                promoted = self._promote_locked(
                    plan["section_id"], float(section["shared_limit"]))
            updated = self.conn.execute(
                "SELECT * FROM plans WHERE id=?", (plan_id,)).fetchone()
        return {
            "plan": dict(updated), "promoted": promoted, "frozen": frozen,
            "released": max(0.0, planned - actual), "over": max(0.0, actual - planned),
        }

    def review_section(self, section_id: int, actor: str) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            section = self.conn.execute(
                "SELECT * FROM sections WHERE id=?", (section_id,)).fetchone()
            if section is None:
                raise NotFoundError("控制断面不存在")
            if int(section["review_required"]) != 1:
                raise ConflictError("断面当前无需复核")
            restored = [int(r["id"]) for r in self.conn.execute(
                """SELECT id FROM plans WHERE section_id=? AND status='frozen'
                   ORDER BY seq""",
                (section_id,),
            ).fetchall()]
            self.conn.execute(
                """UPDATE plans SET status='queued', version=version+1
                   WHERE section_id=? AND status='frozen'""",
                (section_id,),
            )
            self.conn.execute(
                """UPDATE sections SET review_required=0, version=version+1,
                   updated_at=? WHERE id=?""",
                (now, section_id),
            )
            promoted = self._promote_locked(section_id, float(section["shared_limit"]))
        return {"restored": restored, "promoted": promoted}

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

    def close(self) -> None:
        with self._lock:
            self.conn.close()
