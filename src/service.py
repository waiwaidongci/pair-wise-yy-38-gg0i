from __future__ import annotations

import threading
from typing import Any, Dict, Optional

from .domain import (ConflictError, ValidationError, ensure_role,
                     normalize_severity, require_number, require_text)
from .queueing import (ALLOCATED, EXECUTE_ROLES, EXECUTED, PLAN_ENTITY,
                       QUEUE_VIEW_ROLES, REGISTER_PLAN_ROLES,
                       REGISTER_SECTION_ROLES, REVIEW_DECISIONS, REVIEW_ROLES,
                       REJECTED, SECTION_ENTITY, is_over_discharge,
                       occupied_quota, reallocate)
from .repository import Repository
from .rules import (AUDIT_ROLES, CREATE_ROLES, ENTITY, RECORD_ROLES, TITLE,
                    VIEW_ROLES, completion_blockers, escalation_required,
                    priority_score, response_deadline_hours, role_for_transition,
                    validate_transition)


class Service:
    def __init__(self, repository: Repository):
        self.repository = repository
        self._queue_lock = threading.RLock()

    def _view(self, role: str) -> None:
        ensure_role(role, VIEW_ROLES)

    def create_item(self, payload: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, CREATE_ROLES)
        actor = require_text(actor, "actor", 100)
        title = require_text(payload.get("title"), "title", 200)
        description = require_text(payload.get("description"), "description")
        severity = normalize_severity(payload.get("severity"))
        quantity = require_number(payload.get("quantity", 0), "quantity")
        threshold = require_number(payload.get("threshold", 1), "threshold", 0.000001)
        external_ref = payload.get("external_ref")
        if external_ref is not None:
            external_ref = require_text(external_ref, "external_ref", 100)
        item = self.repository.create_item(title, description, severity, quantity,
                                           threshold, external_ref, actor)
        self.repository.append_audit("create", ENTITY, item["id"], actor, {
            "title": title, "severity": severity, "quantity": quantity,
            "priority": priority_score(severity, quantity, threshold),
        })
        return self.enrich(item)

    def add_record(self, item_id: int, payload: Dict[str, Any], actor: str,
                   role: str) -> Dict[str, Any]:
        ensure_role(role, RECORD_ROLES)
        actor = require_text(actor, "actor", 100)
        kind = require_text(payload.get("kind"), "kind", 100)
        detail = require_text(payload.get("detail"), "detail")
        status = payload.get("status", "open")
        if status not in ("open", "closed"):
            raise ValueError("status必须是open或closed")
        external_ref = payload.get("external_ref")
        if external_ref is not None:
            external_ref = require_text(external_ref, "external_ref", 100)
        record = self.repository.add_record(item_id, kind, detail, status,
                                            external_ref, actor)
        self.repository.append_audit("record", ENTITY, item_id, actor, {
            "record_id": record["id"], "kind": kind, "status": status,
        })
        return record

    def transition(self, item_id: int, target: str, expected_version: int,
                   actor: str, role: str) -> Dict[str, Any]:
        actor = require_text(actor, "actor", 100)
        item = self.repository.get_item(item_id)
        validate_transition(item["status"], target)
        ensure_role(role, role_for_transition(target))
        if not isinstance(expected_version, int) or expected_version < 1:
            raise ValueError("expected_version必须是正整数")
        blockers = completion_blockers(target, self.repository.open_record_count(item_id))
        if blockers:
            from .domain import ConflictError
            raise ConflictError("；".join(blockers))
        updated = self.repository.transition_item(item_id, target, expected_version, actor)
        self.repository.append_audit("transition", ENTITY, item_id, actor, {
            "from": item["status"], "to": target,
            "escalation_required": escalation_required(
                item["severity"], item["quantity"], item["threshold"]),
        })
        return self.enrich(updated)

    def get_item(self, item_id: int, role: str) -> Dict[str, Any]:
        self._view(role)
        return self.enrich(self.repository.get_item(item_id))

    def list_items(self, role: str, status: Optional[str] = None) -> list:
        self._view(role)
        return [self.enrich(item) for item in self.repository.list_items(status)]

    def list_records(self, item_id: int, role: str) -> list:
        self._view(role)
        return self.repository.list_records(item_id)

    def audit(self, role: str, item_id: Optional[int] = None) -> list:
        ensure_role(role, AUDIT_ROLES)
        return self.repository.list_audit(item_id)

    # ----- 联调排队 -----

    def register_section(self, payload: Dict[str, Any], actor: str,
                         role: str) -> Dict[str, Any]:
        ensure_role(role, REGISTER_SECTION_ROLES)
        actor = require_text(actor, "actor", 100)
        name = require_text(payload.get("name"), "name", 100)
        shared_cap = require_number(payload.get("shared_cap"), "shared_cap", 0.0)
        if shared_cap <= 0:
            raise ValidationError("shared_cap必须大于0")
        with self._queue_lock:
            section = self.repository.create_section(name, shared_cap, actor)
            self.repository.append_audit(
                "register_section", SECTION_ENTITY, section["id"], actor,
                {"name": name, "shared_cap": shared_cap})
            return self.section_view(section["id"], role)

    def register_plan(self, section_id: int, payload: Dict[str, Any],
                      actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, REGISTER_PLAN_ROLES)
        actor = require_text(actor, "actor", 100)
        reservoir = require_text(payload.get("reservoir"), "reservoir", 100)
        planned = require_number(payload.get("planned"), "planned", 0.0)
        if planned <= 0:
            raise ValidationError("planned必须大于0")
        with self._queue_lock:
            section = self.repository.get_section(section_id)
            plan = self.repository.create_plan(section_id, reservoir, planned, actor)
            if not section["frozen"]:
                self._promote_waiting(section_id)
            self.repository.append_audit(
                "register_plan", PLAN_ENTITY, plan["id"], actor,
                {"section_id": section_id, "seq": plan["seq"],
                 "reservoir": reservoir, "planned": planned,
                 "status_after": self.repository.get_plan(plan["id"])["status"]})
            return self.section_view(section_id, role)

    def execute_plan(self, plan_id: int, payload: Dict[str, Any],
                     actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, EXECUTE_ROLES)
        actor = require_text(actor, "actor", 100)
        actual = require_number(payload.get("actual"), "actual", 0.0)
        with self._queue_lock:
            plan = self.repository.get_plan(plan_id)
            if plan["status"] == EXECUTED:
                raise ConflictError("计划已执行并回填，不能改写")
            section_id = plan["section_id"]
            self.repository.mark_plan_executed(plan_id, actual, actor)
            over = is_over_discharge(plan["planned"], actual)
            if over:
                # 偏大：冻结后续计划，交总工复核，不做再分配
                self.repository.freeze_later_plans(section_id, plan["seq"])
            else:
                # 偏小或持平：释放剩余额度给后续待排队计划
                self._promote_waiting(section_id)
            self.repository.append_audit(
                "execute_plan", PLAN_ENTITY, plan_id, actor,
                {"section_id": section_id, "planned": plan["planned"],
                 "actual": actual, "over_discharge": over,
                 "released": max(0.0, plan["planned"] - actual)})
            return self.section_view(section_id, role)

    def review_plan(self, plan_id: int, payload: Dict[str, Any],
                    actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, REVIEW_ROLES)
        actor = require_text(actor, "actor", 100)
        decision = payload.get("decision")
        if decision not in REVIEW_DECISIONS:
            raise ValidationError("decision必须是approve或reject")
        note = payload.get("note")
        if note is not None:
            note = require_text(note, "note", 2000)
        with self._queue_lock:
            plan = self.repository.get_plan(plan_id)
            section_id = plan["section_id"]
            new_status = ALLOCATED if decision == "approve" else REJECTED
            self.repository.review_plan(plan_id, new_status, actor)
            promoted = []
            if not self.repository.section_has_frozen(section_id):
                self.repository.clear_section_frozen(section_id)
                promoted = self._promote_waiting(section_id)
            self.repository.append_audit(
                "review_plan", PLAN_ENTITY, plan_id, actor,
                {"section_id": section_id, "decision": decision,
                 "note": note, "promoted": promoted})
            return self.section_view(section_id, role)

    def list_sections(self, role: str) -> list:
        ensure_role(role, QUEUE_VIEW_ROLES)
        return [self._section_summary(section)
                for section in self.repository.list_sections()]

    def section_view(self, section_id: int, role: str) -> Dict[str, Any]:
        ensure_role(role, QUEUE_VIEW_ROLES)
        section = self.repository.get_section(section_id)
        plans = self.repository.list_plans(section_id)
        result = self._section_summary(section)
        result["plans"] = [self._plan_view(plan) for plan in plans]
        return result

    def _promote_waiting(self, section_id: int) -> list:
        """按提交顺序把装得下的待排队计划提升为已分配，返回提升的计划id。"""
        section = self.repository.get_section(section_id)
        plans = self.repository.list_plans(section_id)
        promotions = reallocate(plans, section["shared_cap"])
        self.repository.mark_plans_allocated(promotions)
        return promotions

    def _section_summary(self, section: Dict[str, Any]) -> Dict[str, Any]:
        plans = self.repository.list_plans(section["id"])
        occupied = occupied_quota(plans)
        result = dict(section)
        result["frozen"] = bool(section["frozen"])
        result["occupied"] = round(occupied, 9)
        result["remaining"] = round(float(section["shared_cap"]) - occupied, 9)
        counts = {status: 0 for status in
                  ("waiting", "allocated", "executed", "frozen", "rejected")}
        for plan in plans:
            counts[plan["status"]] += 1
        result["counts"] = counts
        return result

    @staticmethod
    def _plan_view(plan: Dict[str, Any]) -> Dict[str, Any]:
        result = dict(plan)
        result["planned"] = float(plan["planned"])
        if plan.get("actual") is not None:
            result["actual"] = float(plan["actual"])
        return result

    @staticmethod
    def enrich(item: Dict[str, Any]) -> Dict[str, Any]:
        result = dict(item)
        result["priority"] = priority_score(
            item["severity"], item["quantity"], item["threshold"])
        result["deadline_hours"] = response_deadline_hours(
            item["severity"], item["quantity"], item["threshold"])
        result["escalation_required"] = escalation_required(
            item["severity"], item["quantity"], item["threshold"])
        return result
