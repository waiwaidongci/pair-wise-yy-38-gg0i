from __future__ import annotations

from typing import Any, Dict, Optional

from .domain import (ValidationError, ensure_role, normalize_severity,
                     require_number, require_text)
from .repository import Repository
from .rules import (AUDIT_ROLES, CREATE_ROLES, ENTITY, PLAN_ENTITY,
                    PLAN_EXECUTE_ROLES, PLAN_REGISTER_ROLES, RECORD_ROLES,
                    REVIEW_ROLES, SECTION_CREATE_ROLES, SECTION_ENTITY,
                    QUEUE_VIEW_ROLES, TITLE, VIEW_ROLES, completion_blockers,
                    escalation_required, priority_score, response_deadline_hours,
                    role_for_transition, validate_transition)


class Service:
    def __init__(self, repository: Repository):
        self.repository = repository

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

    def create_section(self, payload: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, SECTION_CREATE_ROLES)
        actor = require_text(actor, "actor", 100)
        name = require_text(payload.get("name"), "name", 200)
        shared_limit = require_number(payload.get("shared_limit"), "shared_limit", 0.0)
        if shared_limit <= 0:
            raise ValidationError("shared_limit必须大于0")
        external_ref = payload.get("external_ref")
        if external_ref is not None:
            external_ref = require_text(external_ref, "external_ref", 100)
        section = self.repository.create_section(name, shared_limit, external_ref, actor)
        self.repository.append_audit("section_create", SECTION_ENTITY, section["id"],
                                     actor, {"name": name, "shared_limit": shared_limit})
        return self.enrich_section(section)

    def list_sections(self, role: str) -> list:
        ensure_role(role, QUEUE_VIEW_ROLES)
        return [self.enrich_section(s) for s in self.repository.list_sections()]

    def get_section_queue(self, section_id: int, role: str) -> Dict[str, Any]:
        ensure_role(role, QUEUE_VIEW_ROLES)
        section = self.enrich_section(self.repository.get_section(section_id))
        plans = [self.enrich_plan(p) for p in self.repository.list_plans(section_id)]
        ahead = section["occupied"]
        for plan in plans:
            if plan["status"] == "queued":
                plan["ahead"] = round(ahead, 6)
                plan["shortfall"] = round(
                    max(0.0, ahead + plan["planned"] - section["shared_limit"]), 6)
                ahead += plan["planned"]
        section["plans"] = plans
        return section

    def register_plan(self, section_id: int, payload: Dict[str, Any], actor: str,
                      role: str) -> Dict[str, Any]:
        ensure_role(role, PLAN_REGISTER_ROLES)
        actor = require_text(actor, "actor", 100)
        reservoir = require_text(payload.get("reservoir"), "reservoir", 200)
        planned = require_number(payload.get("planned"), "planned", 0.0)
        if planned <= 0:
            raise ValidationError("planned必须大于0")
        external_ref = payload.get("external_ref")
        if external_ref is not None:
            external_ref = require_text(external_ref, "external_ref", 100)
        plan = self.repository.create_plan(section_id, reservoir, planned,
                                           external_ref, actor)
        self.repository.append_audit("plan_register", PLAN_ENTITY, plan["id"], actor, {
            "section_id": section_id, "reservoir": reservoir, "planned": planned,
            "seq": plan["seq"], "status": plan["status"],
        })
        return self.enrich_plan(plan)

    def execute_plan(self, plan_id: int, payload: Dict[str, Any], actor: str,
                     role: str) -> Dict[str, Any]:
        ensure_role(role, PLAN_EXECUTE_ROLES)
        actor = require_text(actor, "actor", 100)
        actual = require_number(payload.get("actual"), "actual", 0.0)
        result = self.repository.execute_plan(plan_id, actual, actor)
        plan = result["plan"]
        self.repository.append_audit("plan_execute", PLAN_ENTITY, plan_id, actor, {
            "section_id": plan["section_id"], "planned": plan["planned"],
            "actual": actual, "released": result["released"], "over": result["over"],
            "promoted": result["promoted"], "frozen": result["frozen"],
        })
        return {
            "plan": self.enrich_plan(plan), "promoted": result["promoted"],
            "frozen": result["frozen"], "released": result["released"],
            "over": result["over"],
        }

    def review_section(self, section_id: int, actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, REVIEW_ROLES)
        actor = require_text(actor, "actor", 100)
        result = self.repository.review_section(section_id, actor)
        self.repository.append_audit("section_review", SECTION_ENTITY, section_id,
                                     actor, result)
        return result

    def enrich_section(self, section: Dict[str, Any]) -> Dict[str, Any]:
        result = dict(section)
        occupied = self.repository.section_occupied(section["id"])
        result["occupied"] = round(occupied, 6)
        result["remaining"] = round(max(0.0, section["shared_limit"] - occupied), 6)
        result["review_required"] = bool(section["review_required"])
        return result

    @staticmethod
    def enrich_plan(plan: Dict[str, Any]) -> Dict[str, Any]:
        result = dict(plan)
        result["deviation"] = (
            round(plan["actual"] - plan["planned"], 6)
            if plan["actual"] is not None else None)
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
