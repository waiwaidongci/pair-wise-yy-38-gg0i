from __future__ import annotations

from typing import Any, Dict, List

WAITING = "waiting"        # 待排队：未拿到共享额度
ALLOCATED = "allocated"    # 已分配：按提交顺序占住额度，可执行
EXECUTED = "executed"      # 已执行：已回填实际下泄量，不可改写
FROZEN = "frozen"          # 已冻结：上游超泄，等待总工复核
REJECTED = "rejected"      # 已驳回：总工复核不通过
PLAN_STATUSES = [WAITING, ALLOCATED, EXECUTED, FROZEN, REJECTED]
ACTIVE_STATUSES = (WAITING, ALLOCATED)

SECTION_ENTITY = "控制断面"
PLAN_ENTITY = "联调计划"

REGISTER_SECTION_ROLES = {"dispatcher"}
REGISTER_PLAN_ROLES = {"dispatcher"}
EXECUTE_ROLES = {"dispatcher"}
REVIEW_ROLES = {"chief_engineer"}
QUEUE_VIEW_ROLES = {"duty_officer", "chief_engineer", "dispatcher", "viewer"}
REVIEW_DECISIONS = {"approve", "reject"}

EPS = 1e-9


def plan_occupancy(plan: Dict[str, Any]) -> float:
    """单个计划当前实际占用的共享额度。"""
    status = plan["status"]
    if status == EXECUTED and plan.get("actual") is not None:
        return float(plan["actual"])
    if status == ALLOCATED:
        return float(plan["planned"])
    return 0.0


def occupied_quota(plans: List[Dict[str, Any]]) -> float:
    return sum(plan_occupancy(plan) for plan in plans)


def reallocate(plans: List[Dict[str, Any]], shared_cap: float) -> List[int]:
    """按提交顺序（seq）重新分配额度，返回可从待排队提升为已分配的计划id。

    - 已执行计划按实际下泄量占用，已分配计划按计划量占用；
    - 冻结计划及其后的待排队计划不放行（冻结只能由总工解除）；
    - 队头计划装不下时发生队头阻塞，后面更小的计划也不越级分配。
    """
    remaining = float(shared_cap) - occupied_quota(plans)
    blocked = False
    promotions: List[int] = []
    for plan in sorted(plans, key=lambda item: item["seq"]):
        status = plan["status"]
        if status == FROZEN:
            blocked = True
        elif status == WAITING:
            if not blocked and float(plan["planned"]) <= remaining + EPS:
                promotions.append(plan["id"])
                remaining -= float(plan["planned"])
            else:
                blocked = True
    return promotions


def is_over_discharge(planned: float, actual: float) -> bool:
    return float(actual) > float(planned) + EPS
