import tempfile
import unittest
from pathlib import Path

from src.domain import ConflictError, PermissionDenied, ValidationError
from src.queueing import ALLOCATED, EXECUTED, FROZEN, REJECTED, WAITING, reallocate
from src.repository import Repository
from src.service import Service


class QueueTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Repository(str(Path(self.tmp.name) / "test.db"))
        self.service = Service(self.repo)
        self.actor = "dispatcher-zhang"

    def tearDown(self):
        self.repo.close()
        self.tmp.cleanup()

    def section(self, cap=100, name=None):
        name = name or f"sec-{id(self)}-{cap}-{self.section_seq()}"
        return self.service.register_section(
            {"name": name, "shared_cap": cap}, self.actor, "dispatcher")

    _seq = 0

    @classmethod
    def section_seq(cls):
        cls._seq += 1
        return cls._seq

    def plan(self, section_id, reservoir, planned):
        return self.service.register_plan(
            section_id, {"reservoir": reservoir, "planned": planned},
            self.actor, "dispatcher")

    def statuses(self, section_id):
        return [p["status"] for p in self.service.section_view(section_id, "viewer")["plans"]]

    def test_fifo_allocation_head_of_line_blocking(self):
        sec = self.section(100)
        self.plan(sec["id"], "A", 60)
        self.plan(sec["id"], "B", 50)   # 只剩40，装不下，待排队
        self.plan(sec["id"], "C", 30)   # 队头B阻塞，C更小也不能越级
        view = self.service.section_view(sec["id"], "viewer")
        self.assertEqual(self.statuses(sec["id"]),
                         [ALLOCATED, WAITING, WAITING])
        self.assertAlmostEqual(view["occupied"], 60)
        self.assertAlmostEqual(view["remaining"], 40)
        waiting = [p for p in view["plans"] if p["status"] == WAITING]
        self.assertEqual([p["reservoir"] for p in waiting], ["B", "C"])

    def test_under_release_frees_quota_and_promotes(self):
        sec = self.section(100)
        self.plan(sec["id"], "A", 60)
        self.plan(sec["id"], "B", 50)
        self.plan(sec["id"], "C", 30)
        plans = self.service.section_view(sec["id"], "viewer")["plans"]
        a = plans[0]
        view = self.service.execute_plan(
            a["id"], {"actual": 55}, self.actor, "dispatcher")
        # A偏小到55，释放5，剩余45：B(50)仍装不下，B阻塞，C也不能越级
        self.assertEqual([p["status"] for p in view["plans"]],
                         [EXECUTED, WAITING, WAITING])
        self.assertAlmostEqual(view["occupied"], 55)
        self.assertAlmostEqual(view["remaining"], 45)

        sec2 = self.section(100)
        self.plan(sec2["id"], "D", 60)
        self.plan(sec2["id"], "E", 40)
        plans2 = self.service.section_view(sec2["id"], "viewer")["plans"]
        view2 = self.service.execute_plan(
            plans2[0]["id"], {"actual": 60}, self.actor, "dispatcher")
        # 持平不释放，但容量恰好够E
        self.assertEqual([p["status"] for p in view2["plans"]],
                         [EXECUTED, ALLOCATED])

    def test_exact_release_promotes_later_plans(self):
        sec = self.section(100)
        self.plan(sec["id"], "A", 60)
        self.plan(sec["id"], "B", 50)
        self.plan(sec["id"], "C", 40)
        a = self.service.section_view(sec["id"], "viewer")["plans"][0]
        view = self.service.execute_plan(
            a["id"], {"actual": 10}, self.actor, "dispatcher")
        # 释放50：B拿到50，剩余40给C
        self.assertEqual([p["status"] for p in view["plans"]],
                         [EXECUTED, ALLOCATED, ALLOCATED])
        self.assertAlmostEqual(view["occupied"], 100)

    def test_over_discharge_freezes_later_plans_for_chief_review(self):
        sec = self.section(100)
        self.plan(sec["id"], "A", 60)
        self.plan(sec["id"], "B", 40)
        self.plan(sec["id"], "C", 10)
        a = self.service.section_view(sec["id"], "viewer")["plans"][0]
        view = self.service.execute_plan(
            a["id"], {"actual": 90}, self.actor, "dispatcher")
        # A超泄到90，B(已分配)与C(待排队)全部冻结
        self.assertEqual([p["status"] for p in view["plans"]],
                         [EXECUTED, FROZEN, FROZEN])
        self.assertTrue(view["frozen"])
        self.assertAlmostEqual(view["occupied"], 90)
        # 冻结期间新登记的计划只能等待
        self.plan(sec["id"], "D", 5)
        self.assertEqual(self.statuses(sec["id"])[-1], WAITING)

        plans = self.service.section_view(sec["id"], "viewer")["plans"]
        b, c, d = plans[1], plans[2], plans[3]
        # 调度员无权复核
        with self.assertRaises(PermissionDenied):
            self.service.review_plan(
                b["id"], {"decision": "approve"}, self.actor, "dispatcher")
        # 总工驳回B：仍有C冻结，断面不解冻，D继续等待
        view = self.service.review_plan(
            b["id"], {"decision": "reject"}, "chief-li", "chief_engineer")
        self.assertEqual([p["status"] for p in view["plans"]],
                         [EXECUTED, REJECTED, FROZEN, WAITING])
        self.assertTrue(view["frozen"])
        # 总工放行C（超占也允许，总工决定）：冻结解除，D按余量仍装不下
        view = self.service.review_plan(
            c["id"], {"decision": "approve"}, "chief-li", "chief_engineer")
        self.assertFalse(view["frozen"])
        self.assertEqual([p["status"] for p in view["plans"]],
                         [EXECUTED, REJECTED, ALLOCATED, WAITING])
        # 实际90 + C计划10 = 100，D(5)无余量继续等待
        self.assertAlmostEqual(view["occupied"], 100)

    def test_executed_plan_is_immutable(self):
        sec = self.section(100)
        self.plan(sec["id"], "A", 60)
        a = self.service.section_view(sec["id"], "viewer")["plans"][0]
        self.service.execute_plan(
            a["id"], {"actual": 55}, self.actor, "dispatcher")
        with self.assertRaises(ConflictError):
            self.service.execute_plan(
                a["id"], {"actual": 999}, self.actor, "dispatcher")
        stored = self.repo.get_plan(a["id"])
        self.assertAlmostEqual(stored["actual"], 55)
        self.assertEqual(stored["status"], EXECUTED)

    def test_roles_and_validation(self):
        with self.assertRaises(PermissionDenied):
            self.service.register_section(
                {"name": "x", "shared_cap": 100}, "v", "viewer")
        sec = self.service.register_section(
            {"name": "guard", "shared_cap": 100}, self.actor, "dispatcher")
        with self.assertRaises(ValidationError):
            self.service.register_plan(
                sec["id"], {"reservoir": "A", "planned": 0},
                self.actor, "dispatcher")
        with self.assertRaises(ValidationError):
            self.service.register_section(
                {"name": "bad", "shared_cap": 0}, self.actor, "dispatcher")
        with self.assertRaises(ConflictError):
            self.service.register_section(
                {"name": "guard", "shared_cap": 50}, self.actor, "dispatcher")

    def test_waiting_and_frozen_plans_cannot_execute(self):
        sec = self.section(100)
        self.plan(sec["id"], "A", 60)
        self.plan(sec["id"], "B", 50)
        plans = self.service.section_view(sec["id"], "viewer")["plans"]
        with self.assertRaises(ConflictError):
            self.service.execute_plan(
                plans[1]["id"], {"actual": 10}, self.actor, "dispatcher")

    def test_reallocate_pure_helper(self):
        plans = [
            {"id": 1, "seq": 1, "status": EXECUTED, "planned": 60, "actual": 20},
            {"id": 2, "seq": 2, "status": WAITING, "planned": 50, "actual": None},
            {"id": 3, "seq": 3, "status": WAITING, "planned": 30, "actual": None},
        ]
        self.assertEqual(reallocate(plans, 100), [2, 3])
        plans[1]["planned"] = 90
        self.assertEqual(reallocate(plans, 100), [])


if __name__ == "__main__":
    unittest.main()
