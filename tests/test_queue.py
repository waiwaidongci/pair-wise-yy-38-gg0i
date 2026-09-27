import tempfile, unittest
from pathlib import Path
from src.domain import ConflictError, PermissionDenied, ValidationError
from src.repository import Repository
from src.service import Service


class QueueTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Repository(str(Path(self.tmp.name) / "test.db"))
        self.service = Service(self.repo)
        self.section = self.service.create_section(
            {"name": "沂河控制断面", "shared_limit": 1000, "external_ref": "SEC-1"},
            "调度员甲", "dispatcher")

    def tearDown(self):
        self.repo.close()
        self.tmp.cleanup()

    def _plan(self, reservoir, planned, ref):
        return self.service.register_plan(
            self.section["id"],
            {"reservoir": reservoir, "planned": planned, "external_ref": ref},
            "调度员甲", "dispatcher")

    def _statuses(self):
        queue = self.service.get_section_queue(self.section["id"], "viewer")
        return queue, {p["id"]: p["status"] for p in queue["plans"]}

    def test_fifo_allocation_and_queue_view(self):
        p1 = self._plan("水库A", 400, "P1")
        p2 = self._plan("水库B", 400, "P2")
        p3 = self._plan("水库C", 300, "P3")
        self.assertEqual(p1["status"], "allocated")
        self.assertEqual(p2["status"], "allocated")
        self.assertEqual(p3["status"], "queued")
        queue, _ = self._statuses()
        self.assertEqual(queue["occupied"], 800)
        self.assertEqual(queue["remaining"], 200)
        queued = [p for p in queue["plans"] if p["status"] == "queued"]
        self.assertEqual(len(queued), 1)
        self.assertEqual(queued[0]["ahead"], 800)
        self.assertEqual(queued[0]["shortfall"], 100)
        p4 = self._plan("水库D", 100, "P4")
        self.assertEqual(p4["status"], "queued")

    def test_execute_under_releases_quota_and_promotes(self):
        p1 = self._plan("水库A", 400, "P1")
        self._plan("水库B", 400, "P2")
        p3 = self._plan("水库C", 300, "P3")
        result = self.service.execute_plan(
            p1["id"], {"actual": 250}, "调度员甲", "dispatcher")
        self.assertEqual(result["released"], 150)
        self.assertEqual(result["over"], 0)
        self.assertIn(p3["id"], result["promoted"])
        queue, statuses = self._statuses()
        self.assertEqual(statuses[p3["id"]], "allocated")
        self.assertEqual(queue["occupied"], 950)
        executed = [p for p in queue["plans"] if p["id"] == p1["id"]][0]
        self.assertEqual(executed["actual"], 250)
        self.assertEqual(executed["deviation"], -150)

    def test_execute_over_freezes_and_chief_reviews(self):
        p1 = self._plan("水库A", 400, "P1")
        p2 = self._plan("水库B", 400, "P2")
        p3 = self._plan("水库C", 300, "P3")
        self.service.execute_plan(p1["id"], {"actual": 250}, "调度员甲", "dispatcher")
        result = self.service.execute_plan(
            p2["id"], {"actual": 500}, "调度员甲", "dispatcher")
        self.assertEqual(result["over"], 100)
        self.assertIn(p3["id"], result["frozen"])
        queue, statuses = self._statuses()
        self.assertTrue(queue["review_required"])
        self.assertEqual(statuses[p3["id"]], "frozen")
        with self.assertRaises(ConflictError):
            self.service.execute_plan(p3["id"], {"actual": 300}, "调度员甲", "dispatcher")
        with self.assertRaises(PermissionDenied):
            self.service.review_section(self.section["id"], "调度员甲", "dispatcher")
        review = self.service.review_section(self.section["id"], "总工", "chief_engineer")
        self.assertIn(p3["id"], review["restored"])
        queue, statuses = self._statuses()
        self.assertFalse(queue["review_required"])
        self.assertEqual(queue["occupied"], 750)
        self.assertEqual(statuses[p3["id"]], "queued")
        with self.assertRaises(ConflictError):
            self.service.review_section(self.section["id"], "总工", "chief_engineer")

    def test_executed_plan_cannot_be_rewritten(self):
        p1 = self._plan("水库A", 400, "P1")
        self.service.execute_plan(p1["id"], {"actual": 400}, "调度员甲", "dispatcher")
        with self.assertRaises(ConflictError):
            self.service.execute_plan(p1["id"], {"actual": 100}, "调度员甲", "dispatcher")
        queue, statuses = self._statuses()
        self.assertEqual(statuses[p1["id"]], "executed")
        executed = [p for p in queue["plans"] if p["id"] == p1["id"]][0]
        self.assertEqual(executed["actual"], 400)

    def test_queued_plan_cannot_execute_and_permissions(self):
        self._plan("水库A", 600, "P1")
        p2 = self._plan("水库B", 600, "P2")
        self.assertEqual(p2["status"], "queued")
        with self.assertRaises(ConflictError):
            self.service.execute_plan(p2["id"], {"actual": 100}, "调度员甲", "dispatcher")
        with self.assertRaises(PermissionDenied):
            self.service.register_plan(
                self.section["id"], {"reservoir": "水库C", "planned": 10},
                "viewer", "viewer")
        with self.assertRaises(PermissionDenied):
            self.service.create_section(
                {"name": "x", "shared_limit": 100}, "viewer", "viewer")
        with self.assertRaises(ValidationError):
            self.service.register_plan(
                self.section["id"], {"reservoir": "水库C", "planned": -5},
                "调度员甲", "dispatcher")
        with self.assertRaises(ValidationError):
            self.service.create_section(
                {"name": "y", "shared_limit": 0}, "调度员甲", "dispatcher")

    def test_audit_trail_and_chain(self):
        p1 = self._plan("水库A", 400, "P1")
        self.service.execute_plan(p1["id"], {"actual": 380}, "调度员甲", "dispatcher")
        events = self.service.audit("chief_engineer")
        actions = [e["action"] for e in events]
        self.assertIn("section_create", actions)
        self.assertIn("plan_register", actions)
        self.assertIn("plan_execute", actions)
        self.assertTrue(self.repo.verify_audit_chain())


if __name__ == "__main__":
    unittest.main()
