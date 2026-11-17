"""可标识解析规则版本的离线重处理：端到端验证。

覆盖四类场景：
1. 旧格式（行内 [RAILCLAIM] 标记 / 旧版 claims 数组）的解析与重处理；
2. 新格式（railway_indicators JSON 字段）的解析与版本差异；
3. 解析失败后修复原文再处理，失败原因全程可追踪；
4. 处理过程中新增抓取不会被误捞或重复处理。
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from railwatch.api import create_app
from railwatch.domain import ProductError
from railwatch.extract import RULE_VERSION_V1, RULE_VERSION_V2, rule_digest
from railwatch.quality import Quality
from railwatch.report import Reports
from railwatch.service import RailwayService
from railwatch.worker import Worker


class ReprocessFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.service = RailwayService(self.root / "evidence.sqlite3",
                                      current_rule_version=RULE_VERSION_V1)
        self.service.register_source({
            "source_id": "news", "host": "example.org",
            "kinds": ["freight", "milestone", "capacity"],
            "max_age_days": 30, "trust_rank": 10, "version": 1, "active": True})

    def tearDown(self):
        self.temp.cleanup()

    def policy(self):
        return self.service.policy("news")

    def claim(self, quantity="100", metric="cargo", project="rail-a", period="2026-11-15",
              quote=None):
        return {"project_key": project, "kind": "freight", "metric": metric, "period": period,
                "amount": quantity, "unit": "tonne",
                "quote": quote or ("Freight reached " + quantity + " tonnes on the route.")}

    def json_capture(self, external_id="article-1", body_claims=None, text=None, extra=None):
        claim = body_claims if body_claims is not None else [self.claim()]
        text = text or (claim[0]["quote"] if claim else "No claims here.")
        payload = {"title": "Railway article", "text": text, "claims": claim}
        if extra:
            payload.update(extra)
        return {"source_id": "news", "external_id": external_id,
                "url": "https://example.org/article/1", "fetched_at": "2026-11-16T00:00:00Z",
                "published_at": "2026-11-15T12:00:00Z", "title": "Railway article",
                "body": json.dumps(payload), "media_type": "application/json",
                "metadata": {"publisher": "synthetic"}}

    def marker_capture(self, external_id="marked-1", quantity="88"):
        quote = "Terminal moved " + quantity + " wagons this week."
        marker = json.dumps({"project_key": "rail-a", "kind": "freight", "metric": "wagons",
                             "period": "2026-11-15", "amount": quantity, "unit": "wagon",
                             "quote": quote})
        body = "Operations note.\n[RAILCLAIM " + marker + "]\n"
        return {"source_id": "news", "external_id": external_id,
                "url": "https://example.org/article/m", "fetched_at": "2026-11-16T00:00:00Z",
                "published_at": "2026-11-15T12:00:00Z", "title": "Marked article",
                "body": body, "media_type": "text/plain"}

    def review(self, claim_id, decision="accepted", expected=0, reviewer="analyst"):
        return self.service.review(claim_id, {"decision": decision, "reviewer": reviewer,
                                              "reason": "Verified quoted source evidence",
                                              "expected_version": expected})


class LegacyFormatTests(ReprocessFixture):
    def test_inline_markers_parse_identically_across_rule_versions(self):
        result = self.service.replay(self.marker_capture())
        self.assertEqual(result["state"], "parsed")
        self.assertEqual(result["rule_version"], RULE_VERSION_V1)
        # 行内标记规则在两个版本中保持一致，重处理结果相同、不制造重复主张。
        again = self.service.reprocess(result["capture_id"], RULE_VERSION_V1)
        self.assertTrue(again["idempotent"])
        v2 = self.service.reprocess(result["capture_id"], RULE_VERSION_V2)
        self.assertFalse(v2["idempotent"])
        self.assertFalse(v2["changed"])
        claims = self.service.capture(result["capture_id"])["claims"]
        self.assertEqual([c["amount"] for c in claims], ["88"])
        self.assertEqual({c["rule_version"] for c in claims}, {RULE_VERSION_V1})

    def test_legacy_claims_array_under_both_rules_without_new_field(self):
        result = self.service.replay(self.json_capture())
        v2 = self.service.reprocess(result["capture_id"], RULE_VERSION_V2)
        # 没有新字段时 v2 回退到 claims 数组，结果一致，不改变既有审核状态。
        self.assertFalse(v2["changed"])
        self.assertEqual(v2["state"], "parsed")
        attempts = self.service.parse_attempts(result["capture_id"])
        self.assertEqual([(a["rule_version"], a["status"]) for a in attempts],
                         [(RULE_VERSION_V1, "parsed"), (RULE_VERSION_V2, "parsed")])
        self.assertTrue(all(a["rule_digest"] == rule_digest(a["rule_version"]) for a in attempts))

    def test_same_version_reprocessing_is_idempotent(self):
        result = self.service.replay(self.json_capture())
        first = self.service.reprocess(result["capture_id"], RULE_VERSION_V1)
        second = self.service.reprocess(result["capture_id"], RULE_VERSION_V1)
        self.assertTrue(first["idempotent"] and second["idempotent"])
        self.assertEqual(first["attempt_id"], second["attempt_id"])
        self.assertEqual(len(self.service.parse_attempts(result["capture_id"])), 1)
        self.assertEqual(len(self.service.capture(result["capture_id"])["claims"]), 1)

    def test_reprocessing_never_touches_network(self):
        # 重处理路径只读取数据库中保存的原文；服务里不存在任何采集器调用。
        result = self.service.replay(self.json_capture())
        stored = self.service.store.capture(result["capture_id"])
        self.assertIn("Freight reached 100", stored["body"])
        service_without_worker = RailwayService(self.root / "evidence.sqlite3",
                                                current_rule_version=RULE_VERSION_V2)
        service_without_worker.schedule = None
        outcome = service_without_worker.reprocess(result["capture_id"], RULE_VERSION_V2)
        self.assertEqual(outcome["state"], "parsed")

    def test_reprocessing_rejects_unknown_rule_version(self):
        result = self.service.replay(self.json_capture())
        with self.assertRaises(ProductError) as caught:
            self.service.reprocess(result["capture_id"], 99)
        self.assertEqual(caught.exception.code, "invalid_rule_version")


class NewFormatTests(ReprocessFixture):
    def _migrated_capture(self, old="100", new="140"):
        # 两个字段引用同一句原文引句，仅数值主张不同（发布方修正的是指标值而非引文）。
        shared_quote = "Freight reached a reported volume on the route, audited in tonnes."
        old_claim = self.claim(old, quote=shared_quote)
        new_claim = self.claim(new, quote=shared_quote)
        return self.json_capture(
            body_claims=[old_claim], text=shared_quote,
            extra={"railway_indicators": [new_claim]})

    def test_v1_ignores_new_field_v2_prefers_it(self):
        item = self._migrated_capture()
        v1_result = self.service.replay(item)
        self.assertEqual(self.service.capture(v1_result["capture_id"])["claims"][0]["amount"], "100")
        v2 = self.service.reprocess(v1_result["capture_id"], RULE_VERSION_V2)
        self.assertEqual(v2["state"], "parsed")
        self.assertTrue(v2["changed"])
        active = [c for c in self.service.capture(v1_result["capture_id"])["claims"]
                  if c["state"] != "superseded"]
        self.assertEqual([c["amount"] for c in active], ["140"])
        self.assertEqual(active[0]["rule_version"], RULE_VERSION_V2)

    def test_changed_indicator_reopens_scope_and_blocks_publication(self):
        item = self._migrated_capture()
        v1_result = self.service.replay(item)
        self.review(v1_result["claim_ids"][0])
        release = self.service.publish({"name": "release-1"})
        v2 = self.service.reprocess(v1_result["capture_id"], RULE_VERSION_V2)
        self.assertTrue(v2["changed"])
        # 旧主张被淘汰，范围回到未解决，发布被阻止。
        self.assertEqual(self.service.claim(v1_result["claim_ids"][0])["state"], "superseded")
        scope = self.service.scopes()[0]
        self.assertTrue(scope["conflict"])
        self.assertIsNone(scope["selected_claim"])
        with self.assertRaises(ProductError) as caught:
            self.service.publish({"name": "release-2"})
        self.assertEqual(caught.exception.code, "unresolved_scope")
        # 冻结快照仍按当时结果重现。
        frozen = self.service.snapshot(release["snapshot_id"])
        self.assertEqual(frozen["content"][0]["selected"]["amount"], "100")
        report = Reports(self.service).snapshot(release["snapshot_id"], "rail-a")
        self.assertEqual(report["series"][0]["latest"], "100")
        # 新主张重新审核后发布资格恢复。
        self.review(v2["claim_ids"][0], expected=scope["version"])
        self.assertTrue(Quality(self.service).publication_readiness("rail-a")["ready"])

    def test_review_history_and_both_rule_results_remain_traceable(self):
        item = self._migrated_capture()
        v1_result = self.service.replay(item)
        self.review(v1_result["claim_ids"][0])
        self.service.reprocess(v1_result["capture_id"], RULE_VERSION_V2)
        attempts = self.service.parse_attempts(v1_result["capture_id"])
        self.assertEqual([a["rule_version"] for a in attempts], [1, 2])
        self.assertEqual([a["status"] for a in attempts], ["parsed", "parsed"])
        self.assertEqual([a["result_hash"] is not None for a in attempts], [True, True])
        history = self.service.claim_reviews(v1_result["claim_ids"][0])
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["decision"], "accepted")
        quality = Quality(self.service).claim_quality("rail-a")
        self.assertEqual(quality["superseded_claim_ids"], v1_result["claim_ids"])

    def test_partial_claim_change_reuses_unchanged_rows(self):
        # v1 产出 [A,B]，v2 产出 [A,C]：A 沿用原行（保留审核），仅 B 被淘汰、C 新插入，
        # 不触发 (capture, claim_hash) 唯一约束，范围只在真正变化处重开。
        quote_a = "Shared audited quote alpha here."
        quote_b = "Shared audited quote beta here."
        claim_a = {"project_key": "rail-a", "kind": "freight", "metric": "ma",
                   "period": "2026-11-15", "amount": "100", "unit": "tonne", "quote": quote_a}
        claim_b = {"project_key": "rail-a", "kind": "freight", "metric": "mb",
                   "period": "2026-11-15", "amount": "200", "unit": "tonne", "quote": quote_b}
        claim_c = {"project_key": "rail-a", "kind": "freight", "metric": "mc",
                   "period": "2026-11-15", "amount": "300", "unit": "tonne", "quote": quote_b}
        item = self.json_capture(body_claims=[claim_a, claim_b],
                                 text=quote_a + " " + quote_b)
        result = self.service.replay(item)
        self.review(result["claim_ids"][0])  # 通过 A 的审核
        # 直接在库内把正文替换为带新字段的版本（模拟离线重处理所读原文已更新）。
        import sqlite3
        con = sqlite3.connect(self.root / "evidence.sqlite3")
        con.execute("UPDATE captures SET body=? WHERE id=?",
                    (json.dumps({"text": quote_a + " " + quote_b, "claims": [claim_a, claim_b],
                                 "railway_indicators": [claim_a, claim_c]}),
                     result["capture_id"]))
        con.commit()
        con.close()
        v2 = self.service.reprocess(result["capture_id"], RULE_VERSION_V2)
        self.assertTrue(v2["changed"])
        claims = self.service.capture(result["capture_id"])["claims"]
        active = [c for c in claims if c["state"] != "superseded"]
        self.assertEqual(sorted(c["metric"] for c in active), ["ma", "mc"])
        # A 的原行与 accepted 审核被保留。
        claim_a_row = next(c for c in active if c["metric"] == "ma")
        self.assertEqual(claim_a_row["id"], result["claim_ids"][0])
        self.assertEqual(claim_a_row["state"], "accepted")
        # B 被淘汰，C 以 proposed 进入队列。
        self.assertEqual(next(c for c in claims if c["metric"] == "mb")["state"], "superseded")
        self.assertEqual(next(c for c in active if c["metric"] == "mc")["state"], "proposed")

    def test_identical_results_keep_review_decisions(self):
        # 新字段与旧字段内容一致时，重处理不应打扰已通过的人工决议。
        shared_quote = "Freight reached a reported volume on the route, audited in tonnes."
        claim = self.claim("100", quote=shared_quote)
        item = self.json_capture(body_claims=[claim], text=shared_quote,
                                 extra={"railway_indicators": [dict(claim)]})
        result = self.service.replay(item)
        self.review(result["claim_ids"][0])
        v2 = self.service.reprocess(result["capture_id"], RULE_VERSION_V2)
        self.assertFalse(v2["changed"])
        self.assertEqual(self.service.claim(result["claim_ids"][0])["state"], "accepted")
        self.assertEqual(self.service.scopes()[0]["conflict"], 0)

    def test_stale_review_flagged_under_new_rule(self):
        result = self.service.replay(self.json_capture())
        self.review(result["claim_ids"][0])
        upgraded = RailwayService(self.root / "evidence.sqlite3",
                                  current_rule_version=RULE_VERSION_V2)
        stale = Quality(upgraded).stale_reviews("rail-a")
        self.assertEqual(stale[0]["reasons"], ["rule_changed"])
        self.assertFalse(Quality(upgraded).publication_readiness("rail-a")["ready"])


class FailureRepairTests(ReprocessFixture):
    def test_failure_reasons_persisted_per_rule_version(self):
        # 缺少 claims 数组：解析期直接报错，文章不会入库。
        broken = self.json_capture()
        broken["body"] = json.dumps({"title": "Railway article", "text": "Evidence text."})
        with self.assertRaises(ProductError) as caught:
            self.service.replay(broken)
        self.assertEqual(caught.exception.code, "missing_claims")
        # 可入库但提取不到主张的原文被隔离，失败原因随解析尝试落库。
        good = self.service.replay(self.json_capture(external_id="good-1"))
        bad_item = self.json_capture(external_id="bad-1")
        bad_item["body"] = json.dumps({"text": "No claims", "claims": []})
        failed = self.service.replay(bad_item)
        self.assertEqual(failed["state"], "quarantined")
        attempts = self.service.parse_attempts(failed["capture_id"])
        self.assertEqual(attempts[0]["status"], "quarantined")
        self.assertEqual(attempts[0]["issues"][0]["code"], "no_claims")
        # 同版本重试隔离稿仍然幂等，失败原因不变。
        again = self.service.reprocess(failed["capture_id"], RULE_VERSION_V1)
        self.assertTrue(again["idempotent"])
        self.assertEqual(again["state"], "quarantined")
        self.assertEqual(again["issues"][0]["code"], "no_claims")
        self.assertEqual(good["state"], "parsed")

    def test_repair_then_reprocess_succeeds_and_is_auditable(self):
        bad_item = self.json_capture(external_id="repair-1")
        bad_item["body"] = json.dumps({"text": "No claims", "claims": []})
        failed = self.service.replay(bad_item)
        self.assertEqual(failed["state"], "quarantined")
        # 发布方修复：同一外部文章的新版本正文入库（模拟修复后的重新抓取，离线回放）。
        fixed = self.json_capture(external_id="repair-1")
        repaired = self.service.replay(fixed)
        self.assertEqual(repaired["state"], "parsed")
        self.assertTrue(repaired["created"])
        # 旧隔离稿保留失败尝试；新稿有成功尝试，两者都可追踪。
        old_attempts = self.service.parse_attempts(failed["capture_id"])
        new_attempts = self.service.parse_attempts(repaired["capture_id"])
        self.assertEqual(old_attempts[0]["status"], "quarantined")
        self.assertEqual(new_attempts[0]["status"], "parsed")
        self.assertEqual(self.service.store.capture(failed["capture_id"])["state"], "superseded")
        events = [e["event"] for e in self.service.audit()]
        self.assertIn("parse_attempt_recorded", events)
        self.assertIn("capture_superseded", events)

    def test_invalid_new_field_quarantines_with_traceable_issue(self):
        item = self.json_capture(
            body_claims=[], text="Publisher text long enough to audit.",
            extra={"railway_indicators": [{"project_key": "rail-a", "kind": "freight"}]})
        service_v2 = RailwayService(self.root / "evidence.sqlite3",
                                    current_rule_version=RULE_VERSION_V2)
        result = service_v2.replay(item)
        self.assertEqual(result["state"], "quarantined")
        self.assertTrue(result["issues"])
        attempt = service_v2.parse_attempts(result["capture_id"])[0]
        self.assertEqual(attempt["rule_version"], RULE_VERSION_V2)
        self.assertEqual(attempt["issues"], result["issues"])

    def test_new_rule_repairs_legacy_quarantine_offline(self):
        # 旧抓取的空 claims 在 v1 下隔离；同稿携带的新字段在 v2 下恢复主张，无需重新抓取。
        shared_quote = "Freight reached a reported volume, audited in tonnes."
        indicator = self.claim("100", quote=shared_quote)
        item = self.json_capture(
            body_claims=[], text=shared_quote,
            extra={"railway_indicators": [indicator]})
        failed = self.service.replay(item)
        self.assertEqual(failed["state"], "quarantined")
        service_v2 = RailwayService(self.root / "evidence.sqlite3",
                                    current_rule_version=RULE_VERSION_V2)
        repaired = service_v2.reprocess(failed["capture_id"], RULE_VERSION_V2)
        self.assertEqual(repaired["state"], "parsed")
        self.assertTrue(repaired["claim_ids"])
        self.assertEqual(service_v2.store.capture(failed["capture_id"])["state"], "parsed")
        attempts = service_v2.parse_attempts(failed["capture_id"])
        self.assertEqual([(a["rule_version"], a["status"]) for a in attempts],
                         [(RULE_VERSION_V1, "quarantined"), (RULE_VERSION_V2, "parsed")])
        # 失败原因与成功结果在同一原文上同时可追踪。
        self.assertTrue(attempts[0]["issues"])
        self.assertEqual(attempts[1]["issues"], [])


class PendingBackfillTests(ReprocessFixture):
    def test_batch_backfill_skips_already_processed_versions(self):
        first = self.service.replay(self.json_capture("a"))
        second = self.service.replay(self.json_capture("b"))
        # 已有 v1 结果的两篇原文都进入 v2 待处理清单。
        pending = self.service.store.captures_pending_rule(RULE_VERSION_V2)
        self.assertEqual([row["id"] for row in pending], [first["capture_id"], second["capture_id"]])
        batch = self.service.reprocess_pending(RULE_VERSION_V2)
        self.assertEqual(batch["selected"], 2)
        self.assertTrue(all(o["state"] == "parsed" for o in batch["outcomes"]))
        # 再跑一次：没有待处理原文，结果为空且完全幂等。
        again = self.service.reprocess_pending(RULE_VERSION_V2)
        self.assertEqual(again["selected"], 0)
        self.assertEqual(again["outcomes"], [])
        # 同版本不会被当作“未完成”反复处理。
        self.assertEqual(len(self.service.store.captures_pending_rule(RULE_VERSION_V2)), 0)

    def test_new_capture_during_processing_is_not_swept_in(self):
        first = self.service.replay(self.json_capture("a"))
        second = self.service.replay(self.json_capture("b"))
        # 清单在批次开始时一次性确定；处理期间新入库的原文不影响本轮。
        pending = self.service.store.captures_pending_rule(RULE_VERSION_V2, limit=10)
        self.assertEqual(len(pending), 2)
        # 新规则已上线：处理过程中新抓取的原文入库时即带 v2 尝试，不属于待补处理集合。
        service_v2 = RailwayService(self.root / "evidence.sqlite3",
                                    current_rule_version=RULE_VERSION_V2)
        third = service_v2.replay(self.json_capture("c"))
        self.assertEqual(third["rule_version"], RULE_VERSION_V2)
        batch = self.service.reprocess_pending(RULE_VERSION_V2)
        self.assertEqual([o["capture_id"] for o in batch["outcomes"]],
                         [first["capture_id"], second["capture_id"]])
        self.assertNotIn(third["capture_id"], [o["capture_id"] for o in batch["outcomes"]])
        # 新原文已带目标版本，下一轮也不会被重复处理。
        next_batch = self.service.reprocess_pending(RULE_VERSION_V2)
        self.assertEqual(next_batch["selected"], 0)

    def test_superseded_captures_excluded_from_backfill(self):
        old = self.service.replay(self.json_capture("article-1", [self.claim("100")]))
        self.service.replay(self.json_capture("article-1", [self.claim("120")]))
        pending = self.service.store.captures_pending_rule(RULE_VERSION_V2)
        self.assertEqual([row["id"] for row in pending], [old["capture_id"] + 1])
        # 已废弃版本显式重处理会被拒绝。
        with self.assertRaises(ProductError) as caught:
            self.service.reprocess(old["capture_id"], RULE_VERSION_V2)
        self.assertEqual(caught.exception.code, "capture_state")


class WorkerIngestionTests(ReprocessFixture):
    class StubAcquisition:
        def __init__(self, body, media_type="application/json"):
            self.body, self.media_type = body, media_type

        def fetch(self, target, policy, etag=None, last_modified=None, max_redirects=2):
            from railwatch.acquire import FetchResult
            return FetchResult(200, target, self.body, self.media_type, "etag-1", None,
                               "2026-11-16T01:00:00+00:00", "2026-11-15T12:00:00+00:00", 0)

    def test_worker_ingests_with_current_rule_and_backfill_follows(self):
        from railwatch.schedule import Schedule
        schedule = Schedule(self.service.store)
        claim = self.claim("100")
        shared_quote = claim["quote"]
        body = json.dumps({"text": shared_quote, "claims": [claim],
                           "railway_indicators": [self.claim("150", quote=shared_quote)]})
        worker_v1 = Worker(self.service, self.StubAcquisition(body))
        schedule.upsert("news", "fetched-1", "https://example.org/article/fetched",
                        "2026-11-16T00:00:00Z", 3600)
        run = worker_v1.run_once("worker-a", at="2026-11-16T00:00:00Z")
        self.assertEqual(run["status"], "parsed")
        self.assertEqual(run["rule_version"], RULE_VERSION_V1)
        self.assertEqual(self.service.capture(run["capture_id"])["claims"][0]["amount"], "100")
        # 新规则上线后离线补处理同一抓取原文，得到新字段值，无需再次抓取。
        upgraded = RailwayService(self.root / "evidence.sqlite3",
                                  current_rule_version=RULE_VERSION_V2)
        v2 = upgraded.reprocess(run["capture_id"], RULE_VERSION_V2)
        self.assertEqual(upgraded.capture(run["capture_id"])["claims"][-1]["amount"], "150")
        self.assertTrue(v2["changed"])
        # 作业的条件请求元数据保留，证明没有发起新请求。
        job = schedule.job(1)
        self.assertEqual(job["etag"], "etag-1")


class ReprocessApiTests(ReprocessFixture):
    def setUp(self):
        super().setUp()
        self.client = TestClient(create_app(self.root / "api.sqlite3"))
        self.client.put("/sources/news", json={
            "source_id": "news", "host": "example.org",
            "kinds": ["freight", "milestone"], "max_age_days": 30,
            "trust_rank": 10, "version": 1, "active": True})

    def tearDown(self):
        self.client.close()
        super().tearDown()

    def test_reprocess_endpoint_and_attempt_history(self):
        capture_id = self.client.post("/captures", json=self.json_capture()).json()["capture_id"]
        response = self.client.post(f"/captures/{capture_id}/reprocess", json={"rule_version": 2})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["rule_version"], 2)
        history = self.client.get(f"/captures/{capture_id}/parse-attempts").json()
        self.assertEqual([a["rule_version"] for a in history], [1, 2])
        repeat = self.client.post(f"/captures/{capture_id}/reprocess", json={"rule_version": 2})
        self.assertTrue(repeat.json()["idempotent"])

    def test_reprocess_pending_endpoint(self):
        self.client.post("/captures", json=self.json_capture("a"))
        response = self.client.post("/captures/reprocess-pending", json={"rule_version": 2})
        self.assertEqual(response.json()["selected"], 1)
        self.assertEqual(response.json()["rule_version"], 2)

    def test_review_history_endpoint(self):
        claim_id = self.client.post("/captures", json=self.json_capture()).json()["claim_ids"][0]
        self.client.post(f"/claims/{claim_id}/reviews",
                         json={"decision": "accepted", "reviewer": "analyst",
                               "reason": "Checked", "expected_version": 0})
        history = self.client.get(f"/claims/{claim_id}/reviews").json()
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["reviewer"], "analyst")

    def test_reprocess_rejects_bad_rule_version(self):
        capture_id = self.client.post("/captures", json=self.json_capture()).json()["capture_id"]
        response = self.client.post(f"/captures/{capture_id}/reprocess", json={"rule_version": 7})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"], "invalid_rule_version")


class CliReprocessTests(ReprocessFixture):
    def test_cli_reprocess_and_attempts(self):
        result = self.service.replay(self.json_capture())
        db = self.root / "cli.sqlite3"
        # 复用同一库文件：通过 CLI 完成离线重处理与历史查询。
        service = RailwayService(db, current_rule_version=RULE_VERSION_V1)
        service.register_source({
            "source_id": "news", "host": "example.org", "kinds": ["freight"],
            "max_age_days": 30, "trust_rank": 10, "version": 1, "active": True})
        capture_id = service.replay(self.json_capture())["capture_id"]
        run = lambda *args: subprocess.run(
            [sys.executable, "-m", "railwatch", "--db", str(db), *args],
            cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True, check=True)
        out = run("reprocess", str(capture_id), "--rule-version", "2")
        self.assertEqual(json.loads(out.stdout)["rule_version"], 2)
        again = run("reprocess", str(capture_id), "--rule-version", "2")
        self.assertTrue(json.loads(again.stdout)["idempotent"])
        attempts = run("attempts", str(capture_id))
        self.assertEqual([a["rule_version"] for a in json.loads(attempts.stdout)], [1, 2])
        pending = run("reprocess-pending", "--rule-version", "2")
        self.assertEqual(json.loads(pending.stdout)["selected"], 0)


if __name__ == "__main__":
    unittest.main()
