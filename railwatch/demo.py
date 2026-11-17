"""铁路新闻证据系统：Offline, deterministic walkthrough of the evidence publication workflow."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

from .quality import Quality
from .report import Reports
from .service import RailwayService


def main():
    with tempfile.TemporaryDirectory() as temporary:
        db_path = Path(temporary) / "rail.sqlite3"
        # 初始系统只知道旧版行内/claims 规则（v1）。
        service = RailwayService(db_path, current_rule_version=1)
        service.register_source({"source_id": "demo_publisher", "host": "example.org",
                                 "kinds": ["freight", "milestone"], "max_age_days": 30,
                                 "trust_rank": 10, "version": 1, "active": True})
        quote = "The synthetic route carried 342 tonnes through October."
        claim = {"project_key": "route_demo", "kind": "freight", "metric": "cumulative_cargo",
                 "period": "2026-10-31", "amount": "342", "unit": "tonne", "quote": quote}
        capture = {"source_id": "demo_publisher", "external_id": "story-1",
                   "url": "https://example.org/news/route-1",
                   "fetched_at": "2026-11-16T08:00:00Z",
                   "published_at": "2026-11-15T08:00:00Z",
                   "title": "Synthetic railway operations example",
                   "body": json.dumps({"title": "Synthetic railway operations example",
                                       "text": quote, "claims": [claim]}),
                   "media_type": "application/json", "metadata": {"origin": "local-synthetic"}}
        first = service.replay(capture)
        repeat = service.replay(capture)
        if not first["created"] or repeat["created"] or first["claim_ids"] != repeat["claim_ids"]:
            raise AssertionError("capture replay must be idempotent")
        review = service.review(first["claim_ids"][0], {"decision": "accepted",
                                                       "reviewer": "demo_reviewer",
                                                       "reason": "Quote matches the synthetic publisher text",
                                                       "expected_version": 0})
        if review["scope"]["selected_claim"] != first["claim_ids"][0]:
            raise AssertionError("accepted evidence was not selected")
        # 另一条 v1 时代已通过的指标，用于演示新规则上线后的 rule_changed 诊断。
        other_quote = "The junction handled 120 trains in October."
        other_claim = {"project_key": "route_other", "kind": "freight", "metric": "train_count",
                       "period": "2026-10-31", "amount": "120", "unit": "train", "quote": other_quote}
        other_capture = dict(capture, external_id="story-other",
                             url="https://example.org/news/junction",
                             body=json.dumps({"text": other_quote, "claims": [other_claim]}))
        other = service.replay(other_capture)
        service.review(other["claim_ids"][0], {"decision": "accepted", "reviewer": "demo_reviewer",
                                               "reason": "Junction count matches quote",
                                               "expected_version": 0})
        released = service.publish({"name": "route_demo_release", "project_key": "route_demo"})
        report = Reports(service).snapshot(released["snapshot_id"], "route_demo")
        quality = Quality(service).publication_readiness("route_demo")
        if report["selected_count"] != 1 or report["series"][0]["latest"] != "342":
            raise AssertionError("snapshot report lost selected evidence")
        if quality["ready"] is not True:
            raise AssertionError("reviewed route must be publishable")

        # 发布方升级规则：修正稿同时携带旧 claims 与新版 railway_indicators 字段。
        revised = dict(claim, amount="350")
        migrated_body = json.dumps({"title": "Synthetic railway operations example",
                                    "text": quote,
                                    "claims": [claim],
                                    "railway_indicators": [revised]})
        migrated = dict(capture, body=migrated_body)
        # 系统切换到新规则版本（复用同一本地库，全程离线，不重新请求站点）。
        service = RailwayService(db_path, current_rule_version=2)
        revised_replay = service.replay(migrated)
        target_id = revised_replay["capture_id"]
        if service.capture(target_id)["claims"][0]["amount"] != "350":
            raise AssertionError("rule v2 must prefer the new railway_indicators field")
        if service.claim(first["claim_ids"][0])["state"] != "retracted":
            raise AssertionError("the superseded capture's accepted claim must be retired")
        # 同版本重复处理幂等：不新增解析尝试、不改变任何状态。
        again = service.reprocess(target_id, rule_version=2)
        if not again["idempotent"] or again["attempt_id"] != revised_replay["attempt_id"]:
            raise AssertionError("same rule version reprocessing must be idempotent")
        # v1 规则产出的已通过证据在新规则下被质量诊断标记，必须重新审视。
        # story-other 从未被新版本取代，其 v1 主张仍处于 accepted 且仍被范围选中。
        stale = Quality(service).stale_reviews("route_other")
        if not any("rule_changed" in row["reasons"] and row["claim_id"] == other["claim_ids"][0]
                   for row in stale):
            raise AssertionError("v1-era accepted evidence must be flagged under rule v2")
        # 在另一篇独立修正稿上用旧规则离线回退：读到旧值，新主张被标记 superseded，结果可追踪。
        second_body = json.dumps({"text": other_quote, "claims": [other_claim],
                                  "railway_indicators": [dict(other_claim, amount="125")]})
        second = service.replay(dict(other_capture, external_id="story-other-v2",
                                     url="https://example.org/news/junction-v2", body=second_body))
        downgraded = service.reprocess(second["capture_id"], rule_version=1)
        if downgraded["idempotent"] or not downgraded["changed"]:
            raise AssertionError("a different rule version must reopen the capture")
        if service.capture(second["capture_id"])["claims"][-1]["amount"] != "120":
            raise AssertionError("rule v1 must read the legacy claims field")
        once_more = service.reprocess(second["capture_id"], rule_version=1)
        if not once_more["idempotent"] or once_more["attempt_id"] != downgraded["attempt_id"]:
            raise AssertionError("repeat of the same rule version must be idempotent")
        attempts = service.parse_attempts(second["capture_id"])
        if [a["rule_version"] for a in attempts] != [2, 1]:
            raise AssertionError("each rule version keeps exactly one traceable attempt")
        # 修正稿使原范围回到未解决，发布被阻止；冻结快照仍按当时结果重现。
        readiness = Quality(service).publication_readiness("route_demo")
        if readiness["ready"]:
            raise AssertionError("changed indicators must force re-review before publication")
        frozen = service.snapshot(released["snapshot_id"])
        if frozen["content"][0]["selected"]["amount"] != "342":
            raise AssertionError("frozen snapshots must still reproduce the historical result")
        # 审核员复核新规则主张后，发布资格恢复。
        new_claim_id = revised_replay["claim_ids"][0]
        service.review(new_claim_id, {"decision": "accepted", "reviewer": "demo_reviewer",
                                      "reason": "Revised indicator verified against publisher text",
                                      "expected_version": 2})
        if not Quality(service).publication_readiness("route_demo")["ready"]:
            raise AssertionError("re-reviewed evidence must be publishable again")
        if len(service.claim_reviews(first["claim_ids"][0])) != 1:
            raise AssertionError("manual review history must remain traceable")
        print(json.dumps({"capture_id": first["capture_id"], "idempotent_replay": True,
                          "claim_id": first["claim_ids"][0], "scope_version": review["scope"]["version"],
                          "snapshot_id": released["snapshot_id"], "digest": released["digest"],
                          "latest_tonnes": report["series"][0]["latest"],
                          "provenance_count": len(report["provenance"]), "ready": quality["ready"],
                          "parse_attempts": len(attempts),
                          "revised_tonnes": service.claim(new_claim_id)["amount"]},
                         ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
