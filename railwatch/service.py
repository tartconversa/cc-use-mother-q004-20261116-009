"""铁路新闻证据系统：Coordinates replay, extraction, review and reproducible evidence releases."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import json

from .domain import Capture, Claim, ProductError, SourcePolicy, fingerprint, token
from .extract import (DEFAULT_RULE_VERSION, Extractor, require_rule_version, rule_digest)
from .schedule import Schedule
from .store import Store


class RailwayService:
    def __init__(self, db_path, current_rule_version=DEFAULT_RULE_VERSION):
        require_rule_version(current_rule_version)
        self.store = Store(db_path)
        self.schedule = Schedule(self.store)
        self.extractor = Extractor()
        self.current_rule_version = current_rule_version

    def register_source(self, data, expected_version=None):
        policy = SourcePolicy.parse(data)
        return self.store.upsert_source(policy, expected_version)

    def policy(self, source_id):
        row = self.store.source(source_id)
        return SourcePolicy(row["source_id"], row["host"], tuple(row["kinds"]),
                            row["max_age_days"], row["trust_rank"], row["version"], row["active"])

    def replay(self, data):
        capture = Capture.parse(data)
        policy = self.policy(capture.source_id)
        capture.check(policy)
        existing = self.store.capture_identity(capture.source_id, capture.external_id)
        same = next((row for row in existing if row["content_hash"] == capture.content_hash()), None)
        if same:
            latest = self.store.latest_parse_attempt(same["id"])
            return {"capture_id": same["id"], "created": False, "state": same["state"],
                    "claim_ids": [c["id"] for c in self.store.claims_for_capture(same["id"])],
                    "issues": [], "rule_version": latest["rule_version"] if latest else None,
                    "attempt_id": latest["id"] if latest else None,
                    "idempotent": True, "changed": False}
        capture_id, created = self.store.put_capture(capture, policy)
        return self._extract(capture_id, capture, policy, self.current_rule_version, trigger="ingest",
                             created=created)

    def _extract(self, capture_id, capture, policy, rule_version, trigger, created=True):
        """对已入库原文执行一次解析并落库解析尝试；只使用本地原文，不发起任何网络请求。"""
        claims, issues = self.extractor.inspect(capture, policy, rule_version)
        status = "parsed" if claims and not issues else "quarantined"
        result_hash = fingerprint({
            "rule_version": rule_version,
            "claims": [claim.__dict__ for claim in claims],
            "issues": issues,
        })
        record = self.store.record_parse_attempt(
            capture_id, rule_version, rule_digest(rule_version), status, result_hash,
            claims, issues, trigger)
        return {"capture_id": capture_id, "created": created, "state": record["state"],
                "claim_ids": record["claim_ids"], "issues": issues,
                "rule_version": rule_version, "attempt_id": record["attempt_id"],
                "idempotent": record["idempotent"], "changed": record["changed"]}

    @staticmethod
    def _stored_capture(row):
        return Capture(row["source_id"], row["external_id"], row["url"], row["fetched_at"],
                       row["published_at"], row["title"], row["body"], row["media_type"],
                       row["metadata"])

    def reprocess(self, capture_id, rule_version=None, trigger="manual"):
        """用指定规则版本离线重新解析已保存原文；同一版本重复处理幂等。"""
        rule_version = self.current_rule_version if rule_version is None else require_rule_version(rule_version)
        row = self.store.capture(capture_id)
        existing = self.store.get_parse_attempt(capture_id, rule_version)
        if existing is not None:
            # 同版本已处理：直接回放当时结果，既不重新解析站点，也不重复落库。
            return {"capture_id": capture_id, "created": False, "state": existing["status"],
                    "claim_ids": [c["id"] for c in self.store.claims_for_capture(capture_id)
                                  if c["parse_attempt_id"] == existing["id"]],
                    "issues": existing["issues"],
                    "rule_version": rule_version, "attempt_id": existing["id"],
                    "idempotent": True, "changed": False}
        capture = self._stored_capture(row)
        policy = self.policy(capture.source_id)
        return self._extract(capture_id, capture, policy, rule_version, trigger, created=False)

    def reprocess_pending(self, rule_version=None, limit=500):
        """批量离线重处理所有缺少指定版本解析尝试的现存原文。

        处理过程中新入库的原文不会被本轮捞起（清单在开始时一次性确定），
        避免“处理中新增抓取”造成无限循环或重复处理。
        """
        rule_version = self.current_rule_version if rule_version is None else require_rule_version(rule_version)
        pending = self.store.captures_pending_rule(rule_version, limit)
        selected_ids = [row["id"] for row in pending]
        outcomes = []
        for capture_id in selected_ids:
            outcomes.append(self.reprocess(capture_id, rule_version, trigger="batch_reprocess"))
        return {"rule_version": rule_version, "requested_limit": limit,
                "selected": len(selected_ids), "processed": len(outcomes), "outcomes": outcomes}

    def parse_attempts(self, capture_id):
        self.store.capture(capture_id)
        return self.store.parse_attempts(capture_id)

    def claim_reviews(self, claim_id):
        self.store.claim(claim_id)
        return self.store.claim_reviews(claim_id)

    def capture(self, capture_id):
        row = self.store.capture(capture_id)
        row["claims"] = self.store.claims_for_capture(capture_id)
        return row

    def review_queue(self, project_key=None, state="proposed", limit=100):
        if state not in {"proposed", "accepted", "rejected", "hold", "retracted", "superseded"}:
            raise ProductError("invalid_state", "unsupported review state")
        if type(limit) is not int or not 1 <= limit <= 500:
            raise ProductError("invalid_limit", "limit must be 1..500")
        if project_key:
            project_key = token(project_key, "project_key")
            return self.store.all("SELECT c.*,a.source_id,a.url,a.published_at FROM claims c JOIN captures a ON a.id=c.capture_id WHERE c.state=? AND c.project_key=? ORDER BY c.id LIMIT ?",
                                  (state, project_key, limit))
        return self.store.all("SELECT c.*,a.source_id,a.url,a.published_at FROM claims c JOIN captures a ON a.id=c.capture_id WHERE c.state=? ORDER BY c.id LIMIT ?",
                              (state, limit))

    def claim(self, claim_id):
        row = self.store.claim(claim_id)
        row["capture"] = self.store.capture(row["capture_id"])
        row["scope"] = self.store.scope(self._key(row))
        return row

    @staticmethod
    def _key(row):
        return (row["project_key"], row["kind"], row["metric"], row["period"])

    @staticmethod
    def _same(a, b):
        if a["kind"] == "milestone":
            return a["milestone"] == b["milestone"]
        return a["unit"] == b["unit"] and Decimal(a["amount"]) == Decimal(b["amount"])

    def compare(self, claim_id):
        claim = self.store.claim(claim_id)
        key = self._key(claim)
        candidates = self.store.candidates(key)
        scope = self.store.scope(key)
        accepted = [c for c in candidates if c["state"] == "accepted" and c["capture_state"] == "parsed"]
        groups = []
        for candidate in accepted:
            group = next((g for g in groups if self._same(g[0], candidate)), None)
            if group is None:
                groups.append([candidate])
            else:
                group.append(candidate)
        groups.sort(key=lambda group: (-min(c["trust_rank"] for c in group), -len(group), min(c["id"] for c in group)))
        return {"key": key, "scope": scope, "candidate_count": len(candidates),
                "accepted_count": len(accepted), "value_groups": [[c["id"] for c in group] for group in groups],
                "conflict": len(groups) > 1,
                "suggested_claim": groups[0][0]["id"] if groups else None}

    def review(self, claim_id, data):
        claim = self.store.claim(claim_id)
        if claim["state"] != "proposed":
            raise ProductError("already_reviewed", "claim has already been reviewed", 409)
        decision = data.get("decision")
        if decision not in {"accepted", "rejected", "hold"}:
            raise ProductError("invalid_decision", "decision must be accepted, rejected or hold")
        reviewer = token(data.get("reviewer"), "reviewer")
        reason = data.get("reason")
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 1000:
            raise ProductError("invalid_reason", "review requires a short reason")
        expected = data.get("expected_version")
        if type(expected) is not int or expected < 0:
            raise ProductError("invalid_version", "expected_version must be nonnegative")
        capture = self.store.capture(claim["capture_id"])
        if capture["state"] != "parsed":
            raise ProductError("capture_state", "claim capture is not parsed", 409)
        key = self._key(claim)
        candidates = self.store.candidates(key)
        accepted = [c for c in candidates if c["state"] == "accepted" and c["capture_state"] == "parsed"]
        if decision == "accepted":
            accepted.append({**claim, "trust_rank": self.store.source(capture["source_id"])["trust_rank"]})
        selected, conflict = self._selection(accepted)
        scope = self.store.record_review(claim_id, decision, reviewer, reason.strip(), expected, selected, conflict)
        return {"claim_id": claim_id, "decision": decision, "scope": scope}

    def _selection(self, accepted):
        if not accepted:
            return None, False
        groups = []
        for candidate in accepted:
            group = next((g for g in groups if self._same(g[0], candidate)), None)
            if group is None:
                groups.append([candidate])
            else:
                group.append(candidate)
        if len(groups) > 1:
            return None, True
        selected = sorted(groups[0], key=lambda c: (-c["trust_rank"], c["id"]))[0]
        return selected["id"], False

    def scopes(self, project_key=None, include_empty=True):
        if project_key:
            project_key = token(project_key, "project_key")
            rows = self.store.all("SELECT * FROM scopes WHERE project_key=? ORDER BY kind,metric,period", (project_key,))
        else:
            rows = self.store.all("SELECT * FROM scopes ORDER BY project_key,kind,metric,period")
        for row in rows:
            if row["selected_claim"]:
                claim = self.store.claim(row["selected_claim"])
                capture = self.store.capture(claim["capture_id"])
                row["selected"] = {"claim_id": claim["id"], "amount": claim["amount"],
                                   "unit": claim["unit"], "milestone": claim["milestone"],
                                   "quote": claim["quote"], "source_id": capture["source_id"],
                                   "url": capture["url"], "published_at": capture["published_at"]}
            else:
                row["selected"] = None
        return rows if include_empty else [row for row in rows if row["selected"]]

    def publish(self, data):
        name = token(data.get("name"), "name")
        project_key = data.get("project_key")
        scopes = self.scopes(project_key)
        if not scopes:
            raise ProductError("empty_release", "no reviewed scopes to publish", 409)
        blocked = [row for row in scopes if row["conflict"] or row["selected"] is None]
        if blocked:
            raise ProductError("unresolved_scope", f"{len(blocked)} scopes are unresolved", 409)
        row, created = self.store.save_snapshot(name, scopes)
        return {"snapshot_id": row["id"], "digest": row["digest"], "created": created,
                "scope_count": row["scope_count"], "claim_count": row["claim_count"]}

    def snapshot(self, snapshot_id):
        return self.store.snapshot(snapshot_id)

    def audit(self, since=0, limit=100):
        if type(since) is not int or since < 0 or type(limit) is not int or not 1 <= limit <= 500:
            raise ProductError("invalid_page", "invalid audit pagination")
        return self.store.audit_events(since, limit)
