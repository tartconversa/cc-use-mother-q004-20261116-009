"""铁路新闻证据系统：Command line entry point for local manifest replay and snapshot inspection."""
from __future__ import annotations

import argparse
import json
import sys

from .batch import BatchRunner
from .domain import ProductError
from .service import RailwayService


def parser():
    root = argparse.ArgumentParser(prog="railwatch")
    root.add_argument("--db", default="railwatch.sqlite3")
    commands = root.add_subparsers(dest="command", required=True)
    replay = commands.add_parser("replay")
    replay.add_argument("manifest")
    replay.add_argument("--offset", type=int, default=0)
    replay.add_argument("--limit", type=int)
    replay.add_argument("--checkpoint")
    replay.add_argument("--stop-on-error", action="store_true")
    source = commands.add_parser("source")
    source.add_argument("policy_json")
    source.add_argument("--expected-version", type=int)
    review = commands.add_parser("review")
    review.add_argument("claim_id", type=int)
    review.add_argument("decision_json")
    snapshot = commands.add_parser("snapshot")
    snapshot.add_argument("name")
    snapshot.add_argument("--project-key")
    reprocess = commands.add_parser("reprocess")
    reprocess.add_argument("capture_id", type=int)
    reprocess.add_argument("--rule-version", type=int)
    reprocess.add_argument("--trigger", default="manual")
    reprocess_pending = commands.add_parser("reprocess-pending")
    reprocess_pending.add_argument("--rule-version", type=int)
    reprocess_pending.add_argument("--limit", type=int, default=500)
    attempts = commands.add_parser("attempts")
    attempts.add_argument("capture_id", type=int)
    review_history = commands.add_parser("review-history")
    review_history.add_argument("claim_id", type=int)
    show = commands.add_parser("show")
    show.add_argument("kind", choices=["scopes", "audit", "claims", "snapshot"])
    show.add_argument("--id", type=int)
    show.add_argument("--project-key")
    return root


def execute(args):
    service = RailwayService(args.db)
    if args.command == "source":
        return service.register_source(json.loads(args.policy_json), args.expected_version)
    if args.command == "replay":
        runner = BatchRunner(service)
        if args.checkpoint:
            return runner.resume(args.manifest, args.checkpoint, args.limit, args.stop_on_error)
        return runner.run_path(args.manifest, args.offset, args.limit, args.stop_on_error)
    if args.command == "review":
        return service.review(args.claim_id, json.loads(args.decision_json))
    if args.command == "snapshot":
        return service.publish({"name": args.name, "project_key": args.project_key})
    if args.command == "reprocess":
        return service.reprocess(args.capture_id, args.rule_version, args.trigger)
    if args.command == "reprocess-pending":
        return service.reprocess_pending(args.rule_version, args.limit)
    if args.command == "attempts":
        return service.parse_attempts(args.capture_id)
    if args.command == "review-history":
        return service.claim_reviews(args.claim_id)
    if args.kind == "scopes":
        return service.scopes(args.project_key)
    if args.kind == "audit":
        return service.audit()
    if args.kind == "claims":
        return service.review_queue(args.project_key)
    if args.id is None:
        raise ProductError("missing_id", "snapshot requires --id")
    return service.snapshot(args.id)


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        result = execute(args)
    except (ProductError, ValueError) as exc:
        if isinstance(exc, ProductError):
            error = {"error": exc.code, "detail": exc.detail}
        else:
            error = {"error": "invalid_json", "detail": str(exc)}
        print(json.dumps(error, ensure_ascii=False), file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
