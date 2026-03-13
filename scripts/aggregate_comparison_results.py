#!/usr/bin/env python3
"""Aggregate direct compare JSON and observed-validation JSON into comparison.json."""
from __future__ import annotations

import json
from pathlib import Path

from common import discover_bag_directories, get_tester_root, load_settings

VALIDATION_HORIZONS = ("half-time-relaxed", "time-relaxed")
VALIDATION_THRESHOLDS = ("approximate", "strict")
COLLISION_KINDS = ("hard", "soft")


def _default_threshold_summary(detail: str) -> dict:
    return {
        "ok": 0,
        "total": 0,
        "rate": 0.0,
        "detail": detail,
    }


def _default_side_summary(detail: str) -> dict:
    return {
        horizon: {threshold: _default_threshold_summary(detail) for threshold in VALIDATION_THRESHOLDS}
        for horizon in VALIDATION_HORIZONS
    }


def _default_collision_kind_summary(detail: str) -> dict:
    return {
        "has_collision": False,
        "collision_paths": 0,
        "checked_paths": 0,
        "detail": detail,
    }


def _default_collision_side_summary(detail: str) -> dict:
    return {kind: _default_collision_kind_summary(detail) for kind in COLLISION_KINDS}


def _load_json_dict(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _normalize_threshold_summary(node: dict, detail_fallback: str) -> dict:
    if not isinstance(node, dict):
        return _default_threshold_summary(detail_fallback)
    try:
        ok = int(node.get("ok", 0) or 0)
    except Exception:
        ok = 0
    try:
        total = int(node.get("total", 0) or 0)
    except Exception:
        total = 0
    try:
        rate = float(node.get("rate", 0.0) or 0.0)
    except Exception:
        rate = 0.0
    if total > 0 and (rate <= 0.0 and ok > 0):
        rate = (100.0 * float(ok)) / float(total)
    detail = str(node.get("detail") or detail_fallback)
    return {
        "ok": ok,
        "total": total,
        "rate": rate,
        "detail": detail,
    }


def _load_side_summary(path: Path, side: str) -> dict:
    raw = _load_json_dict(path)
    if raw is None:
        return _default_side_summary(f"{side}_summary_missing")

    out = _default_side_summary(f"{side}_summary_missing")
    has_horizon_keys = any((h in raw) or (h.replace("-", "_") in raw) for h in VALIDATION_HORIZONS)
    if has_horizon_keys:
        for horizon in VALIDATION_HORIZONS:
            hnode = raw.get(horizon, raw.get(horizon.replace("-", "_"), {}))
            if not isinstance(hnode, dict):
                hnode = {}
            for threshold in VALIDATION_THRESHOLDS:
                node = hnode.get(threshold, {})
                if threshold == "strict" and (not isinstance(node, dict) or not node):
                    # Backward compatibility: old "time-relaxed as threshold".
                    if horizon == "time-relaxed":
                        node = hnode.get("time-relaxed", {}) or hnode.get("loose", {})
                out[horizon][threshold] = _normalize_threshold_summary(
                    node,
                    detail_fallback=f"{side}_{horizon}_{threshold}_summary_missing",
                )
    else:
        # Backward compatibility: old summary schema had only threshold keys.
        out["half-time-relaxed"]["strict"] = _normalize_threshold_summary(
            raw.get("half-time-relaxed", {}) or raw.get("half_time_relaxed", {}),
            detail_fallback=f"{side}_half-time-relaxed_strict_summary_missing",
        )
        out["time-relaxed"]["strict"] = _normalize_threshold_summary(
            raw.get("time-relaxed", {}) or raw.get("loose", {}),
            detail_fallback=f"{side}_time-relaxed_strict_summary_missing",
        )
    return out


def _normalize_collision_kind_summary(node: dict, detail_fallback: str) -> dict:
    if not isinstance(node, dict):
        return _default_collision_kind_summary(detail_fallback)
    has_collision = bool(node.get("has_collision", False))
    try:
        collision_paths = max(0, int(node.get("collision_paths", 0) or 0))
    except Exception:
        collision_paths = 0
    try:
        checked_paths = max(0, int(node.get("checked_paths", 0) or 0))
    except Exception:
        checked_paths = 0
    detail = str(node.get("detail") or detail_fallback)
    if collision_paths > 0:
        has_collision = True
    return {
        "has_collision": has_collision,
        "collision_paths": collision_paths,
        "checked_paths": checked_paths,
        "detail": detail,
    }


def _load_side_collision(path: Path, side: str) -> dict:
    raw = _load_json_dict(path)
    if raw is None:
        return _default_collision_side_summary(f"{side}_collision_summary_missing")
    collision = raw.get("collision", {})
    if not isinstance(collision, dict):
        return _default_collision_side_summary(f"{side}_collision_summary_missing")
    out = _default_collision_side_summary(f"{side}_collision_summary_missing")
    for kind in COLLISION_KINDS:
        out[kind] = _normalize_collision_kind_summary(
            collision.get(kind, {}),
            detail_fallback=f"{side}_{kind}_collision_summary_missing",
        )
    return out


def main() -> int:
    root = get_tester_root()
    settings = load_settings(root)
    paths = settings["paths"]

    baseline_root = root / paths["baseline_results"]
    test_results_root = root / paths["test_results"]
    dirs = discover_bag_directories(root / paths["test_bags"])
    if not dirs:
        return 0

    written = 0
    for rel, _dir_path in dirs:
        out_dir = test_results_root / rel
        out_dir.mkdir(parents=True, exist_ok=True)

        direct_path = out_dir / "comparison_direct.json"
        merged_path = out_dir / "comparison.json"
        baseline_summary_path = baseline_root / rel / "validation_baseline_summary.json"
        test_summary_path = out_dir / "validation_test_summary.json"

        comp = _load_json_dict(direct_path)
        if comp is None:
            comp = {
                "directory": rel,
                "lane_ids_ok": None,
                "vsl_ok": None,
                "object_ids_ok": None,
                "path_ok": None,
                "traffic_ok": None,
                "overall_ok": False,
                "detail": "comparison_direct_missing",
            }

        comp["validation"] = {
            "baseline": _load_side_summary(baseline_summary_path, "baseline"),
            "test": _load_side_summary(test_summary_path, "test"),
        }
        comp["collision"] = {
            "baseline": _load_side_collision(baseline_summary_path, "baseline"),
            "test": _load_side_collision(test_summary_path, "test"),
        }

        with open(merged_path, "w", encoding="utf-8") as f:
            json.dump(comp, f, indent=2, ensure_ascii=False)
        written += 1

    print(f"[aggregate] wrote {written} comparison.json file(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
