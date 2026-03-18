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


def _aggregate_validation_common(raw_summary: dict, common_ids: set[int]) -> dict:
    """Aggregate per_object_validation for common_ids only. Returns same structure as _load_side_summary."""
    out = _default_side_summary("ok")
    pov = raw_summary.get("per_object_validation") or {}
    if not isinstance(pov, dict):
        return out
    for oid in common_ids:
        node_by_h = pov.get(str(oid), {})
        if not isinstance(node_by_h, dict):
            continue
        for horizon in VALIDATION_HORIZONS:
            node_by_t = node_by_h.get(horizon, {})
            if not isinstance(node_by_t, dict):
                continue
            for threshold in VALIDATION_THRESHOLDS:
                node = node_by_t.get(threshold, {})
                out[horizon][threshold]["ok"] += int(node.get("ok", 0) or 0)
                out[horizon][threshold]["total"] += int(node.get("total", 0) or 0)
    for horizon in VALIDATION_HORIZONS:
        for threshold in VALIDATION_THRESHOLDS:
            n = out[horizon][threshold]
            if n["total"] > 0:
                n["rate"] = (100.0 * float(n["ok"])) / float(n["total"])
            else:
                n["rate"] = 0.0
    return out


def _aggregate_collision_common(raw_summary: dict, common_ids: set[int]) -> dict:
    """Aggregate per_object_collision for common_ids only. Returns same structure as _load_side_collision."""
    out = _default_collision_side_summary("ok")
    poc = raw_summary.get("per_object_collision") or {}
    if not isinstance(poc, dict):
        return out
    for oid in common_ids:
        kind_d = poc.get(str(oid), {})
        if not isinstance(kind_d, dict):
            continue
        for kind in COLLISION_KINDS:
            node = kind_d.get(kind, {})
            out[kind]["collision_paths"] += int(node.get("collision_paths", 0) or 0)
            out[kind]["checked_paths"] += int(node.get("checked_paths", 0) or 0)
    for kind in COLLISION_KINDS:
        out[kind]["has_collision"] = out[kind]["collision_paths"] > 0
    return out


def _default_dt_summary(detail: str) -> dict:
    return {
        "dt_status": "valid",
        "dt_max": "-",
        "sample_count": 0,
        "detail": detail,
    }


def _normalize_dt_summary(node: dict | None, detail_fallback: str) -> dict:
    if not isinstance(node, dict):
        return _default_dt_summary(detail_fallback)
    status = str(node.get("dt_status") or "valid")
    dt_max = node.get("dt_max", "-")
    try:
        sample_count = max(0, int(node.get("sample_count", 0) or 0))
    except Exception:
        sample_count = 0
    return {
        "dt_status": status,
        "dt_max": dt_max,
        "sample_count": sample_count,
        "detail": str(node.get("detail") or detail_fallback),
    }


def _load_dt_summary(path: Path, side: str) -> dict:
    return _normalize_dt_summary(
        _load_json_dict(path),
        detail_fallback=f"{side}_dt_summary_missing",
    )


def _load_dt_subscenes(path: Path, side: str) -> dict[int, dict]:
    raw = _load_json_dict(path)
    if raw is None:
        return {}
    raw_subscenes = raw.get("subscenes", [])
    if not isinstance(raw_subscenes, list):
        return {}
    out: dict[int, dict] = {}
    for idx, node in enumerate(raw_subscenes):
        if not isinstance(node, dict):
            continue
        meta = _normalize_subscene_meta(node, idx)
        sub = dict(meta)
        sub.update(
            _normalize_dt_summary(
                node,
                detail_fallback=f"{side}_subscene_{meta['index']}_dt_summary_missing",
            )
        )
        out[meta["index"]] = sub
    return out


def _normalize_subscene_meta(node: dict, fallback_index: int) -> dict:
    if not isinstance(node, dict):
        node = {}
    start_offset = float(node.get("start_offset_sec", 0.0) or 0.0)
    end_offset = float(node.get("end_offset_sec", start_offset) or start_offset)
    raw_index = node.get("index", fallback_index)
    try:
        index = int(fallback_index if raw_index is None else raw_index)
    except Exception:
        index = int(fallback_index)
    return {
        "index": index,
        "label": str(node.get("label", "")),
        "start_offset_sec": start_offset,
        "end_offset_sec": end_offset,
        "duration_sec": float(node.get("duration_sec", max(0.0, end_offset - start_offset)) or 0.0),
    }


def _load_side_subscenes(path: Path, side: str) -> dict[int, dict]:
    raw = _load_json_dict(path)
    if raw is None:
        return {}
    raw_subscenes = raw.get("subscenes", [])
    if not isinstance(raw_subscenes, list):
        return {}
    out: dict[int, dict] = {}
    for idx, node in enumerate(raw_subscenes):
        if not isinstance(node, dict):
            continue
        meta = _normalize_subscene_meta(node, idx)
        sub = dict(meta)
        for horizon in VALIDATION_HORIZONS:
            hnode = node.get(horizon, node.get(horizon.replace("-", "_"), {}))
            if not isinstance(hnode, dict):
                hnode = {}
            sub[horizon] = {}
            for threshold in VALIDATION_THRESHOLDS:
                sub[horizon][threshold] = _normalize_threshold_summary(
                    hnode.get(threshold, {}),
                    detail_fallback=f"{side}_subscene_{meta['index']}_{horizon}_{threshold}_summary_missing",
                )
        collision_raw = node.get("collision", {})
        if not isinstance(collision_raw, dict):
            collision_raw = {}
        sub["collision"] = {}
        for kind in COLLISION_KINDS:
            sub["collision"][kind] = _normalize_collision_kind_summary(
                collision_raw.get(kind, {}),
                detail_fallback=f"{side}_subscene_{meta['index']}_{kind}_collision_summary_missing",
            )
        out[meta["index"]] = sub
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
        baseline_dt_summary_path = baseline_root / rel / "dt_summary.json"
        test_dt_summary_path = out_dir / "dt_summary.json"
        baseline_summary_raw = _load_json_dict(baseline_summary_path) or {}
        test_summary_raw = _load_json_dict(test_summary_path) or {}

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

        direct_subscenes = comp.get("subscenes", [])
        if not isinstance(direct_subscenes, list):
            direct_subscenes = []
        scene_timing = comp.get("scene_timing")
        if not isinstance(scene_timing, dict):
            scene_timing = baseline_summary_raw.get("scene_timing")
        if not isinstance(scene_timing, dict):
            scene_timing = test_summary_raw.get("scene_timing")
        if isinstance(scene_timing, dict):
            comp["scene_timing"] = scene_timing

        comp["validation"] = {
            "baseline": _load_side_summary(baseline_summary_path, "baseline"),
            "test": _load_side_summary(test_summary_path, "test"),
        }
        comp["collision"] = {
            "baseline": _load_side_collision(baseline_summary_path, "baseline"),
            "test": _load_side_collision(test_summary_path, "test"),
        }
        # Common-object aggregates: only objects evaluated in both baseline and test
        def _oid_set(lst):
            s = set()
            for x in lst or []:
                try:
                    s.add(int(x))
                except (TypeError, ValueError):
                    pass
            return s
        baseline_oids = _oid_set(baseline_summary_raw.get("evaluated_object_ids"))
        test_oids = _oid_set(test_summary_raw.get("evaluated_object_ids"))
        common_ids = baseline_oids & test_oids
        comp["validation"]["baseline_common"] = _aggregate_validation_common(baseline_summary_raw, common_ids)
        comp["validation"]["test_common"] = _aggregate_validation_common(test_summary_raw, common_ids)
        comp["collision"]["baseline_common"] = _aggregate_collision_common(baseline_summary_raw, common_ids)
        comp["collision"]["test_common"] = _aggregate_collision_common(test_summary_raw, common_ids)
        comp["common_object_ids"] = sorted(common_ids)
        baseline_dt = _load_dt_summary(baseline_dt_summary_path, "baseline")
        test_dt = _load_dt_summary(test_dt_summary_path, "test")
        comp["dt_status_baseline"] = baseline_dt["dt_status"]
        comp["dt_max_baseline"] = baseline_dt["dt_max"]
        comp["dt_status_test"] = test_dt["dt_status"]
        comp["dt_max_test"] = test_dt["dt_max"]

        baseline_subscene_map = _load_side_subscenes(baseline_summary_path, "baseline")
        test_subscene_map = _load_side_subscenes(test_summary_path, "test")
        baseline_dt_subscene_map = _load_dt_subscenes(baseline_dt_summary_path, "baseline")
        test_dt_subscene_map = _load_dt_subscenes(test_dt_summary_path, "test")
        direct_subscene_map: dict[int, dict] = {}
        ordered_indices: list[int] = []
        for idx, node in enumerate(direct_subscenes):
            if not isinstance(node, dict):
                continue
            meta = _normalize_subscene_meta(node, idx)
            merged_direct = dict(node)
            merged_direct.update(meta)
            direct_subscene_map[meta["index"]] = merged_direct
            ordered_indices.append(meta["index"])
        for node in ((scene_timing or {}).get("subscenes", []) if isinstance(scene_timing, dict) else []):
            meta = _normalize_subscene_meta(node, len(ordered_indices))
            if meta["index"] not in ordered_indices:
                ordered_indices.append(meta["index"])
        for idx in baseline_subscene_map:
            if idx not in ordered_indices:
                ordered_indices.append(idx)
        for idx in test_subscene_map:
            if idx not in ordered_indices:
                ordered_indices.append(idx)
        for idx in baseline_dt_subscene_map:
            if idx not in ordered_indices:
                ordered_indices.append(idx)
        for idx in test_dt_subscene_map:
            if idx not in ordered_indices:
                ordered_indices.append(idx)

        comp_subscenes = []
        for index in ordered_indices:
            sub = dict(direct_subscene_map.get(index, {}))
            if not sub:
                meta_source = baseline_subscene_map.get(index) or test_subscene_map.get(index)
                if meta_source is not None:
                    sub.update(_normalize_subscene_meta(meta_source, index))
                    sub["directory"] = None
                    sub["lane_ids_ok"] = None
                    sub["vsl_ok"] = None
                    sub["object_ids_ok"] = None
                    sub["path_ok"] = None
                    sub["traffic_ok"] = None
                    sub["overall_ok"] = False
                    sub["diff_by_source"] = {
                        "lane": {k: False for k in ("along", "opposite", "crossing")},
                        "vsl": {k: False for k in ("along", "opposite", "crossing")},
                        "object_ids": {k: False for k in ("along", "opposite", "crossing", "other", "base")},
                        "path": {k: False for k in ("along", "opposite", "crossing", "other", "base")},
                    }
                    sub["diff_counts_by_source"] = {
                        "lane": {k: 0 for k in ("along", "opposite", "crossing")},
                        "vsl": {k: 0 for k in ("along", "opposite", "crossing")},
                        "object_ids": {k: 0 for k in ("along", "opposite", "crossing", "other", "base")},
                        "path": {k: 0 for k in ("along", "opposite", "crossing", "other", "base")},
                    }
            if "directory" not in sub:
                sub["directory"] = None
            sub["validation"] = {
                "baseline": {
                    horizon: baseline_subscene_map.get(index, {}).get(horizon, _default_side_summary("baseline_subscene_missing")[horizon])
                    for horizon in VALIDATION_HORIZONS
                },
                "test": {
                    horizon: test_subscene_map.get(index, {}).get(horizon, _default_side_summary("test_subscene_missing")[horizon])
                    for horizon in VALIDATION_HORIZONS
                },
            }
            # Flatten the side summary structure back to the same schema as scene-level comp["validation"][side].
            sub["validation"]["baseline"] = {
                horizon: {
                    threshold: baseline_subscene_map.get(index, {}).get(horizon, {}).get(
                        threshold,
                        _default_threshold_summary(f"baseline_subscene_{index}_{horizon}_{threshold}_missing"),
                    )
                    for threshold in VALIDATION_THRESHOLDS
                }
                for horizon in VALIDATION_HORIZONS
            }
            sub["validation"]["test"] = {
                horizon: {
                    threshold: test_subscene_map.get(index, {}).get(horizon, {}).get(
                        threshold,
                        _default_threshold_summary(f"test_subscene_{index}_{horizon}_{threshold}_missing"),
                    )
                    for threshold in VALIDATION_THRESHOLDS
                }
                for horizon in VALIDATION_HORIZONS
            }
            sub["collision"] = {
                "baseline": baseline_subscene_map.get(index, {}).get(
                    "collision",
                    _default_collision_side_summary(f"baseline_subscene_{index}_collision_missing"),
                ),
                "test": test_subscene_map.get(index, {}).get(
                    "collision",
                    _default_collision_side_summary(f"test_subscene_{index}_collision_missing"),
                ),
            }
            baseline_dt_sub = baseline_dt_subscene_map.get(index, _default_dt_summary(f"baseline_subscene_{index}_dt_missing"))
            test_dt_sub = test_dt_subscene_map.get(index, _default_dt_summary(f"test_subscene_{index}_dt_missing"))
            sub["dt_status_baseline"] = baseline_dt_sub.get("dt_status", "valid")
            sub["dt_max_baseline"] = baseline_dt_sub.get("dt_max", "-")
            sub["dt_status_test"] = test_dt_sub.get("dt_status", "valid")
            sub["dt_max_test"] = test_dt_sub.get("dt_max", "-")
            comp_subscenes.append(sub)
        comp["subscenes"] = comp_subscenes

        with open(merged_path, "w", encoding="utf-8") as f:
            json.dump(comp, f, indent=2, ensure_ascii=False)
        written += 1

    print(f"[aggregate] wrote {written} comparison.json file(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
