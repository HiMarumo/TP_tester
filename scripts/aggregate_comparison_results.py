#!/usr/bin/env python3
"""Aggregate direct compare JSON and observed-validation JSON into comparison.json."""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

from common import discover_bag_directories, find_subscene_index, get_tester_root, load_settings

try:
    import rosbag

    HAS_ROSBAG = True
except Exception:
    HAS_ROSBAG = False

VALIDATION_HORIZONS = ("half-time-relaxed", "time-relaxed")
VALIDATION_THRESHOLDS = ("approximate", "strict")
COLLISION_KINDS = ("hard", "soft")
COLLISION_SOURCE_GROUPS = ("along", "opposite", "crossing")
PATH_CLASS_GROUPS = ("four_wheel", "two_wheel", "pedestrian")
VALIDATION_SAMPLE_TOPIC_RE = re.compile(
    r"^/validation/eval/(?P<side>baseline|test)/(?P<horizon>[^/]+)/(?P<threshold>[^/]+)/"
    r"(?P<group>along|opposite|crossing|other)/(?P<status>optimal|observed_ok|observed_ng)/WM/tracked_object_set_with_prediction$"
)


def _print_progress_line(prefix: str, done: int, total: int) -> None:
    if total <= 0:
        return
    done = max(0, min(done, total))
    width = 30
    fill = int((done * width) / total)
    bar = "#" * fill + "-" * (width - fill)
    percent = int((done * 100) / total)
    print(f"{prefix} [{bar}] {percent}% ({done}/{total})", file=sys.stderr, flush=True)


def _print_live_progress(prefix: str, done: int, total: int, state: dict) -> None:
    if total <= 0:
        return
    done = max(0, min(done, total))
    percent = int((done * 100) / total)
    last_percent = int(state.get("last_percent", -1) or -1)
    if percent == last_percent and done < total:
        return
    state["last_percent"] = percent
    width = 30
    fill = int((done * width) / total)
    bar = "#" * fill + "-" * (width - fill)
    line = f"{prefix} [{bar}] {percent}% ({done}/{total})"
    last_len = int(state.get("last_len", 0) or 0)
    pad = " " * max(0, last_len - len(line))
    state["last_len"] = len(line)
    if done >= total:
        print(f"\r{line}{pad}", file=sys.stderr, flush=True)
        state["last_len"] = 0
    else:
        print(f"\r{line}{pad}", end="", file=sys.stderr, flush=True)


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
        "by_group": {
            group: {
                "has_collision": False,
                "collision_paths": 0,
                "checked_paths": 0,
                "detail": detail,
            }
            for group in COLLISION_SOURCE_GROUPS
        },
        "detail": detail,
    }


def _default_collision_side_summary(detail: str) -> dict:
    return {kind: _default_collision_kind_summary(detail) for kind in COLLISION_KINDS}


def _default_path_class_group_validation(detail: str) -> dict:
    return {
        class_group: {
            horizon: {
                threshold: _default_threshold_summary(detail)
                for threshold in VALIDATION_THRESHOLDS
            }
            for horizon in VALIDATION_HORIZONS
        }
        for class_group in PATH_CLASS_GROUPS
    }


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


def _load_side_path_class_group_validation(path: Path, side: str) -> dict:
    raw = _load_json_dict(path)
    if raw is None:
        return _default_path_class_group_validation(f"{side}_path_class_group_missing")
    class_raw = raw.get("path_class_group_validation", {})
    if not isinstance(class_raw, dict):
        return _default_path_class_group_validation(f"{side}_path_class_group_missing")
    out = _default_path_class_group_validation("ok")
    for class_group in PATH_CLASS_GROUPS:
        group_node = class_raw.get(class_group, {})
        if not isinstance(group_node, dict):
            group_node = {}
        for horizon in VALIDATION_HORIZONS:
            hnode = group_node.get(horizon, group_node.get(horizon.replace("-", "_"), {}))
            if not isinstance(hnode, dict):
                hnode = {}
            for threshold in VALIDATION_THRESHOLDS:
                out[class_group][horizon][threshold] = _normalize_threshold_summary(
                    hnode.get(threshold, {}),
                    detail_fallback=f"{side}_{class_group}_{horizon}_{threshold}_summary_missing",
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
    by_group_raw = node.get("by_group", {})
    by_group = {}
    for group in COLLISION_SOURCE_GROUPS:
        gnode = by_group_raw.get(group, {}) if isinstance(by_group_raw, dict) else {}
        if not isinstance(gnode, dict):
            gnode = {}
        try:
            g_collision_paths = max(0, int(gnode.get("collision_paths", 0) or 0))
        except Exception:
            g_collision_paths = 0
        try:
            g_checked_paths = max(0, int(gnode.get("checked_paths", 0) or 0))
        except Exception:
            g_checked_paths = 0
        g_has_collision = bool(gnode.get("has_collision", False) or (g_collision_paths > 0))
        by_group[group] = {
            "has_collision": g_has_collision,
            "collision_paths": g_collision_paths,
            "checked_paths": g_checked_paths,
            "detail": str(gnode.get("detail") or detail_fallback),
        }

    return {
        "has_collision": has_collision,
        "collision_paths": collision_paths,
        "checked_paths": checked_paths,
        "by_group": by_group,
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


def _to_oid_set(values) -> set[int]:
    out = set()
    for value in values or []:
        try:
            out.add(int(value))
        except (TypeError, ValueError):
            continue
    return out


def _to_nsec(ts) -> int:
    if ts is None:
        return 0
    if hasattr(ts, "to_nsec"):
        try:
            return int(ts.to_nsec())
        except Exception:
            return 0
    sec = getattr(ts, "secs", getattr(ts, "sec", 0)) or 0
    nsec = getattr(ts, "nsecs", getattr(ts, "nsec", getattr(ts, "nanosec", 0))) or 0
    try:
        return int(sec) * 10**9 + int(nsec)
    except Exception:
        return 0


def _msg_stamp_ns(msg, bag_t) -> int:
    if msg is None:
        return _to_nsec(bag_t)
    header = getattr(msg, "header", None)
    stamp = getattr(header, "stamp", None) if header is not None else None
    if stamp is not None:
        return _to_nsec(stamp)
    return _to_nsec(bag_t)


def _obj_id_of(obj) -> int:
    base_obj = getattr(obj, "object", obj)
    try:
        return int(getattr(base_obj, "object_id", -1))
    except Exception:
        return -1


def _new_validation_sample_sets() -> dict:
    return {
        horizon: {
            threshold: {"ok": set(), "total": set()}
            for threshold in VALIDATION_THRESHOLDS
        }
        for horizon in VALIDATION_HORIZONS
    }


def _extract_validation_sample_sets(
    validation_bag_path: Path,
    side: str,
    scene_timing: dict | None,
    evaluated_scene_oids: set[int] | None = None,
    evaluated_subscene_oids: dict[int, set[int]] | None = None,
    scene_path_class_group_by_oid: dict[int, str] | None = None,
    subscene_path_class_group_by_oid: dict[int, dict[int, str]] | None = None,
    progress_prefix: str | None = None,
) -> tuple[dict, dict[int, dict], dict[str, dict], dict[int, dict[str, dict]], bool]:
    scene_sets = _new_validation_sample_sets()
    subscene_sets: dict[int, dict] = {}
    scene_sets_by_class = {class_group: _new_validation_sample_sets() for class_group in PATH_CLASS_GROUPS}
    subscene_sets_by_class: dict[int, dict[str, dict]] = {}
    scene_observed_ok = _new_validation_sample_sets()
    scene_observed_ng = _new_validation_sample_sets()
    scene_optimal = _new_validation_sample_sets()
    subscene_observed_ok: dict[int, dict] = {}
    subscene_observed_ng: dict[int, dict] = {}
    subscene_optimal: dict[int, dict] = {}
    scene_observed_ok_by_class = {class_group: _new_validation_sample_sets() for class_group in PATH_CLASS_GROUPS}
    scene_observed_ng_by_class = {class_group: _new_validation_sample_sets() for class_group in PATH_CLASS_GROUPS}
    scene_optimal_by_class = {class_group: _new_validation_sample_sets() for class_group in PATH_CLASS_GROUPS}
    subscene_observed_ok_by_class: dict[int, dict[str, dict]] = {}
    subscene_observed_ng_by_class: dict[int, dict[str, dict]] = {}
    subscene_optimal_by_class: dict[int, dict[str, dict]] = {}
    has_optimal_topic = {
        horizon: {threshold: False for threshold in VALIDATION_THRESHOLDS}
        for horizon in VALIDATION_HORIZONS
    }
    if not HAS_ROSBAG or not validation_bag_path.exists():
        return scene_sets, subscene_sets, scene_sets_by_class, subscene_sets_by_class, False
    try:
        with rosbag.Bag(str(validation_bag_path), "r") as bag:
            info = bag.get_type_and_topic_info()
            raw_topics = info[1] if isinstance(info, tuple) and len(info) >= 2 else getattr(info, "topics", {})
            topic_names = raw_topics.keys() if hasattr(raw_topics, "keys") else raw_topics

            topic_meta: dict[str, tuple[str, str, str]] = {}
            target_topics: list[str] = []
            for topic in topic_names:
                m = VALIDATION_SAMPLE_TOPIC_RE.match(str(topic))
                if not m:
                    continue
                if m.group("side") != side:
                    continue
                horizon = m.group("horizon").replace("_", "-")
                threshold = m.group("threshold").replace("_", "-")
                status = m.group("status")
                if horizon not in VALIDATION_HORIZONS or threshold not in VALIDATION_THRESHOLDS:
                    continue
                if status == "optimal":
                    has_optimal_topic[horizon][threshold] = True
                topic_str = str(topic)
                topic_meta[topic_str] = (horizon, threshold, status)
                target_topics.append(topic_str)

            if not target_topics:
                return scene_sets, subscene_sets, scene_sets_by_class, subscene_sets_by_class, False

            read_total = 0
            try:
                read_total = max(0, int(bag.get_message_count(topic_filters=target_topics) or 0))
            except Exception:
                read_total = 0
            read_done = 0
            progress_state = {"last_percent": -1, "last_len": 0}
            if progress_prefix and read_total > 0:
                _print_live_progress(progress_prefix, 0, read_total, progress_state)

            for topic, msg, bag_t in bag.read_messages(topics=target_topics):
                read_done += 1
                horizon, threshold, status = topic_meta.get(str(topic), ("", "", ""))
                if not horizon or not threshold:
                    if progress_prefix and read_total > 0:
                        _print_live_progress(progress_prefix, read_done, read_total, progress_state)
                    continue
                stamp_ns = _msg_stamp_ns(msg, bag_t)
                subscene_index = find_subscene_index(stamp_ns, scene_timing)
                if subscene_index is not None and subscene_index not in subscene_sets:
                    subscene_sets[subscene_index] = _new_validation_sample_sets()
                    subscene_observed_ok[subscene_index] = _new_validation_sample_sets()
                    subscene_observed_ng[subscene_index] = _new_validation_sample_sets()
                    subscene_optimal[subscene_index] = _new_validation_sample_sets()
                    subscene_sets_by_class[subscene_index] = {
                        class_group: _new_validation_sample_sets()
                        for class_group in PATH_CLASS_GROUPS
                    }
                    subscene_observed_ok_by_class[subscene_index] = {
                        class_group: _new_validation_sample_sets()
                        for class_group in PATH_CLASS_GROUPS
                    }
                    subscene_observed_ng_by_class[subscene_index] = {
                        class_group: _new_validation_sample_sets()
                        for class_group in PATH_CLASS_GROUPS
                    }
                    subscene_optimal_by_class[subscene_index] = {
                        class_group: _new_validation_sample_sets()
                        for class_group in PATH_CLASS_GROUPS
                    }

                objects = getattr(msg, "objects", []) or []
                for obj in objects:
                    oid = _obj_id_of(obj)
                    if oid < 0:
                        continue
                    if evaluated_scene_oids is not None and oid not in evaluated_scene_oids:
                        continue
                    key = (int(stamp_ns), int(oid))
                    class_group = None
                    if isinstance(scene_path_class_group_by_oid, dict):
                        class_group = scene_path_class_group_by_oid.get(int(oid))
                    if status == "observed_ok":
                        scene_observed_ok[horizon][threshold]["ok"].add(key)
                    elif status == "observed_ng":
                        scene_observed_ng[horizon][threshold]["total"].add(key)
                    elif status == "optimal":
                        scene_optimal[horizon][threshold]["ok"].add(key)
                    if class_group in PATH_CLASS_GROUPS:
                        if status == "observed_ok":
                            scene_observed_ok_by_class[class_group][horizon][threshold]["ok"].add(key)
                        elif status == "observed_ng":
                            scene_observed_ng_by_class[class_group][horizon][threshold]["total"].add(key)
                        elif status == "optimal":
                            scene_optimal_by_class[class_group][horizon][threshold]["ok"].add(key)
                    if subscene_index is not None:
                        allow_subscene = True
                        if evaluated_subscene_oids is not None:
                            allowed_ids = evaluated_subscene_oids.get(int(subscene_index))
                            if allowed_ids is not None and oid not in allowed_ids:
                                allow_subscene = False
                        if allow_subscene:
                            sub_class_group = class_group
                            if isinstance(subscene_path_class_group_by_oid, dict):
                                sub_class_group = (
                                    subscene_path_class_group_by_oid.get(int(subscene_index), {}) or {}
                                ).get(int(oid), sub_class_group)
                            if status == "observed_ok":
                                subscene_observed_ok[subscene_index][horizon][threshold]["ok"].add(key)
                            elif status == "observed_ng":
                                subscene_observed_ng[subscene_index][horizon][threshold]["total"].add(key)
                            elif status == "optimal":
                                subscene_optimal[subscene_index][horizon][threshold]["ok"].add(key)
                            if sub_class_group in PATH_CLASS_GROUPS:
                                if status == "observed_ok":
                                    subscene_observed_ok_by_class[subscene_index][sub_class_group][horizon][threshold]["ok"].add(key)
                                elif status == "observed_ng":
                                    subscene_observed_ng_by_class[subscene_index][sub_class_group][horizon][threshold]["total"].add(key)
                                elif status == "optimal":
                                    subscene_optimal_by_class[subscene_index][sub_class_group][horizon][threshold]["ok"].add(key)
                if progress_prefix and read_total > 0:
                    _print_live_progress(progress_prefix, read_done, read_total, progress_state)
            if progress_prefix and read_total > 0 and read_done < read_total:
                _print_live_progress(progress_prefix, read_total, read_total, progress_state)
    except Exception:
        return _new_validation_sample_sets(), {}, {class_group: _new_validation_sample_sets() for class_group in PATH_CLASS_GROUPS}, {}, False

    for horizon in VALIDATION_HORIZONS:
        for threshold in VALIDATION_THRESHOLDS:
            ok_candidates = scene_observed_ok[horizon][threshold]["ok"]
            ng_keys = scene_observed_ng[horizon][threshold]["total"]
            if has_optimal_topic[horizon][threshold]:
                ok_keys = ok_candidates & scene_optimal[horizon][threshold]["ok"]
            else:
                ok_keys = set(ok_candidates)
            scene_sets[horizon][threshold]["ok"] = ok_keys
            scene_sets[horizon][threshold]["total"] = set(ok_keys) | set(ng_keys)
            for class_group in PATH_CLASS_GROUPS:
                ok_candidates_cls = scene_observed_ok_by_class[class_group][horizon][threshold]["ok"]
                ng_keys_cls = scene_observed_ng_by_class[class_group][horizon][threshold]["total"]
                if has_optimal_topic[horizon][threshold]:
                    ok_keys_cls = ok_candidates_cls & scene_optimal_by_class[class_group][horizon][threshold]["ok"]
                else:
                    ok_keys_cls = set(ok_candidates_cls)
                scene_sets_by_class[class_group][horizon][threshold]["ok"] = ok_keys_cls
                scene_sets_by_class[class_group][horizon][threshold]["total"] = set(ok_keys_cls) | set(ng_keys_cls)

    for subscene_index in subscene_sets.keys():
        for horizon in VALIDATION_HORIZONS:
            for threshold in VALIDATION_THRESHOLDS:
                ok_candidates = subscene_observed_ok[subscene_index][horizon][threshold]["ok"]
                ng_keys = subscene_observed_ng[subscene_index][horizon][threshold]["total"]
                if has_optimal_topic[horizon][threshold]:
                    ok_keys = ok_candidates & subscene_optimal[subscene_index][horizon][threshold]["ok"]
                else:
                    ok_keys = set(ok_candidates)
                subscene_sets[subscene_index][horizon][threshold]["ok"] = ok_keys
                subscene_sets[subscene_index][horizon][threshold]["total"] = set(ok_keys) | set(ng_keys)
                for class_group in PATH_CLASS_GROUPS:
                    ok_candidates_cls = subscene_observed_ok_by_class[subscene_index][class_group][horizon][threshold]["ok"]
                    ng_keys_cls = subscene_observed_ng_by_class[subscene_index][class_group][horizon][threshold]["total"]
                    if has_optimal_topic[horizon][threshold]:
                        ok_keys_cls = ok_candidates_cls & subscene_optimal_by_class[subscene_index][class_group][horizon][threshold]["ok"]
                    else:
                        ok_keys_cls = set(ok_candidates_cls)
                    subscene_sets_by_class[subscene_index][class_group][horizon][threshold]["ok"] = ok_keys_cls
                    subscene_sets_by_class[subscene_index][class_group][horizon][threshold]["total"] = set(ok_keys_cls) | set(ng_keys_cls)

    return scene_sets, subscene_sets, scene_sets_by_class, subscene_sets_by_class, True


def _build_validation_common_from_sample_sets(
    baseline_sets: dict,
    test_sets: dict,
) -> tuple[dict, dict, list[int]]:
    baseline_common = _default_side_summary("ok")
    test_common = _default_side_summary("ok")
    common_object_ids: set[int] = set()

    for horizon in VALIDATION_HORIZONS:
        for threshold in VALIDATION_THRESHOLDS:
            baseline_total = baseline_sets[horizon][threshold]["total"]
            test_total = test_sets[horizon][threshold]["total"]
            common_total = baseline_total & test_total
            baseline_ok = baseline_sets[horizon][threshold]["ok"] & common_total
            test_ok = test_sets[horizon][threshold]["ok"] & common_total

            total_count = len(common_total)
            baseline_ok_count = len(baseline_ok)
            test_ok_count = len(test_ok)

            b_node = baseline_common[horizon][threshold]
            t_node = test_common[horizon][threshold]
            b_node["ok"] = baseline_ok_count
            b_node["total"] = total_count
            b_node["rate"] = (100.0 * float(baseline_ok_count) / float(total_count)) if total_count > 0 else 0.0
            t_node["ok"] = test_ok_count
            t_node["total"] = total_count
            t_node["rate"] = (100.0 * float(test_ok_count) / float(total_count)) if total_count > 0 else 0.0

            common_object_ids |= {int(oid) for _stamp, oid in common_total}

    return baseline_common, test_common, sorted(common_object_ids)


def _evaluated_oid_filters_from_summary(raw_summary: dict | None) -> tuple[set[int] | None, dict[int, set[int]]]:
    scene_oids: set[int] | None = None
    subscene_oids: dict[int, set[int]] = {}
    if not isinstance(raw_summary, dict):
        return scene_oids, subscene_oids

    if "evaluated_object_ids" in raw_summary:
        scene_vals = raw_summary.get("evaluated_object_ids")
        scene_oids = _to_oid_set(scene_vals if isinstance(scene_vals, list) else [])
    elif isinstance(raw_summary.get("per_object_validation"), dict):
        scene_oids = _to_oid_set(list(raw_summary.get("per_object_validation", {}).keys()))

    subscenes = raw_summary.get("subscenes", [])
    if not isinstance(subscenes, list):
        return scene_oids, subscene_oids
    for idx, node in enumerate(subscenes):
        if not isinstance(node, dict):
            continue
        if "evaluated_object_ids" not in node:
            pov = node.get("per_object_validation")
            if not isinstance(pov, dict):
                continue
        raw_index = node.get("index", idx)
        try:
            sub_idx = int(idx if raw_index is None else raw_index)
        except Exception:
            sub_idx = idx
        vals = node.get("evaluated_object_ids")
        if isinstance(vals, list):
            subscene_oids[sub_idx] = _to_oid_set(vals)
        else:
            subscene_oids[sub_idx] = _to_oid_set(list(node.get("per_object_validation", {}).keys()))
    return scene_oids, subscene_oids


def _path_class_group_maps_from_summary(raw_summary: dict | None) -> tuple[dict[int, str], dict[int, dict[int, str]]]:
    scene_map: dict[int, str] = {}
    subscene_map: dict[int, dict[int, str]] = {}
    if not isinstance(raw_summary, dict):
        return scene_map, subscene_map
    raw_scene_map = raw_summary.get("evaluated_object_path_class_group", {})
    if isinstance(raw_scene_map, dict):
        for oid, class_group in raw_scene_map.items():
            try:
                oid_int = int(oid)
            except Exception:
                continue
            class_group_str = str(class_group or "")
            if class_group_str in PATH_CLASS_GROUPS:
                scene_map[oid_int] = class_group_str
    for idx, sub in enumerate(raw_summary.get("subscenes", []) or []):
        if not isinstance(sub, dict):
            continue
        try:
            sub_idx = int(sub.get("index", idx) or idx)
        except Exception:
            sub_idx = idx
        raw_sub_map = sub.get("evaluated_object_path_class_group", {})
        if not isinstance(raw_sub_map, dict):
            continue
        out_map: dict[int, str] = {}
        for oid, class_group in raw_sub_map.items():
            try:
                oid_int = int(oid)
            except Exception:
                continue
            class_group_str = str(class_group or "")
            if class_group_str in PATH_CLASS_GROUPS:
                out_map[oid_int] = class_group_str
        if out_map:
            subscene_map[sub_idx] = out_map
    return scene_map, subscene_map


def _build_validation_common_from_bags(
    baseline_validation_bag_path: Path,
    test_validation_bag_path: Path,
    scene_timing: dict | None,
    baseline_summary_raw: dict | None = None,
    test_summary_raw: dict | None = None,
    progress_label: str | None = None,
) -> tuple[dict, dict, list[int], dict[int, dict], dict, dict, bool]:
    baseline_scene_oids, baseline_subscene_oids = _evaluated_oid_filters_from_summary(baseline_summary_raw)
    test_scene_oids, test_subscene_oids = _evaluated_oid_filters_from_summary(test_summary_raw)
    baseline_scene_class_map, baseline_subscene_class_map = _path_class_group_maps_from_summary(baseline_summary_raw)
    test_scene_class_map, test_subscene_class_map = _path_class_group_maps_from_summary(test_summary_raw)

    (
        baseline_scene_sets,
        baseline_subscene_sets,
        baseline_scene_class_sets,
        baseline_subscene_class_sets,
        baseline_ok,
    ) = _extract_validation_sample_sets(
        baseline_validation_bag_path,
        side="baseline",
        scene_timing=scene_timing,
        evaluated_scene_oids=baseline_scene_oids,
        evaluated_subscene_oids=baseline_subscene_oids,
        scene_path_class_group_by_oid=baseline_scene_class_map,
        subscene_path_class_group_by_oid=baseline_subscene_class_map,
        progress_prefix=(f"[aggregate] {progress_label} baseline validation" if progress_label else None),
    )
    (
        test_scene_sets,
        test_subscene_sets,
        test_scene_class_sets,
        test_subscene_class_sets,
        test_ok,
    ) = _extract_validation_sample_sets(
        test_validation_bag_path,
        side="test",
        scene_timing=scene_timing,
        evaluated_scene_oids=test_scene_oids,
        evaluated_subscene_oids=test_subscene_oids,
        scene_path_class_group_by_oid=test_scene_class_map,
        subscene_path_class_group_by_oid=test_subscene_class_map,
        progress_prefix=(f"[aggregate] {progress_label} test validation" if progress_label else None),
    )
    if not baseline_ok or not test_ok:
        return (
            _default_side_summary("ok"),
            _default_side_summary("ok"),
            [],
            {},
            _default_path_class_group_validation("ok"),
            _default_path_class_group_validation("ok"),
            False,
        )

    scene_baseline_common, scene_test_common, scene_common_ids = _build_validation_common_from_sample_sets(
        baseline_scene_sets,
        test_scene_sets,
    )
    scene_baseline_common_by_class = _default_path_class_group_validation("ok")
    scene_test_common_by_class = _default_path_class_group_validation("ok")
    for class_group in PATH_CLASS_GROUPS:
        b_common, t_common, _common_ids = _build_validation_common_from_sample_sets(
            baseline_scene_class_sets.get(class_group, _new_validation_sample_sets()),
            test_scene_class_sets.get(class_group, _new_validation_sample_sets()),
        )
        scene_baseline_common_by_class[class_group] = b_common
        scene_test_common_by_class[class_group] = t_common

    subscene_common_map: dict[int, dict] = {}
    indices = set(baseline_subscene_sets.keys()) | set(test_subscene_sets.keys())
    if isinstance(scene_timing, dict):
        for idx, sub in enumerate(scene_timing.get("subscenes", []) or []):
            if not isinstance(sub, dict):
                continue
            try:
                sub_idx = int(sub.get("index", idx) or idx)
            except Exception:
                sub_idx = idx
            indices.add(sub_idx)
    for sub_idx in sorted(indices):
        b_sets = baseline_subscene_sets.get(sub_idx, _new_validation_sample_sets())
        t_sets = test_subscene_sets.get(sub_idx, _new_validation_sample_sets())
        b_common, t_common, common_ids = _build_validation_common_from_sample_sets(b_sets, t_sets)
        b_common_by_class = _default_path_class_group_validation("ok")
        t_common_by_class = _default_path_class_group_validation("ok")
        for class_group in PATH_CLASS_GROUPS:
            b_cls_sets = (baseline_subscene_class_sets.get(sub_idx, {}) or {}).get(class_group, _new_validation_sample_sets())
            t_cls_sets = (test_subscene_class_sets.get(sub_idx, {}) or {}).get(class_group, _new_validation_sample_sets())
            b_cls_common, t_cls_common, _common_ids = _build_validation_common_from_sample_sets(
                b_cls_sets,
                t_cls_sets,
            )
            b_common_by_class[class_group] = b_cls_common
            t_common_by_class[class_group] = t_cls_common
        subscene_common_map[sub_idx] = {
            "baseline_common": b_common,
            "test_common": t_common,
            "baseline_common_by_class_group": b_common_by_class,
            "test_common_by_class_group": t_common_by_class,
            "common_object_ids": common_ids,
        }

    return (
        scene_baseline_common,
        scene_test_common,
        scene_common_ids,
        subscene_common_map,
        scene_baseline_common_by_class,
        scene_test_common_by_class,
        True,
    )


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
            by_group = node.get("by_group", {})
            if isinstance(by_group, dict):
                for group in COLLISION_SOURCE_GROUPS:
                    gnode = by_group.get(group, {})
                    if not isinstance(gnode, dict):
                        continue
                    out_group = out[kind].setdefault("by_group", {}).setdefault(
                        group,
                        {"has_collision": False, "collision_paths": 0, "checked_paths": 0, "detail": "ok"},
                    )
                    out_group["collision_paths"] += int(gnode.get("collision_paths", 0) or 0)
                    out_group["checked_paths"] += int(gnode.get("checked_paths", 0) or 0)
    for kind in COLLISION_KINDS:
        out[kind]["has_collision"] = out[kind]["collision_paths"] > 0
        for group in COLLISION_SOURCE_GROUPS:
            gnode = out[kind].setdefault("by_group", {}).setdefault(
                group,
                {"has_collision": False, "collision_paths": 0, "checked_paths": 0, "detail": "ok"},
            )
            gnode["has_collision"] = bool(gnode.get("collision_paths", 0) > 0)
    return out


def _default_dt_summary(detail: str) -> dict:
    return {
        "dt_status": "valid",
        "dt_max": "-",
        "dt_mean": "-",
        "sample_count": 0,
        "detail": detail,
    }


def _normalize_dt_summary(node: dict | None, detail_fallback: str) -> dict:
    if not isinstance(node, dict):
        return _default_dt_summary(detail_fallback)
    status = str(node.get("dt_status") or "valid")
    dt_max_raw = node.get("dt_max", "-")
    dt_mean_raw = node.get("dt_mean", "-")
    dt_max = "-" if dt_max_raw in (None, "") else dt_max_raw
    dt_mean = "-" if dt_mean_raw in (None, "") else dt_mean_raw
    try:
        sample_count = max(0, int(node.get("sample_count", 0) or 0))
    except Exception:
        sample_count = 0
    return {
        "dt_status": status,
        "dt_max": dt_max,
        "dt_mean": dt_mean,
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
        sub_class_raw = node.get("path_class_group_validation", {})
        if isinstance(sub_class_raw, dict):
            sub["path_class_group_validation"] = _default_path_class_group_validation("ok")
            for class_group in PATH_CLASS_GROUPS:
                group_node = sub_class_raw.get(class_group, {})
                if not isinstance(group_node, dict):
                    group_node = {}
                for horizon in VALIDATION_HORIZONS:
                    hnode = group_node.get(horizon, group_node.get(horizon.replace("-", "_"), {}))
                    if not isinstance(hnode, dict):
                        hnode = {}
                    for threshold in VALIDATION_THRESHOLDS:
                        sub["path_class_group_validation"][class_group][horizon][threshold] = _normalize_threshold_summary(
                            hnode.get(threshold, {}),
                            detail_fallback=f"{side}_subscene_{meta['index']}_{class_group}_{horizon}_{threshold}_missing",
                        )
        else:
            sub["path_class_group_validation"] = _default_path_class_group_validation(
                f"{side}_subscene_{meta['index']}_path_class_group_missing"
            )
        sub["evaluated_object_ids"] = sorted(_to_oid_set(node.get("evaluated_object_ids", [])))
        class_map = node.get("evaluated_object_path_class_group", {})
        if isinstance(class_map, dict):
            sub["evaluated_object_path_class_group"] = {
                str(oid): str(class_group)
                for oid, class_group in class_map.items()
                if str(class_group) in PATH_CLASS_GROUPS
            }
        else:
            sub["evaluated_object_path_class_group"] = {}
        per_object_validation = node.get("per_object_validation", {})
        sub["per_object_validation"] = per_object_validation if isinstance(per_object_validation, dict) else {}
        per_object_collision = node.get("per_object_collision", {})
        sub["per_object_collision"] = per_object_collision if isinstance(per_object_collision, dict) else {}
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

    total_dirs = len(dirs)
    written = 0
    common_skipped: list[str] = []
    _print_progress_line("[aggregate] progress", 0, total_dirs)
    for idx, (rel, _dir_path) in enumerate(dirs, start=1):
        print(f"[aggregate] processing {rel} ({idx}/{total_dirs})", file=sys.stderr, flush=True)
        out_dir = test_results_root / rel
        out_dir.mkdir(parents=True, exist_ok=True)

        direct_path = out_dir / "comparison_direct.json"
        merged_path = out_dir / "comparison.json"
        baseline_summary_path = baseline_root / rel / "validation_baseline_summary.json"
        test_summary_path = out_dir / "validation_test_summary.json"
        baseline_validation_bag_path = baseline_root / rel / "validation_baseline.bag"
        test_validation_bag_path = out_dir / "validation_test.bag"
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
            "baseline_by_class_group": _load_side_path_class_group_validation(baseline_summary_path, "baseline"),
            "test_by_class_group": _load_side_path_class_group_validation(test_summary_path, "test"),
        }
        comp["collision"] = {
            "baseline": _load_side_collision(baseline_summary_path, "baseline"),
            "test": _load_side_collision(test_summary_path, "test"),
        }
        (
            scene_baseline_common,
            scene_test_common,
            scene_common_ids,
            subscene_common_map,
            scene_baseline_common_by_class,
            scene_test_common_by_class,
            sample_common_ok,
        ) = _build_validation_common_from_bags(
            baseline_validation_bag_path=baseline_validation_bag_path,
            test_validation_bag_path=test_validation_bag_path,
            scene_timing=scene_timing,
            baseline_summary_raw=baseline_summary_raw,
            test_summary_raw=test_summary_raw,
            progress_label=rel,
        )
        if not sample_common_ok:
            print(
                f"[aggregate] {rel}: validation common (ID+stamp) の算出に失敗しました。"
                "この scene の common は未算出としてスキップします。",
                file=sys.stderr,
            )
            comp["validation"]["baseline_common"] = _default_side_summary(f"{rel}_common_samples_missing")
            comp["validation"]["test_common"] = _default_side_summary(f"{rel}_common_samples_missing")
            comp["validation"]["baseline_common_by_class_group"] = _default_path_class_group_validation(
                f"{rel}_common_samples_missing"
            )
            comp["validation"]["test_common_by_class_group"] = _default_path_class_group_validation(
                f"{rel}_common_samples_missing"
            )
            comp["common_object_ids"] = []
            collision_common_ids = set()
            subscene_common_map = {}
            common_skipped.append(rel)
        else:
            comp["validation"]["baseline_common"] = scene_baseline_common
            comp["validation"]["test_common"] = scene_test_common
            comp["validation"]["baseline_common_by_class_group"] = scene_baseline_common_by_class
            comp["validation"]["test_common_by_class_group"] = scene_test_common_by_class
            comp["common_object_ids"] = scene_common_ids
            collision_common_ids = set(scene_common_ids)
        comp["collision"]["baseline_common"] = _aggregate_collision_common(baseline_summary_raw, collision_common_ids)
        comp["collision"]["test_common"] = _aggregate_collision_common(test_summary_raw, collision_common_ids)
        baseline_dt = _load_dt_summary(baseline_dt_summary_path, "baseline")
        test_dt = _load_dt_summary(test_dt_summary_path, "test")
        comp["dt_status_baseline"] = baseline_dt["dt_status"]
        comp["dt_max_baseline"] = baseline_dt["dt_max"]
        comp["dt_mean_baseline"] = baseline_dt["dt_mean"]
        comp["dt_sample_count_baseline"] = baseline_dt["sample_count"]
        comp["dt_status_test"] = test_dt["dt_status"]
        comp["dt_max_test"] = test_dt["dt_max"]
        comp["dt_mean_test"] = test_dt["dt_mean"]
        comp["dt_sample_count_test"] = test_dt["sample_count"]

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
        subscene_total = len(ordered_indices)
        subscene_progress_state = {"last_percent": -1, "last_len": 0}
        if subscene_total > 0:
            _print_live_progress(f"[aggregate] {rel} subscene merge", 0, subscene_total, subscene_progress_state)
        for sub_idx, index in enumerate(ordered_indices, start=1):
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
                        "traffic": {"base": False},
                    }
                    sub["diff_counts_by_source"] = {
                        "lane": {k: 0 for k in ("along", "opposite", "crossing")},
                        "vsl": {k: 0 for k in ("along", "opposite", "crossing")},
                        "object_ids": {k: 0 for k in ("along", "opposite", "crossing", "other", "base")},
                        "path": {k: 0 for k in ("along", "opposite", "crossing", "other", "base")},
                        "traffic": {"base": 0},
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
            sub["validation"]["baseline_by_class_group"] = (
                baseline_subscene_map.get(index, {}).get("path_class_group_validation")
                if isinstance(baseline_subscene_map.get(index, {}), dict)
                else None
            ) or _default_path_class_group_validation(f"baseline_subscene_{index}_path_class_group_missing")
            sub["validation"]["test_by_class_group"] = (
                test_subscene_map.get(index, {}).get("path_class_group_validation")
                if isinstance(test_subscene_map.get(index, {}), dict)
                else None
            ) or _default_path_class_group_validation(f"test_subscene_{index}_path_class_group_missing")
            baseline_sub = baseline_subscene_map.get(index, {})
            test_sub = test_subscene_map.get(index, {})
            if index in subscene_common_map:
                sub["validation"]["baseline_common"] = subscene_common_map[index]["baseline_common"]
                sub["validation"]["test_common"] = subscene_common_map[index]["test_common"]
                sub["validation"]["baseline_common_by_class_group"] = subscene_common_map[index].get(
                    "baseline_common_by_class_group",
                    _default_path_class_group_validation(f"baseline_subscene_{index}_common_path_class_group_missing"),
                )
                sub["validation"]["test_common_by_class_group"] = subscene_common_map[index].get(
                    "test_common_by_class_group",
                    _default_path_class_group_validation(f"test_subscene_{index}_common_path_class_group_missing"),
                )
                sub_common_ids = set(subscene_common_map[index]["common_object_ids"])
            else:
                sub["validation"]["baseline_common"] = _default_side_summary(
                    f"baseline_subscene_{index}_common_samples_missing"
                )
                sub["validation"]["test_common"] = _default_side_summary(
                    f"test_subscene_{index}_common_samples_missing"
                )
                sub["validation"]["baseline_common_by_class_group"] = _default_path_class_group_validation(
                    f"baseline_subscene_{index}_common_samples_missing"
                )
                sub["validation"]["test_common_by_class_group"] = _default_path_class_group_validation(
                    f"test_subscene_{index}_common_samples_missing"
                )
                sub_common_ids = set()
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
            sub["collision"]["baseline_common"] = _aggregate_collision_common(
                {"per_object_collision": baseline_sub.get("per_object_collision", {})},
                sub_common_ids,
            )
            sub["collision"]["test_common"] = _aggregate_collision_common(
                {"per_object_collision": test_sub.get("per_object_collision", {})},
                sub_common_ids,
            )
            sub["common_object_ids"] = sorted(sub_common_ids)
            baseline_dt_sub = baseline_dt_subscene_map.get(index, _default_dt_summary(f"baseline_subscene_{index}_dt_missing"))
            test_dt_sub = test_dt_subscene_map.get(index, _default_dt_summary(f"test_subscene_{index}_dt_missing"))
            sub["dt_status_baseline"] = baseline_dt_sub.get("dt_status", "valid")
            sub["dt_max_baseline"] = baseline_dt_sub.get("dt_max", "-")
            sub["dt_mean_baseline"] = baseline_dt_sub.get("dt_mean", "-")
            sub["dt_sample_count_baseline"] = baseline_dt_sub.get("sample_count", 0)
            sub["dt_status_test"] = test_dt_sub.get("dt_status", "valid")
            sub["dt_max_test"] = test_dt_sub.get("dt_max", "-")
            sub["dt_mean_test"] = test_dt_sub.get("dt_mean", "-")
            sub["dt_sample_count_test"] = test_dt_sub.get("sample_count", 0)
            comp_subscenes.append(sub)
            if subscene_total > 0:
                _print_live_progress(f"[aggregate] {rel} subscene merge", sub_idx, subscene_total, subscene_progress_state)
        comp["subscenes"] = comp_subscenes

        with open(merged_path, "w", encoding="utf-8") as f:
            json.dump(comp, f, indent=2, ensure_ascii=False)
        written += 1
        _print_progress_line("[aggregate] progress", idx, total_dirs)

    print(f"[aggregate] wrote {written} comparison.json file(s)")
    if common_skipped:
        print(
            f"[aggregate] common skipped {len(common_skipped)} scene(s): "
            + ", ".join(common_skipped),
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
