#!/usr/bin/env python3
"""Observed validation evaluator: result vs observed for baseline/test."""
from __future__ import annotations

import argparse
import copy
import json
import math
import sys
from bisect import bisect_left
from collections import defaultdict
from pathlib import Path

from common import VALIDATION_BASELINE_NS, VALIDATION_TEST_NS

try:
    import rosbag

    HAS_ROSBAG = True
except ImportError:
    HAS_ROSBAG = False

try:
    import rospy

    HAS_CLOCK_MSG = True
except ImportError:
    HAS_CLOCK_MSG = False

GROUP_TO_WM_SUFFIX = {
    "base": "/WM/tracked_object_set_with_prediction",
    "along": "/WM/along_object_set_with_prediction",
    "opposite": "/WM/oncoming_object_set_with_prediction",
    "crossing": "/WM/crossing_object_set_with_prediction",
    "other": "/WM/other_object_set_with_prediction",
}

VALIDATION_GROUPS = ("along", "opposite", "crossing", "other")
VALIDATION_THRESHOLDS = ("approximate", "strict")
VALIDATION_HORIZONS = ("half-time-relaxed", "time-relaxed")
VALIDATION_HORIZON_WINDOWS = {
    "half-time-relaxed": {"max_t": 5.0, "other_max_t": 1.5},
    "time-relaxed": {"max_t": 10.0, "other_max_t": 3.0},
}
CLASSIFICATION_UNCLASSIFIED = 0
CLASSIFICATION_PEDESTRIAN = 1
CLASSIFICATION_CYCLIST = 2
CLASSIFICATION_MOTORCYCLIST = 3
CLASSIFICATION_CAR = 4
CLASSIFICATION_TRUCK = 5
CLASSIFICATION_BUS = 6

CLASS_GROUP_VEHICLE = "vehicle"
CLASS_GROUP_BIKE = "bike"
CLASS_GROUP_PEDESTRIAN = "pedestrian"

CLASS_GROUP_BY_CLASSIFICATION = {
    CLASSIFICATION_CAR: CLASS_GROUP_VEHICLE,
    CLASSIFICATION_TRUCK: CLASS_GROUP_VEHICLE,
    CLASSIFICATION_BUS: CLASS_GROUP_VEHICLE,
    CLASSIFICATION_CYCLIST: CLASS_GROUP_BIKE,
    CLASSIFICATION_MOTORCYCLIST: CLASS_GROUP_BIKE,
    CLASSIFICATION_PEDESTRIAN: CLASS_GROUP_PEDESTRIAN,
}

CLASS_GROUP_TOLERANCES = {
    CLASS_GROUP_VEHICLE: {
        "strict": {
            "longitudinal": 2.0,
            "lateral_near": 0.5,
            "lateral_far": 1.2,
        },
        "time-relaxed_start": {
            "longitudinal": 1.0,
            "lateral_near": 0.5,
            "lateral_far": 1.2,
        },
        "time-relaxed_end": {
            "longitudinal": 7.5,
            "lateral_near": 2.0,
            "lateral_far": 4.8,
        },
    },
    CLASS_GROUP_BIKE: {
        "strict": {
            "longitudinal": 1.5,
            "lateral_near": 0.35,
            "lateral_far": 0.84,
        },
        "time-relaxed_start": {
            "longitudinal": 0.5,
            "lateral_near": 0.35,
            "lateral_far": 0.84,
        },
        "time-relaxed_end": {
            "longitudinal": 4.0,
            "lateral_near": 1.5,
            "lateral_far": 3.6,
        },
    },
    CLASS_GROUP_PEDESTRIAN: {
        "strict": {
            "longitudinal": 0.7,
            "lateral_near": 0.3,
            "lateral_far": 0.6,
        },
        "time-relaxed_start": {
            "longitudinal": 0.5,
            "lateral_near": 0.3,
            "lateral_far": 0.6,
        },
        "time-relaxed_end": {
            "longitudinal": 0.8,
            "lateral_near": 0.5,
            "lateral_far": 1.0,
        },
    },
}
VELOCITY_HEADING_FALLBACK_THRESHOLD_MPS = 3.6 / 3.6
VALIDATION_STATUSES = ("optimal", "ignore", "fail", "observed_ok", "observed_ng")
OPTIMAL_SELECTION_GROUP_PRIORITY = {
    "along": 0,
    "opposite": 1,
    "crossing": 2,
    "other": 3,
}
VALIDATION_NS_ROOT = "/validation/eval"
VSL_STOPLINE_ID = 500001
VSL_CROSSING_MIN = 500100
MIN_OBSERVED_EVAL_DURATION_SEC = 3.0
APPROXIMATE_REQUIRED_STRICT_RATIO = 0.7
APPROXIMATE_TOLERANCE_SCALE = 1.5


def _print_progress_line(prefix: str, done: int, total: int, state: dict) -> None:
    if total <= 0:
        return
    percent = int((done * 100) / total)
    last_percent = state.get("last_percent", -1)
    if percent == last_percent and done < total:
        return
    state["last_percent"] = percent
    width = 30
    fill = int((done * width) / total)
    bar = "#" * fill + "-" * (width - fill)
    print(
        f"\r{prefix} [{bar}] {percent}% ({done}/{total})",
        end="",
        file=sys.stderr,
        flush=True,
    )
    if done >= total:
        print(file=sys.stderr, flush=True)


def _default_threshold_summary(detail: str = "ok") -> dict:
    return {
        "ok": 0,
        "total": 0,
        "rate": 0.0,
        "detail": detail,
    }


def default_side_summary(detail: str = "not_evaluated") -> dict:
    return {
        horizon: {threshold: _default_threshold_summary(detail) for threshold in VALIDATION_THRESHOLDS}
        for horizon in VALIDATION_HORIZONS
    }


def _validation_log_key(side: str, horizon: str, threshold: str) -> str:
    return f"{side}-{horizon}-{threshold}"


def _to_nsec(ts) -> int:
    if ts is None:
        return 0
    if hasattr(ts, "to_nsec"):
        return int(ts.to_nsec())
    sec = getattr(ts, "secs", getattr(ts, "sec", 0)) or 0
    nsec = getattr(ts, "nsecs", getattr(ts, "nsec", getattr(ts, "nanosec", 0))) or 0
    return int(sec) * 10**9 + int(nsec)


def _msg_stamp(msg, bag_t) -> int:
    if msg is None:
        return _to_nsec(bag_t)
    h = getattr(msg, "header", None)
    if h is not None:
        s = getattr(h, "stamp", None)
        if s is not None:
            return _to_nsec(s)
    return _to_nsec(bag_t)


def _bag_has_namespaced(bag_path: Path, ns: str) -> bool:
    if not HAS_ROSBAG or not bag_path.exists():
        return False
    try:
        with rosbag.Bag(str(bag_path), "r") as bag:
            info = bag.get_type_and_topic_info()
            if isinstance(info, tuple) and len(info) >= 2:
                topic_infos = info[1]
                topics = topic_infos.keys() if hasattr(topic_infos, "keys") else topic_infos
            else:
                topics = getattr(info, "topics", {})
                topics = topics.keys() if hasattr(topics, "keys") else topics
            for topic in topics:
                if topic.startswith(ns):
                    return True
    except Exception:
        return False
    return False


def _resolve_group_topics(bag_path: Path, ns: str | None = None) -> dict[str, str]:
    use_ns = bool(ns) and _bag_has_namespaced(bag_path, str(ns))
    out = {}
    for group, suffix in GROUP_TO_WM_SUFFIX.items():
        out[group] = f"{ns}{suffix}" if use_ns else suffix
    return out


def _collect_group_messages_by_stamp(bag_path: Path, group_topics: dict[str, str]) -> dict[str, dict[int, tuple]]:
    out = {g: {} for g in group_topics}
    if not HAS_ROSBAG or not bag_path.exists():
        return out
    topics = list(group_topics.values())
    with rosbag.Bag(str(bag_path), "r") as bag:
        for topic, msg, t in bag.read_messages(topics=topics):
            stamp = _msg_stamp(msg, t)
            for group, topic_name in group_topics.items():
                if topic == topic_name:
                    out[group][stamp] = (t, msg)
                    break
    return out


def _set_msg_stamp_ns(msg, stamp_nsec: int) -> None:
    header = getattr(msg, "header", None)
    if header is None:
        return
    stamp = getattr(header, "stamp", None)
    if stamp is None:
        return
    sec = int(stamp_nsec // 10**9)
    nsec = int(stamp_nsec % 10**9)
    if HAS_CLOCK_MSG:
        try:
            header.stamp = rospy.Time(sec, nsec)
            return
        except Exception:
            pass
    if hasattr(stamp, "secs"):
        stamp.secs = sec
    elif hasattr(stamp, "sec"):
        stamp.sec = sec
    if hasattr(stamp, "nsecs"):
        stamp.nsecs = nsec
    elif hasattr(stamp, "nsec"):
        stamp.nsec = nsec
    elif hasattr(stamp, "nanosec"):
        stamp.nanosec = nsec


def _make_empty_object_set_like(template_msg, stamp_nsec: int):
    if template_msg is None:
        return None
    out = copy.deepcopy(template_msg)
    _set_msg_stamp_ns(out, stamp_nsec)
    out.objects = []
    return out


def _obj_id_of(obj) -> int:
    base_obj = getattr(obj, "object", obj)
    try:
        return int(getattr(base_obj, "object_id", -1))
    except Exception:
        return -1


def _obj_classification_of(obj) -> int:
    base_obj = getattr(obj, "object", obj)
    try:
        return int(getattr(base_obj, "classification", CLASSIFICATION_UNCLASSIFIED))
    except Exception:
        return CLASSIFICATION_UNCLASSIFIED


def _class_group_for_classification(classification: int) -> str:
    return CLASS_GROUP_BY_CLASSIFICATION.get(int(classification), CLASS_GROUP_VEHICLE)


def _has_predicted_path(traj_obj) -> bool:
    for path_msg in getattr(traj_obj, "trajectory_set", []) or []:
        if getattr(path_msg, "trajectory", []) or []:
            return True
    return False


def _is_vsl_id(object_id: int) -> bool:
    return object_id == VSL_STOPLINE_ID or object_id >= VSL_CROSSING_MIN


def _normalize_angle(angle: float) -> float:
    while angle > math.pi:
        angle -= 2.0 * math.pi
    while angle < -math.pi:
        angle += 2.0 * math.pi
    return angle


def _sorted_traj_points(path_msg, max_t: float | None = None) -> list:
    pts = []
    for pt in getattr(path_msg, "trajectory", []) or []:
        t = float(getattr(pt, "t", 0.0) or 0.0)
        if max_t is not None and t > max_t + 1e-6:
            continue
        x = float(getattr(pt, "x", 0.0) or 0.0)
        y = float(getattr(pt, "y", 0.0) or 0.0)
        yaw = float(getattr(pt, "yaw", 0.0) or 0.0)
        pts.append((t, x, y, yaw))
    pts.sort(key=lambda v: v[0])
    return pts


def _has_min_observed_duration(traj_obj, min_duration_sec: float = MIN_OBSERVED_EVAL_DURATION_SEC) -> bool:
    traj_set = list(getattr(traj_obj, "trajectory_set", []) or [])
    if not traj_set:
        return False
    pts = _sorted_traj_points(traj_set[0], max_t=None)
    if not pts:
        return False
    end_t = float(pts[-1][0])
    return end_t >= (float(min_duration_sec) - 1e-6)


def _trajectory_within_threshold(
    observed_path,
    predicted_path,
    horizon: str,
    threshold: str,
    horizon_max_t: float,
    classification: int,
    max_t: float | None = None,
) -> tuple[bool, float]:
    obs_pts = _sorted_traj_points(observed_path, max_t=max_t)
    pred_pts = _sorted_traj_points(predicted_path, max_t=max_t)
    if not obs_pts or not pred_pts:
        return (False, float("inf"))

    pred_times = [p[0] for p in pred_pts]
    cost = 0.0
    used = 0
    strict_ok_steps = 0
    max_dt = 0.26
    for i, (obs_t, obs_x, obs_y, obs_yaw) in enumerate(obs_pts):
        idx = bisect_left(pred_times, obs_t)
        candidates = []
        if idx < len(pred_pts):
            candidates.append(pred_pts[idx])
        if idx - 1 >= 0:
            candidates.append(pred_pts[idx - 1])
        if not candidates:
            return (False, float("inf"))
        pred_t, pred_x, pred_y, pred_yaw = min(candidates, key=lambda p: abs(p[0] - obs_t))
        if abs(pred_t - obs_t) > max_dt:
            return (False, float("inf"))

        dx = pred_x - obs_x
        dy = pred_y - obs_y
        heading = _observed_heading_from_velocity(obs_pts, i)
        c = math.cos(heading)
        s = math.sin(heading)
        longitudinal = c * dx + s * dy
        lateral = -s * dx + c * dy
        longitudinal_tol, lateral_near, lateral_far = _threshold_tolerances(
            horizon,
            threshold,
            obs_t,
            horizon_max_t,
            classification,
        )
        lateral_tol = _trapezoid_lateral_tolerance(abs(longitudinal), longitudinal_tol, lateral_near, lateral_far)
        strict_ok = abs(longitudinal) <= longitudinal_tol and abs(lateral) <= lateral_tol
        if threshold == "strict":
            if not strict_ok:
                return (False, float("inf"))
        elif threshold == "approximate":
            expanded_longitudinal_tol = max(1e-6, longitudinal_tol * APPROXIMATE_TOLERANCE_SCALE)
            expanded_lateral_near = max(1e-6, lateral_near * APPROXIMATE_TOLERANCE_SCALE)
            expanded_lateral_far = max(1e-6, lateral_far * APPROXIMATE_TOLERANCE_SCALE)
            expanded_lateral_tol = _trapezoid_lateral_tolerance(
                abs(longitudinal),
                expanded_longitudinal_tol,
                expanded_lateral_near,
                expanded_lateral_far,
            )
            if abs(longitudinal) > expanded_longitudinal_tol or abs(lateral) > expanded_lateral_tol:
                return (False, float("inf"))
            if strict_ok:
                strict_ok_steps += 1
        else:
            raise ValueError(f"unsupported validation threshold: {threshold}")

        yaw_diff = _normalize_angle(pred_yaw - obs_yaw)
        cost += (
            (longitudinal / longitudinal_tol) ** 2
            + (lateral / lateral_tol) ** 2
            + 0.05 * (yaw_diff ** 2)
        )
        used += 1

    if used <= 0:
        return (False, float("inf"))
    if threshold == "approximate":
        strict_ratio = float(strict_ok_steps) / float(used)
        if strict_ratio + 1e-9 < APPROXIMATE_REQUIRED_STRICT_RATIO:
            return (False, float("inf"))
    return (True, cost)


def _threshold_tolerances(
    horizon: str,
    threshold: str,
    step_t: float,
    horizon_max_t: float,
    classification: int,
) -> tuple[float, float, float]:
    del threshold
    class_group = _class_group_for_classification(classification)
    class_tolerances = CLASS_GROUP_TOLERANCES[class_group]
    if horizon not in ("time-relaxed", "half-time-relaxed"):
        raise ValueError(f"unsupported validation horizon: {horizon}")
    denom = max(1e-6, float(horizon_max_t))
    ratio = min(1.0, max(0.0, float(step_t) / denom))
    start = class_tolerances["time-relaxed_start"]
    end = class_tolerances["time-relaxed_end"]
    longitudinal = (
        start["longitudinal"]
        + (end["longitudinal"] - start["longitudinal"]) * ratio
    )
    lateral_near = (
        start["lateral_near"]
        + (end["lateral_near"] - start["lateral_near"]) * ratio
    )
    lateral_far = (
        start["lateral_far"]
        + (end["lateral_far"] - start["lateral_far"]) * ratio
    )
    return (
        max(1e-6, float(longitudinal)),
        max(1e-6, float(lateral_near)),
        max(1e-6, float(lateral_far)),
    )


def _trapezoid_lateral_tolerance(abs_longitudinal: float, longitudinal_tol: float, lateral_near: float, lateral_far: float) -> float:
    ratio = min(1.0, max(0.0, float(abs_longitudinal) / max(1e-6, float(longitudinal_tol))))
    return max(1e-6, float(lateral_near) + (float(lateral_far) - float(lateral_near)) * ratio)


def _observed_heading_from_velocity(obs_pts: list[tuple[float, float, float, float]], idx: int) -> float:
    _, _, _, yaw = obs_pts[idx]
    n = len(obs_pts)
    if n <= 1:
        return yaw

    def _calc(i0: int, i1: int):
        if not (0 <= i0 < n and 0 <= i1 < n) or i0 == i1:
            return None
        t0, x0, y0, _ = obs_pts[i0]
        t1, x1, y1, _ = obs_pts[i1]
        dt = float(t1) - float(t0)
        if dt <= 1e-6:
            return None
        dx = float(x1) - float(x0)
        dy = float(y1) - float(y0)
        speed = math.hypot(dx, dy) / dt
        return dx, dy, speed

    candidates = []
    if 0 < idx < (n - 1):
        c = _calc(idx - 1, idx + 1)
        if c is not None:
            candidates.append(c)
    c = _calc(idx, idx + 1)
    if c is not None:
        candidates.append(c)
    c = _calc(idx - 1, idx)
    if c is not None:
        candidates.append(c)
    if not candidates:
        return yaw

    dx, dy, speed = candidates[0]
    if speed <= VELOCITY_HEADING_FALLBACK_THRESHOLD_MPS:
        return yaw
    if math.hypot(dx, dy) <= 1e-9:
        return yaw
    return math.atan2(dy, dx)


def _filter_obj_paths_by_indices(obj, indices: list[int]):
    out = copy.deepcopy(obj)
    kept = []
    traj_set = list(getattr(obj, "trajectory_set", []) or [])
    for idx in sorted(set(indices)):
        if 0 <= idx < len(traj_set):
            kept.append(copy.deepcopy(traj_set[idx]))
    out.trajectory_set = kept
    return out


def _make_validation_topic(side: str, horizon: str, threshold: str, group: str, status: str) -> str:
    horizon_topic = horizon.replace("-", "_")
    threshold_topic = threshold.replace("-", "_")
    return f"{VALIDATION_NS_ROOT}/{side}/{horizon_topic}/{threshold_topic}/{group}/{status}{GROUP_TO_WM_SUFFIX['base']}"


def _build_validation_for_side_threshold(
    rel: str,
    out_dir: Path,
    side: str,
    horizon: str,
    threshold: str,
    max_t: float,
    other_max_t: float,
    result_bag: Path,
    observed_bag: Path,
    result_ns: str,
    observed_ns: str,
) -> dict:
    summary = _default_threshold_summary("ok")
    log_key = _validation_log_key(side, horizon, threshold)
    out_records: dict[str, dict[str, list[tuple]]] = {
        group: {status: [] for status in VALIDATION_STATUSES}
        for group in VALIDATION_GROUPS
    }

    if not HAS_ROSBAG:
        summary["detail"] = "rosbag not available"
    elif not result_bag.exists():
        summary["detail"] = f"result bag missing: {result_bag}"
    elif not observed_bag.exists():
        summary["detail"] = f"observed bag missing: {observed_bag}"
    else:
        result_topics = _resolve_group_topics(result_bag, result_ns)
        observed_topics = _resolve_group_topics(observed_bag, observed_ns)
        result_by_group = _collect_group_messages_by_stamp(result_bag, result_topics)
        observed_by_group = _collect_group_messages_by_stamp(observed_bag, observed_topics)

        result_base_stamps = set(result_by_group.get("base", {}).keys())
        observed_base_stamps = set(observed_by_group.get("base", {}).keys())
        eval_stamp_set = result_base_stamps & observed_base_stamps
        master_stamps = sorted(result_base_stamps) if result_base_stamps else sorted(eval_stamp_set)

        progress_state = {"last_percent": -1}
        if master_stamps:
            _print_progress_line(
                f"[validation] {rel} {log_key}",
                0,
                len(master_stamps),
                progress_state,
            )

        def _first_template(group: str):
            by_group_o = observed_by_group.get(group, {})
            if by_group_o:
                return next(iter(by_group_o.values()))[1]
            by_group_r = result_by_group.get(group, {})
            if by_group_r:
                return next(iter(by_group_r.values()))[1]
            return None

        group_templates = {g: _first_template(g) for g in VALIDATION_GROUPS}
        base_template = _first_template("base")

        for idx, stamp in enumerate(master_stamps, start=1):
            ref_t = None
            ref_msg = None
            obs_base_rec = observed_by_group.get("base", {}).get(stamp)
            res_base_rec = result_by_group.get("base", {}).get(stamp)
            if res_base_rec is not None:
                ref_t, ref_msg = res_base_rec
            elif obs_base_rec is not None:
                ref_t, ref_msg = obs_base_rec
            else:
                _print_progress_line(
                    f"[validation] {rel} {log_key}",
                    idx,
                    len(master_stamps),
                    progress_state,
                )
                continue

            observed_objs_by_group: dict[str, dict[int, object]] = {}
            predicted_objs_by_group: dict[str, dict[int, object]] = {}
            excluded_short_observed_ids: set[int] = set()
            for group in VALIDATION_GROUPS:
                observed_objs_by_group[group] = {}
                observed_rec = observed_by_group.get(group, {}).get(stamp)
                if observed_rec is not None:
                    _t_obs, msg_obs = observed_rec
                    for obj in getattr(msg_obs, "objects", []) or []:
                        oid = _obj_id_of(obj)
                        if oid < 0:
                            continue
                        if _is_vsl_id(oid):
                            continue
                        if _has_predicted_path(obj):
                            observed_objs_by_group[group][oid] = obj
                            if not _has_min_observed_duration(obj):
                                excluded_short_observed_ids.add(oid)

                predicted_objs_by_group[group] = {}
                pred_rec = result_by_group.get(group, {}).get(stamp)
                if pred_rec is not None:
                    _t_pred, msg_pred = pred_rec
                    for obj in getattr(msg_pred, "objects", []) or []:
                        oid = _obj_id_of(obj)
                        if oid < 0:
                            continue
                        if _is_vsl_id(oid):
                            continue
                        if _has_predicted_path(obj):
                            predicted_objs_by_group[group][oid] = obj

            # "other" validation target is limited to objects that are actually prediction targets
            # in result other_object_set (i.e. objects with at least one predicted path).
            if "other" in observed_objs_by_group:
                other_pred_target_ids = set(predicted_objs_by_group.get("other", {}).keys())
                if other_pred_target_ids:
                    observed_objs_by_group["other"] = {
                        oid: obj
                        for oid, obj in observed_objs_by_group["other"].items()
                        if oid in other_pred_target_ids
                    }
                else:
                    observed_objs_by_group["other"] = {}
                excluded_short_observed_ids = {
                    oid for oid in excluded_short_observed_ids if oid in observed_objs_by_group["other"] or any(
                        oid in observed_objs_by_group[g] for g in VALIDATION_GROUPS if g != "other"
                    )
                }

            status_pred_indices: dict[str, dict[str, dict[int, list[int]]]] = {
                "optimal": {g: defaultdict(list) for g in VALIDATION_GROUPS},
                "ignore": {g: defaultdict(list) for g in VALIDATION_GROUPS},
                "fail": {g: defaultdict(list) for g in VALIDATION_GROUPS},
            }
            status_observed_objs: dict[str, dict[str, dict[int, object]]] = {
                "observed_ok": {g: {} for g in VALIDATION_GROUPS},
                "observed_ng": {g: {} for g in VALIDATION_GROUPS},
            }

            candidates_by_oid: dict[int, list[tuple[str, int, float]]] = {}
            for group in VALIDATION_GROUPS:
                group_max_t = other_max_t if group == "other" else max_t
                obs_map = observed_objs_by_group[group]
                pred_map = predicted_objs_by_group[group]
                for oid, obs_obj in obs_map.items():
                    if oid in excluded_short_observed_ids:
                        continue
                    obs_paths = list(getattr(obs_obj, "trajectory_set", []) or [])
                    if not obs_paths:
                        continue
                    classification = _obj_classification_of(obs_obj)
                    pred_obj = pred_map.get(oid)
                    if pred_obj is None:
                        continue
                    best = None
                    for path_idx, pred_path in enumerate(getattr(pred_obj, "trajectory_set", []) or []):
                        matched, cost = _trajectory_within_threshold(
                            obs_paths[0],
                            pred_path,
                            horizon=horizon,
                            threshold=threshold,
                            horizon_max_t=group_max_t,
                            classification=classification,
                            max_t=group_max_t,
                        )
                        if matched and (best is None or cost < best[1]):
                            best = (path_idx, cost)
                    if best is not None:
                        candidates_by_oid.setdefault(oid, []).append((group, best[0], best[1]))

            observed_ids = set()
            for group in VALIDATION_GROUPS:
                observed_ids |= set(observed_objs_by_group[group].keys())

            if stamp in eval_stamp_set:
                for oid in sorted(observed_ids):
                    if oid in excluded_short_observed_ids:
                        # Out-of-scope for metrics (<3s observed), but keep in bags as observed_ok/ignore.
                        for group in VALIDATION_GROUPS:
                            obs_obj = observed_objs_by_group[group].get(oid)
                            if obs_obj is not None:
                                status_observed_objs["observed_ok"][group][oid] = copy.deepcopy(obs_obj)
                        for group in VALIDATION_GROUPS:
                            pred_obj = predicted_objs_by_group[group].get(oid)
                            if pred_obj is None:
                                continue
                            n_paths = len(getattr(pred_obj, "trajectory_set", []) or [])
                            for pidx in range(n_paths):
                                status_pred_indices["ignore"][group][oid].append(pidx)
                        continue
                    summary["total"] += 1
                    candidates = candidates_by_oid.get(oid, [])
                    if candidates:
                        summary["ok"] += 1
                        opt_group, opt_idx, _opt_cost = min(
                            candidates,
                            key=lambda v: (
                                OPTIMAL_SELECTION_GROUP_PRIORITY.get(v[0], len(OPTIMAL_SELECTION_GROUP_PRIORITY)),
                                v[2],
                            ),
                        )
                        for group in VALIDATION_GROUPS:
                            obs_obj = observed_objs_by_group[group].get(oid)
                            if obs_obj is not None:
                                status_observed_objs["observed_ok"][group][oid] = copy.deepcopy(obs_obj)
                        for group in VALIDATION_GROUPS:
                            pred_obj = predicted_objs_by_group[group].get(oid)
                            if pred_obj is None:
                                continue
                            n_paths = len(getattr(pred_obj, "trajectory_set", []) or [])
                            for pidx in range(n_paths):
                                if group == opt_group and pidx == opt_idx:
                                    status_pred_indices["optimal"][group][oid].append(pidx)
                                else:
                                    status_pred_indices["ignore"][group][oid].append(pidx)
                    else:
                        for group in VALIDATION_GROUPS:
                            obs_obj = observed_objs_by_group[group].get(oid)
                            if obs_obj is not None:
                                status_observed_objs["observed_ng"][group][oid] = copy.deepcopy(obs_obj)
                        for group in VALIDATION_GROUPS:
                            pred_obj = predicted_objs_by_group[group].get(oid)
                            if pred_obj is None:
                                continue
                            n_paths = len(getattr(pred_obj, "trajectory_set", []) or [])
                            for pidx in range(n_paths):
                                status_pred_indices["fail"][group][oid].append(pidx)

            for group in VALIDATION_GROUPS:
                group_template_msg = group_templates.get(group) or ref_msg or base_template
                for status in VALIDATION_STATUSES:
                    msg_out = _make_empty_object_set_like(group_template_msg, stamp)
                    if msg_out is None:
                        continue
                    if status in ("optimal", "ignore", "fail"):
                        idx_map = status_pred_indices[status][group]
                        for oid, idx_list in idx_map.items():
                            src_obj = predicted_objs_by_group[group].get(oid)
                            if src_obj is None:
                                continue
                            filtered = _filter_obj_paths_by_indices(src_obj, idx_list)
                            if getattr(filtered, "trajectory_set", []) or []:
                                msg_out.objects.append(filtered)
                    elif status in ("observed_ok", "observed_ng"):
                        for _oid, obs_obj in status_observed_objs[status][group].items():
                            msg_out.objects.append(copy.deepcopy(obs_obj))
                    out_records[group][status].append((ref_t, msg_out))

            _print_progress_line(
                f"[validation] {rel} {log_key}",
                idx,
                len(master_stamps),
                progress_state,
            )

    if summary["total"] > 0:
        summary["rate"] = (100.0 * float(summary["ok"])) / float(summary["total"])
    else:
        summary["rate"] = 0.0

    total_write_steps = sum(len(out_records[g][s]) for g in VALIDATION_GROUPS for s in VALIDATION_STATUSES)
    write_state = {"last_percent": -1}
    if total_write_steps > 0:
        _print_progress_line(
            f"[validation] {rel} {log_key} write",
            0,
            total_write_steps,
            write_state,
        )
    written = 0
    for group in VALIDATION_GROUPS:
        for status in VALIDATION_STATUSES:
            bag_path = out_dir / f"{horizon}_{threshold}_{group}_{side}_{status}.bag"
            topic = _make_validation_topic(side, horizon, threshold, group, status)
            with rosbag.Bag(str(bag_path), "w", chunk_threshold=64 * 1024) as out_bag:
                for t, msg in out_records[group][status]:
                    out_bag.write(topic, msg, t)
                    written += 1
                    if total_write_steps > 0:
                        _print_progress_line(
                            f"[validation] {rel} {log_key} write",
                            written,
                            total_write_steps,
                            write_state,
                        )
    if total_write_steps == 0:
        for group in VALIDATION_GROUPS:
            for status in VALIDATION_STATUSES:
                bag_path = out_dir / f"{horizon}_{threshold}_{group}_{side}_{status}.bag"
                with rosbag.Bag(str(bag_path), "w", chunk_threshold=64 * 1024):
                    pass
    return summary


def evaluate_validation_for_side(
    rel: str,
    out_dir: Path,
    side: str,
    result_bag: Path,
    observed_bag: Path,
    result_ns: str,
    observed_ns: str,
) -> dict:
    summary = {}
    for horizon in VALIDATION_HORIZONS:
        summary[horizon] = {}
        window_cfg = VALIDATION_HORIZON_WINDOWS.get(horizon, VALIDATION_HORIZON_WINDOWS["half-time-relaxed"])
        max_t = float(window_cfg.get("max_t", 5.0))
        other_max_t = float(window_cfg.get("other_max_t", max_t))
        for threshold in VALIDATION_THRESHOLDS:
            side_summary = _build_validation_for_side_threshold(
                rel=rel,
                out_dir=out_dir,
                side=side,
                horizon=horizon,
                threshold=threshold,
                max_t=max_t,
                other_max_t=other_max_t,
                result_bag=result_bag,
                observed_bag=observed_bag,
                result_ns=result_ns,
                observed_ns=observed_ns,
            )
            summary[horizon][threshold] = side_summary
            log_key = _validation_log_key(side, horizon, threshold)
            print(
                f"[validation] {rel} {log_key}: "
                f"{side_summary['rate']:.1f}% ({side_summary['ok']}/{side_summary['total']})"
            )
    return summary


def _default_result_ns_for_side(side: str) -> str:
    return VALIDATION_BASELINE_NS if side == "baseline" else VALIDATION_TEST_NS


def _default_observed_ns_for_side(side: str) -> str:
    return "/validation/observed_baseline" if side == "baseline" else "/validation/observed_test"


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate observed validation for one side and one scene")
    parser.add_argument("--rel", default="", help="Scene relative path for logging")
    parser.add_argument("--side", required=True, choices=("baseline", "test"))
    parser.add_argument("--result-bag", required=True)
    parser.add_argument("--observed-bag", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--summary-file", default="")
    parser.add_argument("--result-ns", default="")
    parser.add_argument("--observed-ns", default="")
    args = parser.parse_args()

    result_bag = Path(args.result_bag)
    observed_bag = Path(args.observed_bag)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    result_ns = args.result_ns or _default_result_ns_for_side(args.side)
    observed_ns = args.observed_ns or _default_observed_ns_for_side(args.side)

    summary = evaluate_validation_for_side(
        rel=args.rel or out_dir.name,
        out_dir=out_dir,
        side=args.side,
        result_bag=result_bag,
        observed_bag=observed_bag,
        result_ns=result_ns,
        observed_ns=observed_ns,
    )

    summary_file = args.summary_file.strip()
    if summary_file:
        out_path = Path(summary_file)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    return 0


if __name__ == "__main__":
    sys.exit(main())
