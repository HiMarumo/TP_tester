#!/usr/bin/env python3
"""Observed validation evaluator: result vs observed for baseline/test."""
from __future__ import annotations

import argparse
import copy
import gc
import json
import math
import sys
from bisect import bisect_left, bisect_right
from collections import defaultdict
from pathlib import Path

from common import (
    VALIDATION_BASELINE_NS,
    VALIDATION_TEST_NS,
    find_subscene_index,
    is_stamp_in_evaluation_range,
)
from native_eval import (
    NATIVE_EVAL_AVAILABLE,
    native_collision_results_from_cache,
    native_validation_match_from_cache,
)

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
        "time-relaxed_start": {
            "longitudinal": 1.0,
            "lateral": 0.5,
        },
        "time-relaxed_end": {
            "longitudinal": 7.5,
            "lateral": 2.0,
        },
    },
    CLASS_GROUP_BIKE: {
        "time-relaxed_start": {
            "longitudinal": 0.5,
            "lateral": 0.35,
        },
        "time-relaxed_end": {
            "longitudinal": 4.0,
            "lateral": 1.5,
        },
    },
    CLASS_GROUP_PEDESTRIAN: {
        "time-relaxed_start": {
            "longitudinal": 0.5,
            "lateral": 0.3,
        },
        "time-relaxed_end": {
            "longitudinal": 0.8,
            "lateral": 0.5,
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
COLLISION_NS_ROOT = "/validation/collision"
COLLISION_KINDS = ("hard", "soft")
COLLISION_GROUPED_STATUSES = ("pred_collision", "pred_safe", "ego_pred_collision", "ego_pred_safe")
COLLISION_BASE_STATUSES = ("ego_observed_collision", "ego_observed_safe")
COLLISION_ALL_STATUSES = COLLISION_GROUPED_STATUSES + COLLISION_BASE_STATUSES
EVAL_STAMP_CHUNK_SIZE = 300
COLLISION_KIND_MULTIPLIERS = {
    "hard": {
        "size_scale": 1.0,
        "major_scale": 1.0,
    },
    "soft": {
        "size_scale": 1.1,
        "major_scale": 2.0,
    },
}
COLLISION_POINT_MATCH_MAX_DT = 0.26
VSL_STOPLINE_ID = 500001
VSL_CROSSING_MIN = 500100
EGO_OBJECT_IDS = (0, 500000)
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
    line = f"{prefix} [{bar}] {percent}% ({done}/{total})"
    last_len = int(getattr(_print_progress_line, "_last_line_len", 0) or 0)
    pad = " " * max(0, last_len - len(line))
    setattr(_print_progress_line, "_last_line_len", len(line))
    print(
        f"\r{line}{pad}",
        end="",
        file=sys.stderr,
        flush=True,
    )
    if done >= total:
        setattr(_print_progress_line, "_last_line_len", 0)
        print(file=sys.stderr, flush=True)


def _default_threshold_summary(detail: str = "ok") -> dict:
    return {
        "ok": 0,
        "total": 0,
        "rate": 0.0,
        "detail": detail,
    }


def _default_collision_kind_summary(detail: str = "ok") -> dict:
    return {
        "has_collision": False,
        "collision_paths": 0,
        "checked_paths": 0,
        "detail": detail,
    }


def _default_collision_summary(detail: str = "ok") -> dict:
    return {kind: _default_collision_kind_summary(detail) for kind in COLLISION_KINDS}


def default_side_summary(detail: str = "not_evaluated") -> dict:
    out = {
        horizon: {threshold: _default_threshold_summary(detail) for threshold in VALIDATION_THRESHOLDS}
        for horizon in VALIDATION_HORIZONS
    }
    out["collision"] = _default_collision_summary(detail)
    return out


def _build_subscene_side_summaries(scene_timing: dict | None, detail: str = "not_evaluated") -> list[dict]:
    if not isinstance(scene_timing, dict):
        return []
    subscenes = scene_timing.get("subscenes", [])
    if not isinstance(subscenes, list):
        return []
    out = []
    for base in subscenes:
        if not isinstance(base, dict):
            continue
        node = {
            "index": int(base.get("index", len(out)) or len(out)),
            "label": str(base.get("label", "")),
            "start_offset_sec": float(base.get("start_offset_sec", 0.0) or 0.0),
            "end_offset_sec": float(base.get("end_offset_sec", 0.0) or 0.0),
            "duration_sec": float(base.get("duration_sec", 0.0) or 0.0),
        }
        for horizon in VALIDATION_HORIZONS:
            node[horizon] = {
                threshold: _default_threshold_summary(detail)
                for threshold in VALIDATION_THRESHOLDS
            }
        node["collision"] = _default_collision_summary(detail)
        out.append(node)
    return out


def _subscene_summary_for_stamp(
    stamp_ns: int,
    scene_timing: dict | None,
    subscene_summaries: list[dict],
) -> dict | None:
    subscene_index = find_subscene_index(stamp_ns, scene_timing)
    if subscene_index is None:
        return None
    if not (0 <= subscene_index < len(subscene_summaries)):
        return None
    return subscene_summaries[subscene_index]


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
    cache_key = int(id(template_msg))
    template_cache = getattr(_make_empty_object_set_like, "_template_cache", None)
    if template_cache is None:
        template_cache = {}
        setattr(_make_empty_object_set_like, "_template_cache", template_cache)
    empty_template = template_cache.get(cache_key)
    if empty_template is None:
        empty_template = copy.deepcopy(template_msg)
        empty_template.objects = []
        template_cache[cache_key] = empty_template
    out = copy.deepcopy(empty_template)
    _set_msg_stamp_ns(out, stamp_nsec)
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
    return object_id in (1, VSL_STOPLINE_ID) or object_id >= VSL_CROSSING_MIN


def _is_ego_id(object_id: int) -> bool:
    return int(object_id) in EGO_OBJECT_IDS


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


def _build_path_cache(path_msg) -> dict:
    steps = []
    for pt in getattr(path_msg, "trajectory", []) or []:
        steps.append(
            {
                "t": float(getattr(pt, "t", 0.0) or 0.0),
                "x": float(getattr(pt, "x", 0.0) or 0.0),
                "y": float(getattr(pt, "y", 0.0) or 0.0),
                "yaw": float(getattr(pt, "yaw", 0.0) or 0.0),
                "speed_hint": float(getattr(pt, "velocity", 0.0) or 0.0) if hasattr(pt, "velocity") else 0.0,
            }
        )
    steps.sort(key=lambda v: v["t"])
    times = [float(step["t"]) for step in steps]
    traj_points = [(step["t"], step["x"], step["y"], step["yaw"]) for step in steps]
    collision_points = [
        {
            "t": step["t"],
            "x": step["x"],
            "y": step["y"],
            "yaw": step["yaw"],
            "speed_hint": abs(float(step["speed_hint"])),
        }
        for step in steps
    ]
    collision_curve = _build_collision_curve(collision_points)
    heading_speed = [_path_heading_speed(collision_points, i) for i in range(len(collision_points))]
    return {
        "path_msg": path_msg,
        "steps": steps,
        "times": times,
        "traj_points": traj_points,
        "collision_points": collision_points,
        "collision_curve": collision_curve,
        "heading_speed": heading_speed,
        "traj_slice_cache": {},
        "traj_curve_cache": {},
        "collision_index_cache": {},
        "collider_cache": {"hard": {}, "soft": {}},
    }


def _path_cache_max_index(cache: dict, max_t: float | None, key: str = "traj_slice_cache") -> int:
    times = cache.get("times", [])
    if max_t is None:
        return len(times)
    cache_map = cache.setdefault(key, {})
    cache_key = None if max_t is None else round(float(max_t), 6)
    if cache_key in cache_map:
        return cache_map[cache_key]
    idx = bisect_right(times, float(max_t) + 1e-6)
    cache_map[cache_key] = idx
    return idx


def _path_cache_traj_points(cache: dict, max_t: float | None = None) -> list[tuple[float, float, float, float]]:
    if max_t is None:
        return cache.get("traj_points", [])
    idx = _path_cache_max_index(cache, max_t)
    return cache.get("traj_points", [])[:idx]


def _path_cache_observed_curve(cache: dict, max_t: float | None = None) -> dict:
    curve_cache = cache.setdefault("traj_curve_cache", {})
    cache_key = None if max_t is None else round(float(max_t), 6)
    if cache_key in curve_cache:
        return curve_cache[cache_key]
    curve = _build_observed_curve(_path_cache_traj_points(cache, max_t))
    curve_cache[cache_key] = curve
    return curve


def _path_cache_collision_points(cache: dict, max_t: float | None = None) -> list[dict]:
    if max_t is None:
        return cache.get("collision_points", [])
    idx = _path_cache_max_index(cache, max_t, key="collision_index_cache")
    return cache.get("collision_points", [])[:idx]


def _build_observed_obj_cache(obj) -> dict:
    traj_set = list(getattr(obj, "trajectory_set", []) or [])
    path_cache = _build_path_cache(traj_set[0]) if traj_set else None
    end_t = 0.0
    if path_cache and path_cache.get("traj_points"):
        end_t = float(path_cache["traj_points"][-1][0])
    return {
        "obj": obj,
        "classification": _obj_classification_of(obj),
        "path_cache": path_cache,
        "has_min_duration": end_t >= (float(MIN_OBSERVED_EVAL_DURATION_SEC) - 1e-6),
        "path_msg": traj_set[0] if traj_set else None,
    }


def _build_predicted_obj_cache(obj) -> dict:
    traj_set = list(getattr(obj, "trajectory_set", []) or [])
    return {
        "obj": obj,
        "dims": _obj_dimensions_of(obj),
        "path_caches": [_build_path_cache(path_msg) for path_msg in traj_set],
    }


def _build_observed_curve(obs_pts: list[tuple[float, float, float, float]]) -> dict:
    s_vals: list[float] = []
    segments: list[dict] = []
    seg_s1: list[float] = []
    if not obs_pts:
        return {"points": [], "s": s_vals, "segments": segments, "seg_s1": seg_s1}
    s_vals = [0.0] * len(obs_pts)
    accum = 0.0
    for i in range(1, len(obs_pts)):
        _, x0, y0, _ = obs_pts[i - 1]
        _, x1, y1, _ = obs_pts[i]
        dx = float(x1) - float(x0)
        dy = float(y1) - float(y0)
        seg_len = math.hypot(dx, dy)
        accum += seg_len
        s_vals[i] = accum
        if seg_len > 1e-9:
            ux = dx / seg_len
            uy = dy / seg_len
            seg = {
                "s0": s_vals[i - 1],
                "s1": s_vals[i],
                "x0": float(x0),
                "y0": float(y0),
                "x1": float(x1),
                "y1": float(y1),
                "ux": ux,
                "uy": uy,
            }
            segments.append(seg)
            seg_s1.append(seg["s1"])
    return {"points": obs_pts, "s": s_vals, "segments": segments, "seg_s1": seg_s1}


def _project_xy_to_observed_sd(
    curve: dict,
    x: float,
    y: float,
    anchor_idx: int | None = None,
    fallback_heading: float = 0.0,
) -> tuple[float, float]:
    points = curve.get("points", [])
    s_vals = curve.get("s", [])
    segments = curve.get("segments", [])
    if not points:
        return (0.0, 0.0)

    px = float(x)
    py = float(y)

    if not segments:
        use_idx = 0 if anchor_idx is None else min(max(0, int(anchor_idx)), len(points) - 1)
        _, ox, oy, yaw = points[use_idx]
        heading = float(fallback_heading) if math.isfinite(float(fallback_heading)) else float(yaw)
        tx = math.cos(heading)
        ty = math.sin(heading)
        vx = px - float(ox)
        vy = py - float(oy)
        s_proj = (float(s_vals[use_idx]) if use_idx < len(s_vals) else 0.0) + (tx * vx + ty * vy)
        d_proj = -ty * vx + tx * vy
        return (s_proj, d_proj)

    best_dist2 = float("inf")
    best_s = 0.0
    best_d = 0.0

    def _consider_candidate(base_x: float, base_y: float, tx: float, ty: float, along: float, s_origin: float):
        nonlocal best_dist2, best_s, best_d
        qx = base_x + tx * along
        qy = base_y + ty * along
        dx = px - qx
        dy = py - qy
        dist2 = dx * dx + dy * dy
        if dist2 >= best_dist2:
            return
        best_dist2 = dist2
        best_s = float(s_origin) + float(along)
        best_d = tx * dy - ty * dx

    for seg in segments:
        seg_len = max(0.0, float(seg["s1"]) - float(seg["s0"]))
        vx = px - float(seg["x0"])
        vy = py - float(seg["y0"])
        along = vx * float(seg["ux"]) + vy * float(seg["uy"])
        along = min(seg_len, max(0.0, along))
        _consider_candidate(
            float(seg["x0"]),
            float(seg["y0"]),
            float(seg["ux"]),
            float(seg["uy"]),
            along,
            float(seg["s0"]),
        )

    first = segments[0]
    vx0 = px - float(first["x0"])
    vy0 = py - float(first["y0"])
    along0 = vx0 * float(first["ux"]) + vy0 * float(first["uy"])
    if along0 < 0.0:
        _consider_candidate(
            float(first["x0"]),
            float(first["y0"]),
            float(first["ux"]),
            float(first["uy"]),
            along0,
            float(first["s0"]),
        )

    last = segments[-1]
    vx1 = px - float(last["x1"])
    vy1 = py - float(last["y1"])
    along1 = vx1 * float(last["ux"]) + vy1 * float(last["uy"])
    if along1 > 0.0:
        _consider_candidate(
            float(last["x1"]),
            float(last["y1"]),
            float(last["ux"]),
            float(last["uy"]),
            along1,
            float(last["s1"]),
        )

    return (best_s, best_d)


def _trajectory_error_sequence(
    observed_path_cache: dict | None,
    predicted_path_cache: dict | None,
    max_t: float | None = None,
) -> list[dict] | None:
    if not observed_path_cache or not predicted_path_cache:
        return None
    obs_pts = _path_cache_traj_points(observed_path_cache, max_t=max_t)
    pred_pts = _path_cache_traj_points(predicted_path_cache, max_t=max_t)
    pred_times = predicted_path_cache.get("times", [])
    if max_t is not None:
        pred_times = pred_times[: len(pred_pts)]
    if not obs_pts or not pred_pts or not pred_times:
        return None

    obs_curve = _path_cache_observed_curve(observed_path_cache, max_t=max_t)
    errors = []
    max_dt = 0.26
    for i, (obs_t, _obs_x, _obs_y, obs_yaw) in enumerate(obs_pts):
        idx = bisect_left(pred_times, obs_t)
        candidates = []
        if idx < len(pred_pts):
            candidates.append(pred_pts[idx])
        if idx - 1 >= 0:
            candidates.append(pred_pts[idx - 1])
        if not candidates:
            return None
        pred_t, pred_x, pred_y, pred_yaw = min(candidates, key=lambda p: abs(p[0] - obs_t))
        if abs(pred_t - obs_t) > max_dt:
            return None

        fallback_heading = _observed_heading_from_velocity(obs_pts, i)
        s_pred, d_pred = _project_xy_to_observed_sd(
            obs_curve,
            float(pred_x),
            float(pred_y),
            anchor_idx=i,
            fallback_heading=fallback_heading,
        )
        s_obs = float(obs_curve["s"][i]) if i < len(obs_curve.get("s", [])) else 0.0
        errors.append(
            {
                "t": float(obs_t),
                "longitudinal": float(s_pred - s_obs),
                "lateral": float(d_pred),
                "yaw_diff": _normalize_angle(float(pred_yaw) - float(obs_yaw)),
            }
        )
    return errors


def _evaluate_error_sequence(
    errors: list[dict] | None,
    horizon: str,
    threshold: str,
    horizon_max_t: float,
    classification: int,
) -> tuple[bool, float]:
    if not errors:
        return (False, float("inf"))
    cost = 0.0
    strict_ok_steps = 0
    for err in errors:
        longitudinal_tol, lateral_tol = _threshold_tolerances(
            horizon,
            threshold,
            float(err["t"]),
            horizon_max_t,
            classification,
        )
        longitudinal = float(err["longitudinal"])
        lateral = float(err["lateral"])
        strict_ok = abs(longitudinal) <= longitudinal_tol and abs(lateral) <= lateral_tol
        if threshold == "strict":
            if not strict_ok:
                return (False, float("inf"))
        elif threshold == "approximate":
            expanded_longitudinal_tol = max(1e-6, longitudinal_tol * APPROXIMATE_TOLERANCE_SCALE)
            expanded_lateral_tol = max(1e-6, lateral_tol * APPROXIMATE_TOLERANCE_SCALE)
            if abs(longitudinal) > expanded_longitudinal_tol or abs(lateral) > expanded_lateral_tol:
                return (False, float("inf"))
            if strict_ok:
                strict_ok_steps += 1
        else:
            raise ValueError(f"unsupported validation threshold: {threshold}")

        cost += (
            (longitudinal / longitudinal_tol) ** 2
            + (lateral / lateral_tol) ** 2
            + 0.05 * (float(err["yaw_diff"]) ** 2)
        )

    if threshold == "approximate":
        strict_ratio = float(strict_ok_steps) / float(len(errors))
        if strict_ratio + 1e-9 < APPROXIMATE_REQUIRED_STRICT_RATIO:
            return (False, float("inf"))
    return (True, cost)


def _trajectory_within_threshold(
    observed_path,
    predicted_path,
    horizon: str,
    threshold: str,
    horizon_max_t: float,
    classification: int,
    max_t: float | None = None,
) -> tuple[bool, float]:
    observed_cache = _build_path_cache(observed_path)
    predicted_cache = _build_path_cache(predicted_path)
    native_result = native_validation_match_from_cache(
        observed_cache,
        predicted_cache,
        horizon=horizon,
        threshold=threshold,
        horizon_max_t=horizon_max_t,
        classification=classification,
        max_t=max_t,
    )
    if native_result is not None:
        return native_result
    errors = _trajectory_error_sequence(observed_cache, predicted_cache, max_t=max_t)
    return _evaluate_error_sequence(
        errors,
        horizon=horizon,
        threshold=threshold,
        horizon_max_t=horizon_max_t,
        classification=classification,
    )


def _threshold_tolerances(
    horizon: str,
    threshold: str,
    step_t: float,
    horizon_max_t: float,
    classification: int,
) -> tuple[float, float]:
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
    lateral = (
        start["lateral"]
        + (end["lateral"] - start["lateral"]) * ratio
    )
    return (
        max(1e-6, float(longitudinal)),
        max(1e-6, float(lateral)),
    )


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


def _obj_dimensions_of(obj) -> tuple[float, float]:
    base_obj = getattr(obj, "object", obj)
    dims = getattr(base_obj, "dimensions", None)
    length = float(getattr(dims, "x", 0.0) or 0.0) if dims is not None else 0.0
    width = float(getattr(dims, "y", 0.0) or 0.0) if dims is not None else 0.0
    if length <= 1e-3:
        length = float(getattr(base_obj, "length", 0.0) or 0.0)
    if width <= 1e-3:
        width = float(getattr(base_obj, "width", 0.0) or 0.0)
    if length <= 1e-3:
        length = 4.0
    if width <= 1e-3:
        width = 1.8
    return (max(0.1, length), max(0.1, width))


def _build_scene_message_cache(
    result_bag: Path,
    observed_bag: Path,
    result_ns: str,
    observed_ns: str,
) -> dict:
    result_topics = _resolve_group_topics(result_bag, result_ns)
    observed_topics = _resolve_group_topics(observed_bag, observed_ns)
    result_by_group = _collect_group_messages_by_stamp(result_bag, result_topics)
    observed_by_group = _collect_group_messages_by_stamp(observed_bag, observed_topics)

    result_base_stamps = set(result_by_group.get("base", {}).keys())
    observed_base_stamps = set(observed_by_group.get("base", {}).keys())
    eval_stamp_set = result_base_stamps & observed_base_stamps
    master_stamps = sorted(result_base_stamps) if result_base_stamps else sorted(eval_stamp_set)

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

    return {
        "result_by_group": result_by_group,
        "observed_by_group": observed_by_group,
        "result_base_stamps": result_base_stamps,
        "observed_base_stamps": observed_base_stamps,
        "eval_stamp_set": eval_stamp_set,
        "master_stamps": master_stamps,
        "group_templates": group_templates,
        "base_template": base_template,
    }


def _build_scene_stamp_contexts(message_cache: dict, master_stamps: list[int]) -> dict[int, dict]:
    result_by_group = message_cache["result_by_group"]
    observed_by_group = message_cache["observed_by_group"]
    group_templates = message_cache["group_templates"]
    base_template = message_cache["base_template"]

    stamp_contexts: dict[int, dict] = {}
    for stamp in master_stamps:
        ref_t = None
        ref_msg = None
        obs_base_rec = observed_by_group.get("base", {}).get(stamp)
        res_base_rec = result_by_group.get("base", {}).get(stamp)
        if res_base_rec is not None:
            ref_t, ref_msg = res_base_rec
        elif obs_base_rec is not None:
            ref_t, ref_msg = obs_base_rec
        else:
            continue

        observed_objs_by_group: dict[str, dict[int, dict]] = {g: {} for g in VALIDATION_GROUPS}
        predicted_objs_by_group: dict[str, dict[int, dict]] = {g: {} for g in VALIDATION_GROUPS}
        excluded_short_observed_ids: set[int] = set()

        for group in VALIDATION_GROUPS:
            observed_rec = observed_by_group.get(group, {}).get(stamp)
            if observed_rec is not None:
                _t_obs, msg_obs = observed_rec
                for obj in getattr(msg_obs, "objects", []) or []:
                    oid = _obj_id_of(obj)
                    if oid < 0:
                        continue
                    if _has_predicted_path(obj):
                        obs_entry = _build_observed_obj_cache(obj)
                        observed_objs_by_group[group][oid] = obs_entry
                        if not obs_entry["has_min_duration"]:
                            excluded_short_observed_ids.add(oid)

            pred_rec = result_by_group.get(group, {}).get(stamp)
            if pred_rec is not None:
                _t_pred, msg_pred = pred_rec
                for obj in getattr(msg_pred, "objects", []) or []:
                    oid = _obj_id_of(obj)
                    if oid < 0:
                        continue
                    if _has_predicted_path(obj):
                        predicted_objs_by_group[group][oid] = _build_predicted_obj_cache(obj)

        other_pred_target_ids = set(predicted_objs_by_group.get("other", {}).keys())
        if other_pred_target_ids:
            observed_objs_by_group["other"] = {
                oid: entry
                for oid, entry in observed_objs_by_group["other"].items()
                if oid in other_pred_target_ids
            }
        else:
            observed_objs_by_group["other"] = {}
        excluded_short_observed_ids = {
            oid
            for oid in excluded_short_observed_ids
            if oid in observed_objs_by_group["other"]
            or any(oid in observed_objs_by_group[g] for g in VALIDATION_GROUPS if g != "other")
        }

        ego_observed_entry = None
        if obs_base_rec is not None:
            _t_obs_base, msg_obs_base = obs_base_rec
            for obj in getattr(msg_obs_base, "objects", []) or []:
                oid = _obj_id_of(obj)
                if oid >= 0 and _is_ego_id(oid) and _has_predicted_path(obj):
                    ego_observed_entry = _build_observed_obj_cache(obj)
                    break
        for group in ("along", "opposite", "crossing", "other"):
            if ego_observed_entry is not None:
                break
            for oid, entry in observed_objs_by_group[group].items():
                if _is_ego_id(oid):
                    ego_observed_entry = entry
                    break
            if ego_observed_entry is not None:
                break

        ego_pred_objs_by_group: dict[str, dict] = {}
        for group in VALIDATION_GROUPS:
            for oid, entry in predicted_objs_by_group[group].items():
                if _is_ego_id(oid):
                    ego_pred_objs_by_group[group] = entry
                    break

        stamp_contexts[stamp] = {
            "stamp": stamp,
            "ref_t": ref_t,
            "ref_msg": ref_msg,
            "observed_objs_by_group": observed_objs_by_group,
            "predicted_objs_by_group": predicted_objs_by_group,
            "excluded_short_observed_ids": excluded_short_observed_ids,
            "group_templates": group_templates,
            "base_template": base_template,
            "ego_observed_entry": ego_observed_entry,
            "ego_pred_objs_by_group": ego_pred_objs_by_group,
        }
    return stamp_contexts


def _build_scene_eval_chunk_cache(message_cache: dict, master_stamps: list[int]) -> dict:
    master_stamps = list(master_stamps)
    stamp_contexts = _build_scene_stamp_contexts(message_cache, master_stamps)
    active_stamps = set(stamp_contexts.keys())
    return {
        "result_by_group": message_cache["result_by_group"],
        "observed_by_group": message_cache["observed_by_group"],
        "result_base_stamps": message_cache["result_base_stamps"],
        "observed_base_stamps": message_cache["observed_base_stamps"],
        "eval_stamp_set": set(message_cache["eval_stamp_set"]) & active_stamps,
        "master_stamps": [stamp for stamp in master_stamps if stamp in active_stamps],
        "group_templates": message_cache["group_templates"],
        "base_template": message_cache["base_template"],
        "stamp_contexts": stamp_contexts,
    }


def _scene_ref_time_for_stamp(message_cache: dict, stamp: int):
    result_base = message_cache.get("result_by_group", {}).get("base", {})
    observed_base = message_cache.get("observed_by_group", {}).get("base", {})
    rec = result_base.get(stamp) or observed_base.get(stamp)
    if rec is None:
        return None
    return rec[0]


def _filter_scene_master_stamps_for_range(message_cache: dict, scene_timing: dict | None) -> list[int]:
    filtered = []
    for stamp in message_cache.get("master_stamps", []):
        ref_t = _scene_ref_time_for_stamp(message_cache, stamp)
        if ref_t is None:
            continue
        if is_stamp_in_evaluation_range(_to_nsec(ref_t), scene_timing):
            filtered.append(stamp)
    return filtered


def _iter_stamp_chunks(master_stamps: list[int], chunk_size: int = EVAL_STAMP_CHUNK_SIZE):
    if chunk_size <= 0:
        chunk_size = EVAL_STAMP_CHUNK_SIZE
    for idx in range(0, len(master_stamps), chunk_size):
        yield master_stamps[idx : idx + chunk_size]


def _build_scene_eval_cache(
    result_bag: Path,
    observed_bag: Path,
    result_ns: str,
    observed_ns: str,
) -> dict:
    message_cache = _build_scene_message_cache(
        result_bag=result_bag,
        observed_bag=observed_bag,
        result_ns=result_ns,
        observed_ns=observed_ns,
    )
    return _build_scene_eval_chunk_cache(message_cache, message_cache.get("master_stamps", []))


def _sorted_collision_points(path_msg, max_t: float | None = None) -> list[dict]:
    pts = []
    for pt in getattr(path_msg, "trajectory", []) or []:
        t = float(getattr(pt, "t", 0.0) or 0.0)
        if max_t is not None and t > max_t + 1e-6:
            continue
        x = float(getattr(pt, "x", 0.0) or 0.0)
        y = float(getattr(pt, "y", 0.0) or 0.0)
        yaw = float(getattr(pt, "yaw", 0.0) or 0.0)
        speed = float(getattr(pt, "velocity", 0.0) or 0.0) if hasattr(pt, "velocity") else 0.0
        pts.append({"t": t, "x": x, "y": y, "yaw": yaw, "speed_hint": abs(speed)})
    pts.sort(key=lambda v: v["t"])
    return pts


def _path_heading_speed(points: list[dict], idx: int) -> tuple[float, float]:
    p = points[idx]
    yaw = float(p.get("yaw", 0.0))
    speed_hint = float(p.get("speed_hint", 0.0) or 0.0)
    n = len(points)
    if n <= 1:
        return yaw, max(0.0, speed_hint)

    def _calc(i0: int, i1: int):
        if not (0 <= i0 < n and 0 <= i1 < n) or i0 == i1:
            return None
        p0 = points[i0]
        p1 = points[i1]
        dt = float(p1["t"]) - float(p0["t"])
        if dt <= 1e-6:
            return None
        dx = float(p1["x"]) - float(p0["x"])
        dy = float(p1["y"]) - float(p0["y"])
        speed = math.hypot(dx, dy) / dt
        return dx, dy, speed

    derived_speed = 0.0
    derived_heading = None
    for pair in ((idx - 1, idx + 1), (idx, idx + 1), (idx - 1, idx)):
        c = _calc(pair[0], pair[1])
        if c is None:
            continue
        dx, dy, speed = c
        if speed > derived_speed:
            derived_speed = speed
        if derived_heading is None and math.hypot(dx, dy) > 1e-9 and speed > 1e-6:
            derived_heading = math.atan2(dy, dx)

    step_speed = speed_hint if speed_hint > 1e-6 else derived_speed
    step_speed = max(0.0, step_speed)
    # At low speed, collider heading must use the path yaw at this step.
    if step_speed <= VELOCITY_HEADING_FALLBACK_THRESHOLD_MPS:
        return (yaw, step_speed)
    # At higher speed, use position-delta heading if available; otherwise yaw.
    if derived_heading is not None:
        return (derived_heading, step_speed)
    return (yaw, max(0.0, step_speed))


def _build_collision_curve(points: list[dict]) -> dict:
    s_vals: list[float] = []
    segments: list[dict] = []
    seg_s1: list[float] = []
    if not points:
        return {"points": [], "s": s_vals, "segments": segments, "seg_s1": seg_s1}
    s_vals = [0.0] * len(points)
    accum = 0.0
    for i in range(1, len(points)):
        x0 = float(points[i - 1]["x"])
        y0 = float(points[i - 1]["y"])
        x1 = float(points[i]["x"])
        y1 = float(points[i]["y"])
        dx = x1 - x0
        dy = y1 - y0
        seg_len = math.hypot(dx, dy)
        accum += seg_len
        s_vals[i] = accum
        if seg_len > 1e-9:
            ux = dx / seg_len
            uy = dy / seg_len
            seg = {
                "s0": s_vals[i - 1],
                "s1": s_vals[i],
                "x0": x0,
                "y0": y0,
                "x1": x1,
                "y1": y1,
                "ux": ux,
                "uy": uy,
            }
            segments.append(seg)
            seg_s1.append(seg["s1"])
    return {"points": points, "s": s_vals, "segments": segments, "seg_s1": seg_s1}


def _sample_collision_curve(
    curve: dict,
    target_s: float,
    anchor_idx: int,
    fallback_heading: float,
) -> tuple[float, float, float, float]:
    points = curve.get("points", [])
    s_vals = curve.get("s", [])
    segments = curve.get("segments", [])
    seg_s1 = curve.get("seg_s1", [])
    if not points:
        tx = math.cos(fallback_heading)
        ty = math.sin(fallback_heading)
        return (0.0, 0.0, tx, ty)

    if not segments:
        use_idx = min(max(0, int(anchor_idx)), len(points) - 1)
        anchor = points[use_idx]
        anchor_s = float(s_vals[use_idx]) if use_idx < len(s_vals) else 0.0
        tx = math.cos(fallback_heading)
        ty = math.sin(fallback_heading)
        delta = float(target_s) - anchor_s
        x = float(anchor["x"]) + tx * delta
        y = float(anchor["y"]) + ty * delta
        return (x, y, tx, ty)

    if target_s <= float(segments[0]["s0"]):
        seg = segments[0]
        delta = float(target_s) - float(seg["s0"])
        x = float(seg["x0"]) + float(seg["ux"]) * delta
        y = float(seg["y0"]) + float(seg["uy"]) * delta
        return (x, y, float(seg["ux"]), float(seg["uy"]))
    if target_s >= float(segments[-1]["s1"]):
        seg = segments[-1]
        delta = float(target_s) - float(seg["s1"])
        x = float(seg["x1"]) + float(seg["ux"]) * delta
        y = float(seg["y1"]) + float(seg["uy"]) * delta
        return (x, y, float(seg["ux"]), float(seg["uy"]))

    idx = bisect_left(seg_s1, float(target_s))
    if idx >= len(segments):
        idx = len(segments) - 1
    seg = segments[idx]
    delta = float(target_s) - float(seg["s0"])
    x = float(seg["x0"]) + float(seg["ux"]) * delta
    y = float(seg["y0"]) + float(seg["uy"]) * delta
    return (x, y, float(seg["ux"]), float(seg["uy"]))


def _nearest_point_by_time(points: list[dict], times: list[float], t: float, max_dt: float = COLLISION_POINT_MATCH_MAX_DT):
    if not points or not times:
        return None
    idx = bisect_left(times, t)
    candidates = []
    if idx < len(points):
        candidates.append((idx, points[idx]))
    if idx - 1 >= 0:
        candidates.append((idx - 1, points[idx - 1]))
    if not candidates:
        return None
    best_idx, best = min(candidates, key=lambda ip: abs(float(ip[1]["t"]) - float(t)))
    if abs(float(best["t"]) - float(t)) > max_dt:
        return None
    return (best_idx, best)


def _collider_polygon(
    curve: dict,
    base_idx: int,
    fallback_heading: float,
    speed: float,
    length: float,
    width: float,
    kind: str,
) -> list[tuple[float, float]]:
    cfg = COLLISION_KIND_MULTIPLIERS.get(kind, COLLISION_KIND_MULTIPLIERS["soft"])
    scale = float(cfg["size_scale"])
    major_scale = float(cfg["major_scale"])
    rect_l = max(0.1, float(length) * scale)
    rect_w = max(0.1, float(width) * scale)
    half_l = 0.5 * rect_l
    half_w = 0.5 * rect_w
    speed_step = max(0.0, float(speed))
    # At very low speed, do not append the forward half-ellipse component.
    # The half-ellipse major axis uses step speed with kind-specific scaling.
    major = 0.0
    if speed_step > VELOCITY_HEADING_FALLBACK_THRESHOLD_MPS:
        major = max(0.0, speed_step * major_scale)
    half_major = 0.5 * major

    local_pts: list[tuple[float, float]] = [
        (-half_l, -half_w),
        (-half_l, +half_w),
        (+half_l, +half_w),
    ]
    if half_major > 1e-6:
        steps = 10
        for i in range(steps + 1):
            ratio = float(i) / float(steps)
            theta = (math.pi * 0.5) - ratio * math.pi
            ex = +half_l + half_major * math.cos(theta)
            ey = half_w * math.sin(theta)
            local_pts.append((ex, ey))
    local_pts.append((+half_l, -half_w))

    out = []
    s_vals = curve.get("s", [])
    if s_vals and 0 <= int(base_idx) < len(s_vals):
        base_s = float(s_vals[int(base_idx)])
    else:
        base_s = 0.0
    for ds, d in local_pts:
        cx, cy, tx, ty = _sample_collision_curve(
            curve,
            base_s + float(ds),
            int(base_idx),
            float(fallback_heading),
        )
        nx = -ty
        ny = tx
        gx = cx + nx * float(d)
        gy = cy + ny * float(d)
        out.append((gx, gy))
    return out


def _collider_local_points(
    speed: float,
    length: float,
    width: float,
    kind: str,
) -> list[tuple[float, float]]:
    cfg = COLLISION_KIND_MULTIPLIERS.get(kind, COLLISION_KIND_MULTIPLIERS["soft"])
    scale = float(cfg["size_scale"])
    major_scale = float(cfg["major_scale"])
    rect_l = max(0.1, float(length) * scale)
    rect_w = max(0.1, float(width) * scale)
    half_l = 0.5 * rect_l
    half_w = 0.5 * rect_w
    speed_step = max(0.0, float(speed))
    major = 0.0
    if speed_step > VELOCITY_HEADING_FALLBACK_THRESHOLD_MPS:
        major = max(0.0, speed_step * major_scale)
    half_major = 0.5 * major

    local_pts: list[tuple[float, float]] = [
        (-half_l, -half_w),
        (-half_l, +half_w),
        (+half_l, +half_w),
    ]
    if half_major > 1e-6:
        steps = 10
        for i in range(steps + 1):
            ratio = float(i) / float(steps)
            theta = (math.pi * 0.5) - ratio * math.pi
            ex = +half_l + half_major * math.cos(theta)
            ey = half_w * math.sin(theta)
            local_pts.append((ex, ey))
    local_pts.append((+half_l, -half_w))
    return local_pts


def _transform_collider_local_points(
    curve: dict,
    base_idx: int,
    base_s: float,
    fallback_heading: float,
    local_pts: list[tuple[float, float]],
) -> list[tuple[float, float]]:
    out = []
    for ds, d in local_pts:
        cx, cy, tx, ty = _sample_collision_curve(
            curve,
            base_s + float(ds),
            int(base_idx),
            float(fallback_heading),
        )
        nx = -ty
        ny = tx
        gx = cx + nx * float(d)
        gy = cy + ny * float(d)
        out.append((gx, gy))
    return out


def _polygon_aabb(poly: list[tuple[float, float]]) -> tuple[float, float, float, float] | None:
    if not poly:
        return None
    xs = [float(p[0]) for p in poly]
    ys = [float(p[1]) for p in poly]
    return (min(xs), min(ys), max(xs), max(ys))


def _aabb_overlaps(a: tuple[float, float, float, float] | None, b: tuple[float, float, float, float] | None) -> bool:
    if a is None or b is None:
        return False
    return not (a[2] < b[0] - 1e-9 or b[2] < a[0] - 1e-9 or a[3] < b[1] - 1e-9 or b[3] < a[1] - 1e-9)


def _merge_aabbs(
    a: tuple[float, float, float, float] | None,
    b: tuple[float, float, float, float] | None,
) -> tuple[float, float, float, float] | None:
    if a is None:
        return b
    if b is None:
        return a
    return (
        min(float(a[0]), float(b[0])),
        min(float(a[1]), float(b[1])),
        max(float(a[2]), float(b[2])),
        max(float(a[3]), float(b[3])),
    )


def _collision_profile_key(length: float, width: float) -> tuple[float, float]:
    return (round(float(length), 6), round(float(width), 6))


def _ensure_collision_profile(cache: dict | None, dims: tuple[float, float]) -> dict | None:
    if not cache:
        return None
    length = float(dims[0]) if dims else 4.0
    width = float(dims[1]) if dims else 1.8
    key = _collision_profile_key(length, width)
    profile_cache = cache.setdefault("collision_profile_cache", {})
    cached = profile_cache.get(key)
    if cached is not None:
        return cached

    point_count = len(cache.get("collision_points", []))
    profile = {
        "dims": key,
        "hard": {"polys": [], "aabbs": [], "path_aabb": None},
        "soft": {"polys": [], "aabbs": [], "path_aabb": None},
    }
    collider_cache = cache.setdefault("collider_cache", {})
    curve = cache.get("collision_curve", {})
    s_vals = curve.get("s", [])
    heading_speed = cache.get("heading_speed", [])
    for idx in range(point_count):
        heading, speed = heading_speed[idx]
        base_s = float(s_vals[idx]) if s_vals and 0 <= idx < len(s_vals) else 0.0
        for kind in COLLISION_KINDS:
            kind_cache = collider_cache.setdefault(kind, {})
            local_pts = _collider_local_points(speed, length, width, kind)
            poly = _transform_collider_local_points(
                curve,
                idx,
                base_s,
                float(heading),
                local_pts,
            )
            aabb = _polygon_aabb(poly)
            kind_cache[idx] = (poly, aabb)
            profile[kind]["polys"].append(poly)
            profile[kind]["aabbs"].append(aabb)
            profile[kind]["path_aabb"] = _merge_aabbs(profile[kind]["path_aabb"], aabb)
    profile_cache[key] = profile
    return profile


def _project_polygon(poly: list[tuple[float, float]], axis_x: float, axis_y: float) -> tuple[float, float]:
    dots = [p[0] * axis_x + p[1] * axis_y for p in poly]
    return (min(dots), max(dots))


def _polygons_intersect(poly_a: list[tuple[float, float]], poly_b: list[tuple[float, float]]) -> bool:
    if len(poly_a) < 3 or len(poly_b) < 3:
        return False
    for poly in (poly_a, poly_b):
        n = len(poly)
        for i in range(n):
            x1, y1 = poly[i]
            x2, y2 = poly[(i + 1) % n]
            ex = x2 - x1
            ey = y2 - y1
            nx = -ey
            ny = ex
            norm = math.hypot(nx, ny)
            if norm <= 1e-9:
                continue
            nx /= norm
            ny /= norm
            a_min, a_max = _project_polygon(poly_a, nx, ny)
            b_min, b_max = _project_polygon(poly_b, nx, ny)
            if a_max < b_min - 1e-9 or b_max < a_min - 1e-9:
                return False
    return True


def _path_has_collision_with_ego(
    pred_path,
    pred_dims: tuple[float, float],
    ego_path,
    ego_dims: tuple[float, float],
    other_kind: str,
) -> bool:
    pred_cache = _build_path_cache(pred_path)
    ego_cache = _build_path_cache(ego_path)
    result = _path_collision_results(pred_cache, pred_dims, ego_cache, ego_dims)
    return bool(result.get(other_kind, False))


def _path_collision_results(
    pred_cache: dict | None,
    pred_dims: tuple[float, float],
    ego_cache: dict | None,
    ego_dims: tuple[float, float],
) -> dict[str, bool]:
    native_result = native_collision_results_from_cache(
        pred_cache,
        pred_dims,
        ego_cache,
        ego_dims,
    )
    if native_result is not None:
        return native_result
    out = {kind: False for kind in COLLISION_KINDS}
    if not pred_cache or not ego_cache:
        return out
    pred_pts = pred_cache.get("collision_points", [])
    ego_pts = ego_cache.get("collision_points", [])
    if not pred_pts or not ego_pts:
        return out
    pred_profile = _ensure_collision_profile(pred_cache, pred_dims)
    ego_profile = _ensure_collision_profile(ego_cache, ego_dims)
    if pred_profile is None or ego_profile is None:
        return out
    ego_times = [float(p["t"]) for p in ego_pts]
    ego_soft_polys = ego_profile["soft"]["polys"]
    ego_soft_aabbs = ego_profile["soft"]["aabbs"]
    ego_soft_path_aabb = ego_profile["soft"]["path_aabb"]
    pred_hard_polys = pred_profile["hard"]["polys"]
    pred_hard_aabbs = pred_profile["hard"]["aabbs"]
    pred_hard_path_aabb = pred_profile["hard"]["path_aabb"]
    pred_soft_polys = pred_profile["soft"]["polys"]
    pred_soft_aabbs = pred_profile["soft"]["aabbs"]
    pred_soft_path_aabb = pred_profile["soft"]["path_aabb"]
    need_hard = _aabb_overlaps(pred_hard_path_aabb, ego_soft_path_aabb)
    need_soft = _aabb_overlaps(pred_soft_path_aabb, ego_soft_path_aabb)
    if not need_hard and not need_soft:
        return out
    for i, pred in enumerate(pred_pts):
        ego_pair = _nearest_point_by_time(ego_pts, ego_times, float(pred["t"]))
        if ego_pair is None:
            continue
        ego_idx, _ = ego_pair
        ego_soft_poly = ego_soft_polys[ego_idx]
        ego_soft_aabb = ego_soft_aabbs[ego_idx]
        if need_hard and not out["hard"]:
            pred_hard_poly = pred_hard_polys[i]
            pred_hard_aabb = pred_hard_aabbs[i]
            if _aabb_overlaps(pred_hard_aabb, ego_soft_aabb) and _polygons_intersect(pred_hard_poly, ego_soft_poly):
                out["hard"] = True
        if need_soft and not out["soft"]:
            pred_soft_poly = pred_soft_polys[i]
            pred_soft_aabb = pred_soft_aabbs[i]
            if _aabb_overlaps(pred_soft_aabb, ego_soft_aabb) and _polygons_intersect(pred_soft_poly, ego_soft_poly):
                out["soft"] = True
        if (not need_hard or out["hard"]) and (not need_soft or out["soft"]):
            return out
    return out


def _filter_obj_paths_by_indices(obj, indices: list[int]):
    out = copy.deepcopy(obj)
    kept = []
    traj_set = list(getattr(obj, "trajectory_set", []) or [])
    for idx in sorted(set(indices)):
        if 0 <= idx < len(traj_set):
            kept.append(copy.deepcopy(traj_set[idx]))
    out.trajectory_set = kept
    return out


def _get_filtered_obj_for_write(entry: dict | None, indices: list[int] | tuple[int, ...]):
    if not entry:
        return None
    key = tuple(sorted({int(idx) for idx in (indices or [])}))
    if not key:
        return None
    cache = entry.setdefault("write_filtered_obj_cache", {})
    cached = cache.get(key)
    if cached is not None:
        return cached
    obj = entry.get("obj")
    if obj is None:
        return None
    template = entry.get("write_obj_no_traj_template")
    if template is None:
        template = copy.deepcopy(obj)
        template.trajectory_set = []
        entry["write_obj_no_traj_template"] = template
    traj_set = list(getattr(obj, "trajectory_set", []) or [])
    path_copy_cache = entry.setdefault("write_path_copy_cache", {})
    out = copy.deepcopy(template)
    kept = []
    for idx in key:
        if not (0 <= idx < len(traj_set)):
            continue
        path_copy = path_copy_cache.get(idx)
        if path_copy is None:
            path_copy = copy.deepcopy(traj_set[idx])
            path_copy_cache[idx] = path_copy
        kept.append(path_copy)
    out.trajectory_set = kept
    cache[key] = out
    return out if kept else None


def _get_observed_obj_for_write(entry: dict | None):
    if not entry:
        return None
    cached = entry.get("write_obj_copy")
    if cached is None and entry.get("obj") is not None:
        cached = copy.deepcopy(entry["obj"])
        entry["write_obj_copy"] = cached
    return cached


def _make_validation_topic(side: str, horizon: str, threshold: str, group: str, status: str) -> str:
    horizon_topic = horizon.replace("-", "_")
    threshold_topic = threshold.replace("-", "_")
    return f"{VALIDATION_NS_ROOT}/{side}/{horizon_topic}/{threshold_topic}/{group}/{status}{GROUP_TO_WM_SUFFIX['base']}"


def _make_collision_topic(side: str, kind: str, status: str, group: str | None = None) -> str:
    if status in COLLISION_GROUPED_STATUSES:
        suffix = GROUP_TO_WM_SUFFIX.get(group or "other", GROUP_TO_WM_SUFFIX["other"])
    else:
        suffix = GROUP_TO_WM_SUFFIX["base"]
    return f"{COLLISION_NS_ROOT}/{side}/{kind}/{status}{suffix}"


def _write_validation_payload(
    side: str,
    horizon: str,
    threshold: str,
    group: str,
    status: str,
    t,
    stamp: int,
    payload,
    context: dict,
    out_bag,
) -> None:
    topic = _make_validation_topic(side, horizon, threshold, group, status)
    group_template_msg = context["group_templates"].get(group) or context["ref_msg"] or context["base_template"]
    msg_out = _make_empty_object_set_like(group_template_msg, stamp)
    if msg_out is None:
        return
    if status in ("optimal", "ignore", "fail"):
        for oid, idx_list in (payload or {}).items():
            src_entry = context["predicted_objs_by_group"][group].get(int(oid))
            if src_entry is None:
                continue
            filtered = _get_filtered_obj_for_write(src_entry, idx_list)
            if getattr(filtered, "trajectory_set", []) or []:
                msg_out.objects.append(filtered)
    else:
        for oid in payload or ():
            obs_entry = context["observed_objs_by_group"][group].get(int(oid))
            if obs_entry is not None:
                observed_obj = _get_observed_obj_for_write(obs_entry)
                if observed_obj is not None:
                    msg_out.objects.append(observed_obj)
    out_bag.write(topic, msg_out, t)


def _empty_validation_status_maps() -> tuple[dict[str, dict[str, dict[int, list[int]]]], dict[str, dict[str, dict[int, int]]]]:
    status_pred_indices: dict[str, dict[str, dict[int, list[int]]]] = {
        "optimal": {g: defaultdict(list) for g in VALIDATION_GROUPS},
        "ignore": {g: defaultdict(list) for g in VALIDATION_GROUPS},
        "fail": {g: defaultdict(list) for g in VALIDATION_GROUPS},
    }
    status_observed_oids: dict[str, dict[str, dict[int, int]]] = {
        "observed_ok": {g: {} for g in VALIDATION_GROUPS},
        "observed_ng": {g: {} for g in VALIDATION_GROUPS},
    }
    return status_pred_indices, status_observed_oids


def _ensure_context_validation_plan_cache(context: dict) -> dict[tuple[str, str], dict]:
    cached = context.get("validation_plan_cache")
    if cached is not None:
        return cached

    observed_objs_by_group = context["observed_objs_by_group"]
    predicted_objs_by_group = context["predicted_objs_by_group"]
    excluded_short_observed_ids = context["excluded_short_observed_ids"]
    metrics = [(horizon, threshold) for horizon in VALIDATION_HORIZONS for threshold in VALIDATION_THRESHOLDS]
    candidates_by_metric: dict[tuple[str, str], dict[int, list[tuple[str, int, float]]]] = {
        metric: {} for metric in metrics
    }

    for group in VALIDATION_GROUPS:
        obs_map = observed_objs_by_group[group]
        pred_map = predicted_objs_by_group[group]
        group_max_t_by_horizon = {
            horizon: float(
                VALIDATION_HORIZON_WINDOWS.get(horizon, VALIDATION_HORIZON_WINDOWS["half-time-relaxed"]).get(
                    "other_max_t" if group == "other" else "max_t",
                    1.5 if group == "other" else 5.0,
                )
            )
            for horizon in VALIDATION_HORIZONS
        }
        for oid, obs_entry in obs_map.items():
            if _is_vsl_id(oid) or oid in excluded_short_observed_ids:
                continue
            obs_path_cache = obs_entry.get("path_cache")
            if obs_path_cache is None:
                continue
            classification = int(obs_entry.get("classification", CLASSIFICATION_UNCLASSIFIED))
            pred_entry = pred_map.get(oid)
            if pred_entry is None:
                continue
            best_by_metric = {metric: None for metric in metrics}
            for path_idx, pred_path_cache in enumerate(pred_entry.get("path_caches", [])):
                error_cache = pred_path_cache.setdefault("validation_error_cache", {})
                metric_cache = pred_path_cache.setdefault("validation_metric_cache", {})
                for horizon in VALIDATION_HORIZONS:
                    group_max_t = group_max_t_by_horizon[horizon]
                    for threshold in VALIDATION_THRESHOLDS:
                        metric = (horizon, threshold)
                        metric_key = (
                            id(obs_path_cache),
                            round(float(group_max_t), 6),
                            horizon,
                            threshold,
                            int(classification),
                        )
                        if metric_key not in metric_cache:
                            native_metric = native_validation_match_from_cache(
                                obs_path_cache,
                                pred_path_cache,
                                horizon=horizon,
                                threshold=threshold,
                                horizon_max_t=group_max_t,
                                classification=classification,
                                max_t=group_max_t,
                            )
                            if native_metric is not None:
                                metric_cache[metric_key] = native_metric
                            else:
                                error_key = (id(obs_path_cache), round(float(group_max_t), 6))
                                if error_key not in error_cache:
                                    error_cache[error_key] = _trajectory_error_sequence(
                                        obs_path_cache,
                                        pred_path_cache,
                                        max_t=group_max_t,
                                    )
                                metric_cache[metric_key] = _evaluate_error_sequence(
                                    error_cache[error_key],
                                    horizon=horizon,
                                    threshold=threshold,
                                    horizon_max_t=group_max_t,
                                    classification=classification,
                                )
                        matched, cost = metric_cache[metric_key]
                        best = best_by_metric[metric]
                        if matched and (best is None or cost < best[1]):
                            best_by_metric[metric] = (path_idx, cost)
            for metric, best in best_by_metric.items():
                if best is not None:
                    candidates_by_metric[metric].setdefault(oid, []).append((group, best[0], best[1]))

    observed_ids = set()
    for group in VALIDATION_GROUPS:
        observed_ids |= {oid for oid in observed_objs_by_group[group].keys() if not _is_vsl_id(oid)}

    plan_cache: dict[tuple[str, str], dict] = {}
    for metric in metrics:
        status_pred_indices, status_observed_oids = _empty_validation_status_maps()
        total = 0
        ok = 0
        for oid in sorted(observed_ids):
            if oid in excluded_short_observed_ids:
                for group in VALIDATION_GROUPS:
                    obs_entry = observed_objs_by_group[group].get(oid)
                    if obs_entry is not None:
                        status_observed_oids["observed_ok"][group][oid] = oid
                for group in VALIDATION_GROUPS:
                    pred_entry = predicted_objs_by_group[group].get(oid)
                    if pred_entry is None:
                        continue
                    for pidx, _ in enumerate(pred_entry.get("path_caches", [])):
                        status_pred_indices["ignore"][group][oid].append(pidx)
                continue
            total += 1
            candidates = candidates_by_metric[metric].get(oid, [])
            if candidates:
                ok += 1
                opt_group, opt_idx, _opt_cost = min(
                    candidates,
                    key=lambda v: (
                        OPTIMAL_SELECTION_GROUP_PRIORITY.get(v[0], len(OPTIMAL_SELECTION_GROUP_PRIORITY)),
                        v[2],
                    ),
                )
                for group in VALIDATION_GROUPS:
                    obs_entry = observed_objs_by_group[group].get(oid)
                    if obs_entry is not None:
                        status_observed_oids["observed_ok"][group][oid] = oid
                for group in VALIDATION_GROUPS:
                    pred_entry = predicted_objs_by_group[group].get(oid)
                    if pred_entry is None:
                        continue
                    for pidx, _ in enumerate(pred_entry.get("path_caches", [])):
                        if group == opt_group and pidx == opt_idx:
                            status_pred_indices["optimal"][group][oid].append(pidx)
                        else:
                            status_pred_indices["ignore"][group][oid].append(pidx)
            else:
                for group in VALIDATION_GROUPS:
                    obs_entry = observed_objs_by_group[group].get(oid)
                    if obs_entry is not None:
                        status_observed_oids["observed_ng"][group][oid] = oid
                for group in VALIDATION_GROUPS:
                    pred_entry = predicted_objs_by_group[group].get(oid)
                    if pred_entry is None:
                        continue
                    for pidx, _ in enumerate(pred_entry.get("path_caches", [])):
                        status_pred_indices["fail"][group][oid].append(pidx)
        per_object: dict[int, dict] = {}
        for oid in sorted(observed_ids):
            if oid in excluded_short_observed_ids:
                continue
            candidates = candidates_by_metric[metric].get(oid, [])
            per_object[int(oid)] = {"ok": 1 if candidates else 0, "total": 1}
        plan_cache[metric] = {
            "status_pred_indices": status_pred_indices,
            "status_observed_oids": status_observed_oids,
            "total": total,
            "ok": ok,
            "per_object": per_object,
        }

    context["validation_plan_cache"] = plan_cache
    return plan_cache


def _write_collision_payload(
    side: str,
    kind: str,
    status: str,
    group: str | None,
    t,
    stamp: int,
    payload,
    context: dict,
    out_bag,
) -> None:
    topic = _make_collision_topic(side, kind, status, group)
    if status in COLLISION_GROUPED_STATUSES:
        group_name = str(group or "other")
        group_template_msg = context["group_templates"].get(group_name) or context["ref_msg"] or context["base_template"]
        msg_out = _make_empty_object_set_like(group_template_msg, stamp)
        if msg_out is None:
            return
        if status in ("pred_collision", "pred_safe"):
            for oid, idx_list in (payload or {}).items():
                src_entry = context["predicted_objs_by_group"][group_name].get(int(oid))
                if src_entry is None:
                    continue
                filtered = _get_filtered_obj_for_write(src_entry, idx_list)
                if getattr(filtered, "trajectory_set", []) or []:
                    msg_out.objects.append(filtered)
        elif bool(payload):
            ego_pred_obj = context.get("ego_pred_objs_by_group", {}).get(group_name)
            if ego_pred_obj is not None:
                ego_pred_copy = _get_observed_obj_for_write(ego_pred_obj)
                if ego_pred_copy is not None:
                    msg_out.objects.append(ego_pred_copy)
    else:
        base_template_msg = context["base_template"] or context["ref_msg"]
        msg_out = _make_empty_object_set_like(base_template_msg, stamp)
        if msg_out is None:
            return
        if bool(payload):
            ego_observed_entry = context.get("ego_observed_entry")
            if ego_observed_entry is not None:
                ego_observed_copy = _get_observed_obj_for_write(ego_observed_entry)
                if ego_observed_copy is not None:
                    msg_out.objects.append(ego_observed_copy)
    out_bag.write(topic, msg_out, t)


def _build_collision_for_side(
    rel: str,
    out_dir: Path,
    side: str,
    result_bag: Path,
    observed_bag: Path,
    result_ns: str,
    observed_ns: str,
    scene_timing: dict | None = None,
    scene_cache: dict | None = None,
    out_bag=None,
    chunk_label: str = "",
) -> tuple[dict, list[dict], dict[int, dict[str, dict]]]:
    """Returns (summary, subscene_summaries, per_object_collision) where per_object_collision[oid][kind] = {collision_paths, checked_paths}."""
    summary = _default_collision_summary("ok")
    subscene_summaries = _build_subscene_side_summaries(scene_timing, "ok")
    per_object_collision: dict[int, dict[str, dict]] = {}  # oid -> kind -> {collision_paths, checked_paths}
    bag_path = out_dir / f"collision_judgement_{side}.bag"
    manage_out_bag = False
    write_state = {"last_percent": -1}
    write_total = 0
    written = 0

    if not HAS_ROSBAG:
        for kind in COLLISION_KINDS:
            summary[kind]["detail"] = "rosbag not available"
            for sub in subscene_summaries:
                sub["collision"][kind]["detail"] = summary[kind]["detail"]
        return summary, subscene_summaries, per_object_collision
    if not result_bag.exists():
        for kind in COLLISION_KINDS:
            summary[kind]["detail"] = f"result bag missing: {result_bag}"
            for sub in subscene_summaries:
                sub["collision"][kind]["detail"] = summary[kind]["detail"]
        return summary, subscene_summaries, per_object_collision
    if not observed_bag.exists():
        for kind in COLLISION_KINDS:
            summary[kind]["detail"] = f"observed bag missing: {observed_bag}"
            for sub in subscene_summaries:
                sub["collision"][kind]["detail"] = summary[kind]["detail"]
        return summary, subscene_summaries, per_object_collision
    else:
        cache = scene_cache or _build_scene_eval_cache(
            result_bag=result_bag,
            observed_bag=observed_bag,
            result_ns=result_ns,
            observed_ns=observed_ns,
        )
        stamp_contexts = cache.get("stamp_contexts", {})
        eval_stamp_set = {
            stamp
            for stamp in cache.get("eval_stamp_set", set())
            if stamp in stamp_contexts and is_stamp_in_evaluation_range(_to_nsec(stamp_contexts[stamp]["ref_t"]), scene_timing)
        }
        master_stamps = [
            stamp
            for stamp in cache.get("master_stamps", [])
            if stamp in stamp_contexts and is_stamp_in_evaluation_range(_to_nsec(stamp_contexts[stamp]["ref_t"]), scene_timing)
        ]

        progress_state = {"last_percent": -1}
        collision_prefix = f"[collision] {rel} {side}"
        if chunk_label:
            collision_prefix += f" {chunk_label}"
        collision_write_prefix = f"{collision_prefix} write"
        if master_stamps:
            _print_progress_line(
                collision_prefix,
                0,
                len(master_stamps),
                progress_state,
            )
        write_total = len(master_stamps) * (
            len(COLLISION_KINDS) * (
                len(COLLISION_GROUPED_STATUSES) * len(VALIDATION_GROUPS)
                + len(COLLISION_BASE_STATUSES)
            )
        )
        if out_bag is None:
            out_bag = rosbag.Bag(str(bag_path), "w", chunk_threshold=64 * 1024)
            manage_out_bag = True
        if write_total > 0:
            _print_progress_line(
                collision_write_prefix,
                0,
                write_total,
                write_state,
            )
        try:
            for idx, stamp in enumerate(master_stamps, start=1):
                context = stamp_contexts.get(stamp)
                if context is None:
                    _print_progress_line(
                        collision_prefix,
                        idx,
                        len(master_stamps),
                        progress_state,
                    )
                    continue
                ref_t = context["ref_t"]
                predicted_objs_by_group = context["predicted_objs_by_group"]

                # VSL objects are not collision-judged, but must be exported as no-collision targets.
                vsl_safe_indices = {g: defaultdict(list) for g in VALIDATION_GROUPS}
                for group in VALIDATION_GROUPS:
                    for oid, pred_entry in predicted_objs_by_group[group].items():
                        if not _is_vsl_id(oid):
                            continue
                        for path_idx, _ in enumerate(pred_entry.get("path_caches", [])):
                            vsl_safe_indices[group][oid].append(path_idx)

                ego_observed_entry = context.get("ego_observed_entry")
                ego_observed_obj = ego_observed_entry["obj"] if ego_observed_entry is not None else None
                ego_observed_dims = (4.7, 1.85)
                ego_observed_path_cache = ego_observed_entry.get("path_cache") if ego_observed_entry is not None else None
                if ego_observed_obj is not None:
                    ego_observed_dims = _obj_dimensions_of(ego_observed_obj)
                if ego_observed_path_cache is not None and stamp in eval_stamp_set:
                    _ensure_collision_profile(ego_observed_path_cache, ego_observed_dims)
                ego_pred_objs_by_group = context.get("ego_pred_objs_by_group", {})

                pred_collision_indices = {kind: {g: defaultdict(list) for g in VALIDATION_GROUPS} for kind in COLLISION_KINDS}
                pred_safe_indices = {kind: {g: defaultdict(list) for g in VALIDATION_GROUPS} for kind in COLLISION_KINDS}
                kind_collision_count = {kind: 0 for kind in COLLISION_KINDS}
                kind_checked_count = {kind: 0 for kind in COLLISION_KINDS}
                stamp_has_collision = {kind: False for kind in COLLISION_KINDS}

                for group in VALIDATION_GROUPS:
                    for oid, pred_entry in predicted_objs_by_group[group].items():
                        if _is_ego_id(oid) or _is_vsl_id(oid):
                            continue
                        pred_dims = pred_entry.get("dims", (4.0, 1.8))
                        for path_idx, pred_path_cache in enumerate(pred_entry.get("path_caches", [])):
                            collision_results = {kind: False for kind in COLLISION_KINDS}
                            if ego_observed_path_cache is not None and stamp in eval_stamp_set:
                                _ensure_collision_profile(pred_path_cache, pred_dims)
                                collision_results = _path_collision_results(
                                    pred_path_cache,
                                    pred_dims,
                                    ego_observed_path_cache,
                                    ego_observed_dims,
                                )
                                for kind in COLLISION_KINDS:
                                    kind_checked_count[kind] += 1
                            for kind in COLLISION_KINDS:
                                if collision_results.get(kind, False):
                                    pred_collision_indices[kind][group][oid].append(path_idx)
                                    kind_collision_count[kind] += 1
                                    stamp_has_collision[kind] = True
                                else:
                                    pred_safe_indices[kind][group][oid].append(path_idx)

                # Keep VSL visible in collision outputs as always safe.
                for kind in COLLISION_KINDS:
                    for group in VALIDATION_GROUPS:
                        for oid, idx_list in vsl_safe_indices[group].items():
                            pred_safe_indices[kind][group][oid].extend(idx_list)

                for kind in COLLISION_KINDS:
                    if stamp in eval_stamp_set:
                        summary[kind]["checked_paths"] += kind_checked_count[kind]
                        summary[kind]["collision_paths"] += kind_collision_count[kind]
                        summary[kind]["has_collision"] = bool(summary[kind]["has_collision"] or stamp_has_collision[kind])
                        sub_summary = _subscene_summary_for_stamp(_to_nsec(ref_t), scene_timing, subscene_summaries)
                        if sub_summary is not None:
                            sub_summary["collision"][kind]["checked_paths"] += kind_checked_count[kind]
                            sub_summary["collision"][kind]["collision_paths"] += kind_collision_count[kind]
                            sub_summary["collision"][kind]["has_collision"] = bool(
                                sub_summary["collision"][kind]["has_collision"] or stamp_has_collision[kind]
                            )
                        # Per-object collision for common-object aggregation
                        for group in VALIDATION_GROUPS:
                            for oid in set(pred_collision_indices[kind][group].keys()) | set(pred_safe_indices[kind][group].keys()):
                                if _is_ego_id(oid) or _is_vsl_id(oid):
                                    continue
                                c = len(pred_collision_indices[kind][group].get(oid, []))
                                s = len(pred_safe_indices[kind][group].get(oid, []))
                                oid_int = int(oid)
                                if oid_int not in per_object_collision:
                                    per_object_collision[oid_int] = {k: {"collision_paths": 0, "checked_paths": 0} for k in COLLISION_KINDS}
                                per_object_collision[oid_int][kind]["collision_paths"] += c
                                per_object_collision[oid_int][kind]["checked_paths"] += c + s

                    for group in VALIDATION_GROUPS:
                        for status in COLLISION_GROUPED_STATUSES:
                            if status == "pred_collision":
                                payload = {
                                    int(oid): tuple(idx_list)
                                    for oid, idx_list in pred_collision_indices[kind][group].items()
                                    if idx_list
                                }
                            elif status == "pred_safe":
                                payload = {
                                    int(oid): tuple(idx_list)
                                    for oid, idx_list in pred_safe_indices[kind][group].items()
                                    if idx_list
                                }
                            elif status == "ego_pred_collision":
                                payload = bool(ego_pred_objs_by_group.get(group) is not None and stamp_has_collision[kind])
                            elif status == "ego_pred_safe":
                                payload = bool(ego_pred_objs_by_group.get(group) is not None and (not stamp_has_collision[kind]))
                            else:
                                payload = {}
                            if out_bag is not None:
                                _write_collision_payload(
                                    side=side,
                                    kind=kind,
                                    status=status,
                                    group=group,
                                    t=ref_t,
                                    stamp=stamp,
                                    payload=payload,
                                    context=context,
                                    out_bag=out_bag,
                                )
                                written += 1
                                if write_total > 0:
                                    _print_progress_line(
                                        collision_write_prefix,
                                        written,
                                        write_total,
                                        write_state,
                                    )

                    for status in COLLISION_BASE_STATUSES:
                        payload = bool(
                            ego_observed_obj is not None
                            and (
                                (status == "ego_observed_collision" and stamp_has_collision[kind])
                                or (status == "ego_observed_safe" and (not stamp_has_collision[kind]))
                            )
                        )
                        if out_bag is not None:
                            _write_collision_payload(
                                side=side,
                                kind=kind,
                                status=status,
                                group=None,
                                t=ref_t,
                                stamp=stamp,
                                payload=payload,
                                context=context,
                                out_bag=out_bag,
                            )
                            written += 1
                            if write_total > 0:
                                _print_progress_line(
                                    collision_write_prefix,
                                    written,
                                    write_total,
                                    write_state,
                                )

                _print_progress_line(
                    collision_prefix,
                    idx,
                    len(master_stamps),
                    progress_state,
                )
        finally:
            if manage_out_bag and out_bag is not None:
                out_bag.close()

    for kind in COLLISION_KINDS:
        summary[kind]["has_collision"] = bool(summary[kind]["collision_paths"] > 0)
        for sub in subscene_summaries:
            sub["collision"][kind]["has_collision"] = bool(sub["collision"][kind]["collision_paths"] > 0)
    if manage_out_bag and out_bag is None and HAS_ROSBAG:
        with rosbag.Bag(str(bag_path), "w", chunk_threshold=64 * 1024):
            pass
    elif manage_out_bag and not bag_path.exists():
        bag_path.write_bytes(b"")

    return summary, subscene_summaries, per_object_collision


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
    scene_timing: dict | None = None,
    scene_cache: dict | None = None,
    out_bag=None,
    chunk_label: str = "",
) -> tuple[dict, list[dict], dict[int, dict]]:
    """Returns (summary, subscene_summaries, per_object) where per_object[oid] = {ok, total} for this horizon/threshold."""
    summary = _default_threshold_summary("ok")
    per_object: dict[int, dict] = {}
    log_key = _validation_log_key(side, horizon, threshold)
    subscene_summaries = _build_subscene_side_summaries(scene_timing, "ok")

    if not HAS_ROSBAG:
        summary["detail"] = "rosbag not available"
        for sub in subscene_summaries:
            sub[horizon][threshold]["detail"] = summary["detail"]
        return summary, subscene_summaries, per_object
    if not result_bag.exists():
        summary["detail"] = f"result bag missing: {result_bag}"
        for sub in subscene_summaries:
            sub[horizon][threshold]["detail"] = summary["detail"]
        return summary, subscene_summaries, per_object
    if not observed_bag.exists():
        summary["detail"] = f"observed bag missing: {observed_bag}"
        for sub in subscene_summaries:
            sub[horizon][threshold]["detail"] = summary["detail"]
        return summary, subscene_summaries, per_object
    else:
        cache = scene_cache or _build_scene_eval_cache(
            result_bag=result_bag,
            observed_bag=observed_bag,
            result_ns=result_ns,
            observed_ns=observed_ns,
        )
        stamp_contexts = cache.get("stamp_contexts", {})
        eval_stamp_set = {
            stamp
            for stamp in cache.get("eval_stamp_set", set())
            if stamp in stamp_contexts and is_stamp_in_evaluation_range(_to_nsec(stamp_contexts[stamp]["ref_t"]), scene_timing)
        }
        master_stamps = [
            stamp
            for stamp in cache.get("master_stamps", [])
            if stamp in stamp_contexts and is_stamp_in_evaluation_range(_to_nsec(stamp_contexts[stamp]["ref_t"]), scene_timing)
        ]

        progress_state = {"last_percent": -1}
        validation_prefix = f"[validation] {rel} {log_key}"
        if chunk_label:
            validation_prefix += f" {chunk_label}"
        validation_write_prefix = f"{validation_prefix} write"
        if master_stamps:
            _print_progress_line(
                validation_prefix,
                0,
                len(master_stamps),
                progress_state,
            )
        write_total = len(master_stamps) * len(VALIDATION_GROUPS) * len(VALIDATION_STATUSES)
        write_state = {"last_percent": -1}
        written = 0
        if out_bag is not None and write_total > 0:
            _print_progress_line(
                validation_write_prefix,
                0,
                write_total,
                write_state,
            )

        for idx, stamp in enumerate(master_stamps, start=1):
            context = stamp_contexts.get(stamp)
            if context is None:
                _print_progress_line(
                    validation_prefix,
                    idx,
                    len(master_stamps),
                    progress_state,
                )
                continue
            ref_t = context["ref_t"]
            metric_plan = None
            if stamp in eval_stamp_set:
                metric_plan = _ensure_context_validation_plan_cache(context).get((horizon, threshold))
                if metric_plan is not None:
                    summary["total"] += int(metric_plan.get("total", 0))
                    summary["ok"] += int(metric_plan.get("ok", 0))
                    for oid, po in (metric_plan.get("per_object") or {}).items():
                        oid_int = int(oid)
                        if oid_int not in per_object:
                            per_object[oid_int] = {"ok": 0, "total": 0}
                        per_object[oid_int]["ok"] += int(po.get("ok", 0))
                        per_object[oid_int]["total"] += int(po.get("total", 0))
                    sub_summary = _subscene_summary_for_stamp(_to_nsec(ref_t), scene_timing, subscene_summaries)
                    if sub_summary is not None:
                        sub_summary[horizon][threshold]["total"] += int(metric_plan.get("total", 0))
                        sub_summary[horizon][threshold]["ok"] += int(metric_plan.get("ok", 0))

            status_pred_indices, status_observed_oids = _empty_validation_status_maps()
            if metric_plan is not None:
                status_pred_indices = metric_plan["status_pred_indices"]
                status_observed_oids = metric_plan["status_observed_oids"]

            for group in VALIDATION_GROUPS:
                for status in VALIDATION_STATUSES:
                    if status in ("optimal", "ignore", "fail"):
                        payload = {
                            int(oid): tuple(idx_list)
                            for oid, idx_list in status_pred_indices[status][group].items()
                            if idx_list
                        }
                    else:
                        payload = tuple(int(oid) for oid in status_observed_oids[status][group].keys())
                    if out_bag is not None:
                        _write_validation_payload(
                            side=side,
                            horizon=horizon,
                            threshold=threshold,
                            group=group,
                            status=status,
                            t=ref_t,
                            stamp=stamp,
                            payload=payload,
                            context=context,
                            out_bag=out_bag,
                        )
                        written += 1
                        if write_total > 0:
                            _print_progress_line(
                                validation_write_prefix,
                                written,
                                write_total,
                                write_state,
                            )

            _print_progress_line(
                validation_prefix,
                idx,
                len(master_stamps),
                progress_state,
            )

    if summary["total"] > 0:
        summary["rate"] = (100.0 * float(summary["ok"])) / float(summary["total"])
    else:
        summary["rate"] = 0.0

    for sub in subscene_summaries:
        sub_summary = sub[horizon][threshold]
        if sub_summary["total"] > 0:
            sub_summary["rate"] = (100.0 * float(sub_summary["ok"])) / float(sub_summary["total"])
        else:
            sub_summary["rate"] = 0.0

    return summary, subscene_summaries, per_object


def evaluate_validation_for_side(
    rel: str,
    out_dir: Path,
    side: str,
    result_bag: Path,
    observed_bag: Path,
    result_ns: str,
    observed_ns: str,
    scene_timing: dict | None = None,
) -> dict:
    summary = {
        "scene_timing": scene_timing,
        "subscenes": _build_subscene_side_summaries(scene_timing, "not_evaluated"),
    }
    for horizon in VALIDATION_HORIZONS:
        summary[horizon] = {
            threshold: _default_threshold_summary("ok")
            for threshold in VALIDATION_THRESHOLDS
        }
    summary["collision"] = _default_collision_summary("ok")
    per_object_validation: dict[int, dict[str, dict[str, dict]]] = {}
    validation_bag_path = out_dir / f"validation_{side}.bag"
    collision_bag_path = out_dir / f"collision_judgement_{side}.bag"
    for stale_path in (validation_bag_path, collision_bag_path):
        if stale_path.exists():
            stale_path.unlink()
    def _set_empty_common_fields():
        summary["evaluated_object_ids"] = []
        summary["per_object_validation"] = {}
        summary["per_object_collision"] = {}

    if not HAS_ROSBAG:
        detail = "rosbag not available"
        for horizon in VALIDATION_HORIZONS:
            for threshold in VALIDATION_THRESHOLDS:
                summary[horizon][threshold]["detail"] = detail
        summary["collision"] = _default_collision_summary(detail)
        _set_empty_common_fields()
        if not validation_bag_path.exists():
            validation_bag_path.write_bytes(b"")
        if not collision_bag_path.exists():
            collision_bag_path.write_bytes(b"")
        return summary
    if not result_bag.exists():
        detail = f"result bag missing: {result_bag}"
        for horizon in VALIDATION_HORIZONS:
            for threshold in VALIDATION_THRESHOLDS:
                summary[horizon][threshold]["detail"] = detail
        summary["collision"] = _default_collision_summary(detail)
        _set_empty_common_fields()
        with rosbag.Bag(str(validation_bag_path), "w", chunk_threshold=64 * 1024):
            pass
        with rosbag.Bag(str(collision_bag_path), "w", chunk_threshold=64 * 1024):
            pass
        return summary
    if not observed_bag.exists():
        detail = f"observed bag missing: {observed_bag}"
        for horizon in VALIDATION_HORIZONS:
            for threshold in VALIDATION_THRESHOLDS:
                summary[horizon][threshold]["detail"] = detail
        summary["collision"] = _default_collision_summary(detail)
        _set_empty_common_fields()
        with rosbag.Bag(str(validation_bag_path), "w", chunk_threshold=64 * 1024):
            pass
        with rosbag.Bag(str(collision_bag_path), "w", chunk_threshold=64 * 1024):
            pass
        return summary

    message_cache = _build_scene_message_cache(
        result_bag=result_bag,
        observed_bag=observed_bag,
        result_ns=result_ns,
        observed_ns=observed_ns,
    )
    filtered_master_stamps = _filter_scene_master_stamps_for_range(message_cache, scene_timing)

    validation_bag = rosbag.Bag(str(validation_bag_path), "w", chunk_threshold=64 * 1024)
    collision_bag = rosbag.Bag(str(collision_bag_path), "w", chunk_threshold=64 * 1024)
    # per_object_validation[oid][horizon][threshold] = {ok, total} for common-object aggregation
    per_object_validation: dict[int, dict[str, dict[str, dict]]] = {}
    per_object_collision: dict[int, dict[str, dict]] = {}  # oid -> kind -> {collision_paths, checked_paths}
    try:
        chunk_total = max(1, (len(filtered_master_stamps) + EVAL_STAMP_CHUNK_SIZE - 1) // EVAL_STAMP_CHUNK_SIZE)
        for chunk_idx, chunk_stamps in enumerate(_iter_stamp_chunks(filtered_master_stamps), start=1):
            chunk_cache = _build_scene_eval_chunk_cache(message_cache, chunk_stamps)
            chunk_label = f"chunk {chunk_idx}/{chunk_total}"
            try:
                for horizon in VALIDATION_HORIZONS:
                    window_cfg = VALIDATION_HORIZON_WINDOWS.get(horizon, VALIDATION_HORIZON_WINDOWS["half-time-relaxed"])
                    max_t = float(window_cfg.get("max_t", 5.0))
                    other_max_t = float(window_cfg.get("other_max_t", max_t))
                    for threshold in VALIDATION_THRESHOLDS:
                        chunk_summary, chunk_subscene_threshold_summaries, chunk_per_object = _build_validation_for_side_threshold(
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
                            scene_timing=scene_timing,
                            scene_cache=chunk_cache,
                            out_bag=validation_bag,
                            chunk_label=chunk_label,
                        )
                        dst_summary = summary[horizon][threshold]
                        dst_summary["ok"] += int(chunk_summary.get("ok", 0))
                        dst_summary["total"] += int(chunk_summary.get("total", 0))
                        if dst_summary.get("detail") == "ok" and chunk_summary.get("detail") not in (None, "ok"):
                            dst_summary["detail"] = chunk_summary.get("detail")
                        for oid, po in (chunk_per_object or {}).items():
                            if oid not in per_object_validation:
                                per_object_validation[oid] = {
                                    h: {t: {"ok": 0, "total": 0} for t in VALIDATION_THRESHOLDS}
                                    for h in VALIDATION_HORIZONS
                                }
                            per_object_validation[oid][horizon][threshold]["ok"] += int(po.get("ok", 0))
                            per_object_validation[oid][horizon][threshold]["total"] += int(po.get("total", 0))
                        for idx, sub in enumerate(chunk_subscene_threshold_summaries):
                            if idx < len(summary["subscenes"]):
                                dst = summary["subscenes"][idx][horizon][threshold]
                                src = sub[horizon][threshold]
                                dst["ok"] += int(src.get("ok", 0))
                                dst["total"] += int(src.get("total", 0))
                                if dst.get("detail") == "not_evaluated" and src.get("detail") not in (None, "ok", "not_evaluated"):
                                    dst["detail"] = src.get("detail")

                chunk_collision_summary, chunk_collision_subscenes, chunk_per_object_collision = _build_collision_for_side(
                    rel=rel,
                    out_dir=out_dir,
                    side=side,
                    result_bag=result_bag,
                    observed_bag=observed_bag,
                    result_ns=result_ns,
                    observed_ns=observed_ns,
                    scene_timing=scene_timing,
                    scene_cache=chunk_cache,
                    out_bag=collision_bag,
                    chunk_label=chunk_label,
                )
                for oid, kind_d in (chunk_per_object_collision or {}).items():
                    if oid not in per_object_collision:
                        per_object_collision[oid] = {k: {"collision_paths": 0, "checked_paths": 0} for k in COLLISION_KINDS}
                    for kind in COLLISION_KINDS:
                        per_object_collision[oid][kind]["collision_paths"] += int(kind_d.get(kind, {}).get("collision_paths", 0))
                        per_object_collision[oid][kind]["checked_paths"] += int(kind_d.get(kind, {}).get("checked_paths", 0))
                for kind in COLLISION_KINDS:
                    dst = summary["collision"][kind]
                    src = chunk_collision_summary.get(kind, {})
                    dst["collision_paths"] += int(src.get("collision_paths", 0))
                    dst["checked_paths"] += int(src.get("checked_paths", 0))
                    dst["has_collision"] = bool(dst["has_collision"] or src.get("has_collision", False))
                    if dst.get("detail") == "ok" and src.get("detail") not in (None, "ok"):
                        dst["detail"] = src.get("detail")
                for idx, sub in enumerate(chunk_collision_subscenes):
                    if idx < len(summary["subscenes"]):
                        for kind in COLLISION_KINDS:
                            dst = summary["subscenes"][idx]["collision"][kind]
                            src = sub["collision"][kind]
                            dst["collision_paths"] += int(src.get("collision_paths", 0))
                            dst["checked_paths"] += int(src.get("checked_paths", 0))
                            dst["has_collision"] = bool(dst["has_collision"] or src.get("has_collision", False))
                            if dst.get("detail") == "not_evaluated" and src.get("detail") not in (None, "ok", "not_evaluated"):
                                dst["detail"] = src.get("detail")
            finally:
                del chunk_cache
                gc.collect()
    finally:
        validation_bag.close()
        collision_bag.close()

    for horizon in VALIDATION_HORIZONS:
        for threshold in VALIDATION_THRESHOLDS:
            node = summary[horizon][threshold]
            if node["total"] > 0:
                node["rate"] = (100.0 * float(node["ok"])) / float(node["total"])
            else:
                node["rate"] = 0.0
            log_key = _validation_log_key(side, horizon, threshold)
            print(
                f"[validation] {rel} {log_key}: "
                f"{node['rate']:.1f}% ({node['ok']}/{node['total']})"
            )
    for sub in summary["subscenes"]:
        for horizon in VALIDATION_HORIZONS:
            for threshold in VALIDATION_THRESHOLDS:
                node = sub[horizon][threshold]
                if node["total"] > 0:
                    node["rate"] = (100.0 * float(node["ok"])) / float(node["total"])
                else:
                    node["rate"] = 0.0
                if node.get("detail") == "not_evaluated":
                    node["detail"] = "ok"
        for kind in COLLISION_KINDS:
            sub["collision"][kind]["has_collision"] = bool(sub["collision"][kind]["collision_paths"] > 0)
            if sub["collision"][kind].get("detail") == "not_evaluated":
                sub["collision"][kind]["detail"] = "ok"
    for kind in COLLISION_KINDS:
        node = summary["collision"].get(kind, {})
        node["has_collision"] = bool(node.get("collision_paths", 0) > 0)
        state = "Collision" if bool(node.get("has_collision")) else "Safe"
        print(
            f"[collision] {rel} {side}-{kind}: {state} "
            f"({int(node.get('collision_paths', 0))}/{int(node.get('checked_paths', 0))})"
        )
    # For common-object aggregation: serializable per-object validation/collision and evaluated object IDs
    evaluated_object_ids = sorted(per_object_validation.keys())
    summary["evaluated_object_ids"] = evaluated_object_ids
    # JSON: oid as string key; inner structure horizon -> threshold -> {ok, total}
    summary["per_object_validation"] = {
        str(oid): {
            h: {t: per_object_validation[oid][h][t] for t in VALIDATION_THRESHOLDS}
            for h in VALIDATION_HORIZONS
        }
        for oid in evaluated_object_ids
    }
    summary["per_object_collision"] = {
        str(oid): {kind: per_object_collision[oid][kind] for kind in COLLISION_KINDS}
        for oid in sorted(per_object_collision.keys())
    }
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
