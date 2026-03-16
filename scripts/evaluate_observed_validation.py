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

from common import (
    VALIDATION_BASELINE_NS,
    VALIDATION_TEST_NS,
    find_subscene_index,
    is_stamp_in_evaluation_range,
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
    obs_curve = _build_observed_curve(obs_pts)

    pred_times = [p[0] for p in pred_pts]
    cost = 0.0
    used = 0
    strict_ok_steps = 0
    max_dt = 0.26
    for i, (obs_t, _obs_x, _obs_y, obs_yaw) in enumerate(obs_pts):
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

        fallback_heading = _observed_heading_from_velocity(obs_pts, i)
        s_pred, d_pred = _project_xy_to_observed_sd(
            obs_curve,
            float(pred_x),
            float(pred_y),
            anchor_idx=i,
            fallback_heading=fallback_heading,
        )
        s_obs = float(obs_curve["s"][i]) if i < len(obs_curve.get("s", [])) else 0.0
        longitudinal = s_pred - s_obs
        lateral = d_pred
        longitudinal_tol, lateral_tol = _threshold_tolerances(
            horizon,
            threshold,
            obs_t,
            horizon_max_t,
            classification,
        )
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
    pred_pts = _sorted_collision_points(pred_path)
    ego_pts = _sorted_collision_points(ego_path)
    if not pred_pts or not ego_pts:
        return False
    pred_curve = _build_collision_curve(pred_pts)
    ego_curve = _build_collision_curve(ego_pts)
    ego_times = [float(p["t"]) for p in ego_pts]
    for i, pred in enumerate(pred_pts):
        heading, speed = _path_heading_speed(pred_pts, i)
        ego_pair = _nearest_point_by_time(ego_pts, ego_times, float(pred["t"]))
        if ego_pair is None:
            continue
        ego_idx, _ = ego_pair
        ego_heading, ego_speed = _path_heading_speed(ego_pts, ego_idx)
        pred_poly = _collider_polygon(
            pred_curve,
            i,
            heading,
            speed,
            pred_dims[0],
            pred_dims[1],
            other_kind,
        )
        ego_poly = _collider_polygon(
            ego_curve,
            ego_idx,
            ego_heading,
            ego_speed,
            ego_dims[0],
            ego_dims[1],
            "soft",
        )
        if _polygons_intersect(pred_poly, ego_poly):
            return True
    return False


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


def _empty_validation_out_records() -> dict[str, dict[str, list[tuple]]]:
    return {
        group: {status: [] for status in VALIDATION_STATUSES}
        for group in VALIDATION_GROUPS
    }


def _make_collision_topic(side: str, kind: str, status: str, group: str | None = None) -> str:
    if status in COLLISION_GROUPED_STATUSES:
        suffix = GROUP_TO_WM_SUFFIX.get(group or "other", GROUP_TO_WM_SUFFIX["other"])
    else:
        suffix = GROUP_TO_WM_SUFFIX["base"]
    return f"{COLLISION_NS_ROOT}/{side}/{kind}/{status}{suffix}"


def _write_validation_records_to_bag(
    rel: str,
    side: str,
    horizon: str,
    threshold: str,
    out_records: dict[str, dict[str, list[tuple]]],
    out_bag,
) -> None:
    log_key = _validation_log_key(side, horizon, threshold)
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
            topic = _make_validation_topic(side, horizon, threshold, group, status)
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


def _build_collision_for_side(
    rel: str,
    out_dir: Path,
    side: str,
    result_bag: Path,
    observed_bag: Path,
    result_ns: str,
    observed_ns: str,
    scene_timing: dict | None = None,
) -> tuple[dict, list[dict]]:
    summary = _default_collision_summary("ok")
    subscene_summaries = _build_subscene_side_summaries(scene_timing, "ok")
    out_grouped: dict[str, dict[str, dict[str, list[tuple]]]] = {
        kind: {status: {group: [] for group in VALIDATION_GROUPS} for status in COLLISION_GROUPED_STATUSES}
        for kind in COLLISION_KINDS
    }
    out_base: dict[str, dict[str, list[tuple]]] = {
        kind: {status: [] for status in COLLISION_BASE_STATUSES}
        for kind in COLLISION_KINDS
    }

    if not HAS_ROSBAG:
        for kind in COLLISION_KINDS:
            summary[kind]["detail"] = "rosbag not available"
            for sub in subscene_summaries:
                sub["collision"][kind]["detail"] = summary[kind]["detail"]
    elif not result_bag.exists():
        for kind in COLLISION_KINDS:
            summary[kind]["detail"] = f"result bag missing: {result_bag}"
            for sub in subscene_summaries:
                sub["collision"][kind]["detail"] = summary[kind]["detail"]
    elif not observed_bag.exists():
        for kind in COLLISION_KINDS:
            summary[kind]["detail"] = f"observed bag missing: {observed_bag}"
            for sub in subscene_summaries:
                sub["collision"][kind]["detail"] = summary[kind]["detail"]
    else:
        result_topics = _resolve_group_topics(result_bag, result_ns)
        observed_topics = _resolve_group_topics(observed_bag, observed_ns)
        result_by_group = _collect_group_messages_by_stamp(result_bag, result_topics)
        observed_by_group = _collect_group_messages_by_stamp(observed_bag, observed_topics)

        result_base_stamps = {
            stamp
            for stamp, record in result_by_group.get("base", {}).items()
            if is_stamp_in_evaluation_range(_to_nsec(record[0]), scene_timing)
        }
        observed_base_stamps = {
            stamp
            for stamp, record in observed_by_group.get("base", {}).items()
            if is_stamp_in_evaluation_range(_to_nsec(record[0]), scene_timing)
        }
        eval_stamp_set = result_base_stamps & observed_base_stamps
        master_stamps = sorted(result_base_stamps)

        progress_state = {"last_percent": -1}
        if master_stamps:
            _print_progress_line(
                f"[collision] {rel} {side}",
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
            res_base_rec = result_by_group.get("base", {}).get(stamp)
            if res_base_rec is None:
                _print_progress_line(
                    f"[collision] {rel} {side}",
                    idx,
                    len(master_stamps),
                    progress_state,
                )
                continue
            ref_t, ref_msg = res_base_rec

            predicted_objs_by_group: dict[str, dict[int, object]] = {g: {} for g in VALIDATION_GROUPS}
            for group in VALIDATION_GROUPS:
                pred_rec = result_by_group.get(group, {}).get(stamp)
                if pred_rec is None:
                    continue
                _t_pred, msg_pred = pred_rec
                for obj in getattr(msg_pred, "objects", []) or []:
                    oid = _obj_id_of(obj)
                    if oid < 0:
                        continue
                    if _has_predicted_path(obj):
                        predicted_objs_by_group[group][oid] = obj

            # VSL objects are not collision-judged, but must be exported as no-collision targets.
            vsl_safe_indices = {g: defaultdict(list) for g in VALIDATION_GROUPS}
            for group in VALIDATION_GROUPS:
                for oid, pred_obj in predicted_objs_by_group[group].items():
                    if not _is_vsl_id(oid):
                        continue
                    traj_set = list(getattr(pred_obj, "trajectory_set", []) or [])
                    for path_idx, _ in enumerate(traj_set):
                        vsl_safe_indices[group][oid].append(path_idx)

            observed_base_rec = observed_by_group.get("base", {}).get(stamp)
            ego_observed_obj = None
            if observed_base_rec is not None:
                _t_obs, msg_obs = observed_base_rec
                for obj in getattr(msg_obs, "objects", []) or []:
                    oid = _obj_id_of(obj)
                    if oid < 0:
                        continue
                    if _is_ego_id(oid) and _has_predicted_path(obj):
                        ego_observed_obj = obj
                        break
            if ego_observed_obj is None:
                for group in VALIDATION_GROUPS:
                    obs_rec = observed_by_group.get(group, {}).get(stamp)
                    if obs_rec is None:
                        continue
                    _t_obs, msg_obs = obs_rec
                    for obj in getattr(msg_obs, "objects", []) or []:
                        oid = _obj_id_of(obj)
                        if oid >= 0 and _is_ego_id(oid) and _has_predicted_path(obj):
                            ego_observed_obj = obj
                            break
                    if ego_observed_obj is not None:
                        break
            ego_observed_path = None
            ego_observed_dims = (4.7, 1.85)
            if ego_observed_obj is not None:
                ego_paths = list(getattr(ego_observed_obj, "trajectory_set", []) or [])
                if ego_paths:
                    ego_observed_path = ego_paths[0]
                    ego_observed_dims = _obj_dimensions_of(ego_observed_obj)

            ego_pred_objs_by_group: dict[str, object] = {}
            for group in VALIDATION_GROUPS:
                for oid, obj in predicted_objs_by_group[group].items():
                    if _is_ego_id(oid):
                        ego_pred_objs_by_group[group] = obj
                        break

            for kind in COLLISION_KINDS:
                pred_collision_indices = {g: defaultdict(list) for g in VALIDATION_GROUPS}
                pred_safe_indices = {g: defaultdict(list) for g in VALIDATION_GROUPS}
                kind_collision_count = 0
                kind_checked_count = 0
                stamp_has_collision = False

                for group in VALIDATION_GROUPS:
                    for oid, pred_obj in predicted_objs_by_group[group].items():
                        if _is_ego_id(oid) or _is_vsl_id(oid):
                            continue
                        pred_dims = _obj_dimensions_of(pred_obj)
                        traj_set = list(getattr(pred_obj, "trajectory_set", []) or [])
                        for path_idx, pred_path in enumerate(traj_set):
                            collided = False
                            if ego_observed_path is not None and stamp in eval_stamp_set:
                                collided = _path_has_collision_with_ego(
                                    pred_path=pred_path,
                                    pred_dims=pred_dims,
                                    ego_path=ego_observed_path,
                                    ego_dims=ego_observed_dims,
                                    other_kind=kind,
                                )
                                kind_checked_count += 1
                            if collided:
                                pred_collision_indices[group][oid].append(path_idx)
                                kind_collision_count += 1
                                stamp_has_collision = True
                            else:
                                pred_safe_indices[group][oid].append(path_idx)

                # Keep VSL visible in collision outputs as always safe.
                for group in VALIDATION_GROUPS:
                    for oid, idx_list in vsl_safe_indices[group].items():
                        pred_safe_indices[group][oid].extend(idx_list)

                if stamp in eval_stamp_set:
                    summary[kind]["checked_paths"] += kind_checked_count
                    summary[kind]["collision_paths"] += kind_collision_count
                    summary[kind]["has_collision"] = bool(summary[kind]["has_collision"] or stamp_has_collision)
                    sub_summary = _subscene_summary_for_stamp(_to_nsec(ref_t), scene_timing, subscene_summaries)
                    if sub_summary is not None:
                        sub_summary["collision"][kind]["checked_paths"] += kind_checked_count
                        sub_summary["collision"][kind]["collision_paths"] += kind_collision_count
                        sub_summary["collision"][kind]["has_collision"] = bool(
                            sub_summary["collision"][kind]["has_collision"] or stamp_has_collision
                        )

                for group in VALIDATION_GROUPS:
                    group_template_msg = group_templates.get(group) or ref_msg or base_template
                    for status in COLLISION_GROUPED_STATUSES:
                        msg_out = _make_empty_object_set_like(group_template_msg, stamp)
                        if msg_out is None:
                            continue
                        if status == "pred_collision":
                            idx_map = pred_collision_indices[group]
                            for oid, idx_list in idx_map.items():
                                src_obj = predicted_objs_by_group[group].get(oid)
                                if src_obj is None:
                                    continue
                                filtered = _filter_obj_paths_by_indices(src_obj, idx_list)
                                if getattr(filtered, "trajectory_set", []) or []:
                                    msg_out.objects.append(filtered)
                        elif status == "pred_safe":
                            idx_map = pred_safe_indices[group]
                            for oid, idx_list in idx_map.items():
                                src_obj = predicted_objs_by_group[group].get(oid)
                                if src_obj is None:
                                    continue
                                filtered = _filter_obj_paths_by_indices(src_obj, idx_list)
                                if getattr(filtered, "trajectory_set", []) or []:
                                    msg_out.objects.append(filtered)
                        elif status == "ego_pred_collision":
                            ego_pred_obj = ego_pred_objs_by_group.get(group)
                            if ego_pred_obj is not None and stamp_has_collision:
                                msg_out.objects.append(copy.deepcopy(ego_pred_obj))
                        elif status == "ego_pred_safe":
                            ego_pred_obj = ego_pred_objs_by_group.get(group)
                            if ego_pred_obj is not None and (not stamp_has_collision):
                                msg_out.objects.append(copy.deepcopy(ego_pred_obj))
                        out_grouped[kind][status][group].append((ref_t, msg_out))

                base_template_msg = base_template or ref_msg
                for status in COLLISION_BASE_STATUSES:
                    msg_out = _make_empty_object_set_like(base_template_msg, stamp)
                    if msg_out is None:
                        continue
                    if ego_observed_obj is not None:
                        if status == "ego_observed_collision" and stamp_has_collision:
                            msg_out.objects.append(copy.deepcopy(ego_observed_obj))
                        elif status == "ego_observed_safe" and (not stamp_has_collision):
                            msg_out.objects.append(copy.deepcopy(ego_observed_obj))
                    out_base[kind][status].append((ref_t, msg_out))

            _print_progress_line(
                f"[collision] {rel} {side}",
                idx,
                len(master_stamps),
                progress_state,
            )

    for kind in COLLISION_KINDS:
        summary[kind]["has_collision"] = bool(summary[kind]["collision_paths"] > 0)
        for sub in subscene_summaries:
            sub["collision"][kind]["has_collision"] = bool(sub["collision"][kind]["collision_paths"] > 0)

    bag_path = out_dir / f"collision_judgement_{side}.bag"
    if HAS_ROSBAG:
        total_write_steps = 0
        for kind in COLLISION_KINDS:
            for status in COLLISION_GROUPED_STATUSES:
                for group in VALIDATION_GROUPS:
                    total_write_steps += len(out_grouped[kind][status][group])
            for status in COLLISION_BASE_STATUSES:
                total_write_steps += len(out_base[kind][status])
        write_state = {"last_percent": -1}
        if total_write_steps > 0:
            _print_progress_line(
                f"[collision] {rel} {side} write",
                0,
                total_write_steps,
                write_state,
            )
        written = 0
        with rosbag.Bag(str(bag_path), "w", chunk_threshold=64 * 1024) as out_bag:
            for kind in COLLISION_KINDS:
                for status in COLLISION_GROUPED_STATUSES:
                    for group in VALIDATION_GROUPS:
                        topic = _make_collision_topic(side, kind, status, group)
                        for t, msg in out_grouped[kind][status][group]:
                            out_bag.write(topic, msg, t)
                            written += 1
                            if total_write_steps > 0:
                                _print_progress_line(
                                    f"[collision] {rel} {side} write",
                                    written,
                                    total_write_steps,
                                    write_state,
                                )
                for status in COLLISION_BASE_STATUSES:
                    topic = _make_collision_topic(side, kind, status)
                    for t, msg in out_base[kind][status]:
                        out_bag.write(topic, msg, t)
                        written += 1
                        if total_write_steps > 0:
                            _print_progress_line(
                                f"[collision] {rel} {side} write",
                                written,
                                total_write_steps,
                                write_state,
                            )
    elif not bag_path.exists():
        bag_path.write_bytes(b"")

    return summary, subscene_summaries


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
) -> tuple[dict, dict[str, dict[str, list[tuple]]], list[dict]]:
    summary = _default_threshold_summary("ok")
    log_key = _validation_log_key(side, horizon, threshold)
    out_records = _empty_validation_out_records()
    subscene_summaries = _build_subscene_side_summaries(scene_timing, "ok")

    if not HAS_ROSBAG:
        summary["detail"] = "rosbag not available"
        for sub in subscene_summaries:
            sub[horizon][threshold]["detail"] = summary["detail"]
    elif not result_bag.exists():
        summary["detail"] = f"result bag missing: {result_bag}"
        for sub in subscene_summaries:
            sub[horizon][threshold]["detail"] = summary["detail"]
    elif not observed_bag.exists():
        summary["detail"] = f"observed bag missing: {observed_bag}"
        for sub in subscene_summaries:
            sub[horizon][threshold]["detail"] = summary["detail"]
    else:
        result_topics = _resolve_group_topics(result_bag, result_ns)
        observed_topics = _resolve_group_topics(observed_bag, observed_ns)
        result_by_group = _collect_group_messages_by_stamp(result_bag, result_topics)
        observed_by_group = _collect_group_messages_by_stamp(observed_bag, observed_topics)

        result_base_stamps = {
            stamp
            for stamp, record in result_by_group.get("base", {}).items()
            if is_stamp_in_evaluation_range(_to_nsec(record[0]), scene_timing)
        }
        observed_base_stamps = {
            stamp
            for stamp, record in observed_by_group.get("base", {}).items()
            if is_stamp_in_evaluation_range(_to_nsec(record[0]), scene_timing)
        }
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
                    sub_summary = _subscene_summary_for_stamp(_to_nsec(ref_t), scene_timing, subscene_summaries)
                    if sub_summary is not None:
                        sub_summary[horizon][threshold]["total"] += 1
                    if candidates:
                        summary["ok"] += 1
                        if sub_summary is not None:
                            sub_summary[horizon][threshold]["ok"] += 1
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

    for sub in subscene_summaries:
        sub_summary = sub[horizon][threshold]
        if sub_summary["total"] > 0:
            sub_summary["rate"] = (100.0 * float(sub_summary["ok"])) / float(sub_summary["total"])
        else:
            sub_summary["rate"] = 0.0

    return summary, out_records, subscene_summaries


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
    validation_bag_path = out_dir / f"validation_{side}.bag"
    validation_bag = None
    try:
        if HAS_ROSBAG:
            validation_bag = rosbag.Bag(str(validation_bag_path), "w", chunk_threshold=64 * 1024)
        for horizon in VALIDATION_HORIZONS:
            summary[horizon] = {}
            window_cfg = VALIDATION_HORIZON_WINDOWS.get(horizon, VALIDATION_HORIZON_WINDOWS["half-time-relaxed"])
            max_t = float(window_cfg.get("max_t", 5.0))
            other_max_t = float(window_cfg.get("other_max_t", max_t))
            for threshold in VALIDATION_THRESHOLDS:
                side_summary, out_records, subscene_threshold_summaries = _build_validation_for_side_threshold(
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
                )
                summary[horizon][threshold] = side_summary
                for idx, sub in enumerate(subscene_threshold_summaries):
                    if idx < len(summary["subscenes"]):
                        summary["subscenes"][idx][horizon][threshold] = sub[horizon][threshold]
                if validation_bag is not None:
                    _write_validation_records_to_bag(
                        rel=rel,
                        side=side,
                        horizon=horizon,
                        threshold=threshold,
                        out_records=out_records,
                        out_bag=validation_bag,
                    )
                log_key = _validation_log_key(side, horizon, threshold)
                print(
                    f"[validation] {rel} {log_key}: "
                    f"{side_summary['rate']:.1f}% ({side_summary['ok']}/{side_summary['total']})"
                )
    finally:
        if validation_bag is not None:
            validation_bag.close()
    if not HAS_ROSBAG and not validation_bag_path.exists():
        validation_bag_path.write_bytes(b"")
    collision_summary, collision_subscenes = _build_collision_for_side(
        rel=rel,
        out_dir=out_dir,
        side=side,
        result_bag=result_bag,
        observed_bag=observed_bag,
        result_ns=result_ns,
        observed_ns=observed_ns,
        scene_timing=scene_timing,
    )
    summary["collision"] = collision_summary
    for idx, sub in enumerate(collision_subscenes):
        if idx < len(summary["subscenes"]):
            summary["subscenes"][idx]["collision"] = sub["collision"]
    for kind in COLLISION_KINDS:
        node = collision_summary.get(kind, {})
        state = "Collision" if bool(node.get("has_collision")) else "Safe"
        print(
            f"[collision] {rel} {side}-{kind}: {state} "
            f"({int(node.get('collision_paths', 0))}/{int(node.get('checked_paths', 0))})"
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
