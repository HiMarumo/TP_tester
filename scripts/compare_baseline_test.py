#!/usr/bin/env python3
"""
Compare baseline vs test result bags and write comparison_direct.json per directory.
All comparisons are timestamp-aligned: only messages with the same header.stamp (or bag time)
are paired. Verifies:
  ① Lane IDs: 共通タイムスタンプ同士で曲線の集合が一致（曲線＝lane id の並び、格納順は不問）
  ② VSL: 共通タイムスタンプ同士で位置・向き集合が一致（object_id は比較に使わない）
  ③ Object IDs: 共通タイムスタンプでの path 付き object_id 集合が一致
  ④ Path / Traffic: 共通タイムスタンプ同士で軌跡・信号状態が一致
"""
from __future__ import annotations

import copy
import json
import math
import os
import sys
from collections import Counter
from pathlib import Path

from common import (
    build_scene_timing,
    collect_clock_stamps_in_bags,
    find_subscene_index,
    is_stamp_in_evaluation_range,
    VALIDATION_BASELINE_NS,
    VALIDATION_TEST_NS,
    discover_bag_directories,
    get_bag_files_in_dir,
    get_tester_root,
    load_settings,
)

# common / diff を表示で競合させないため別トピックで保存。common は baseline から 1 本のみ
VALIDATION_COMMON_NS = "/validation/common"
VALIDATION_DIFF_BASELINE_NS = "/validation/diff_baseline"
VALIDATION_DIFF_TEST_NS = "/validation/diff_test"

# Optional: use rosbag when running in ROS environment
try:
    import rosbag
    HAS_ROSBAG = True
except ImportError:
    HAS_ROSBAG = False

# Optional: for writing /clock at common stamps only
try:
    import rospy
    from rosgraph_msgs.msg import Clock
    HAS_CLOCK_MSG = True
except ImportError:
    HAS_CLOCK_MSG = False


LANE_TOPICS = [
    "/viz/su/multi_lane_ids_set",
    "/viz/su/crossing_lane_ids_set",
    "/viz/su/opposite_lane_ids_set",
]
WM_TOPICS = [
    "/WM/tracked_object_set_with_prediction",
    "/WM/along_object_set_with_prediction",
    "/WM/crossing_object_set_with_prediction",
    "/WM/oncoming_object_set_with_prediction",
    "/WM/other_object_set_with_prediction",
]
TRAFFIC_TOPIC = "/viz/su/ym0_converted_traffic_light_state"

LANE_SOURCES = ("along", "opposite", "crossing")
OBJECT_PATH_SOURCES = ("along", "opposite", "crossing", "other", "base")
VSL_SOURCES = ("along", "opposite", "crossing")
PATH_COMPARE_SOURCES = ("along", "opposite", "crossing", "other")

LANE_TOPIC_TO_SOURCE = {
    "/viz/su/multi_lane_ids_set": "along",
    "/viz/su/opposite_lane_ids_set": "opposite",
    "/viz/su/crossing_lane_ids_set": "crossing",
}
WM_TOPIC_TO_SOURCE = {
    "/WM/tracked_object_set_with_prediction": "base",
    "/WM/along_object_set_with_prediction": "along",
    # topic名は oncoming だが比較ソース名は opposite として扱う
    "/WM/oncoming_object_set_with_prediction": "opposite",
    "/WM/crossing_object_set_with_prediction": "crossing",
    "/WM/other_object_set_with_prediction": "other",
}

GROUP_TO_WM_SUFFIX = {
    "base": "/WM/tracked_object_set_with_prediction",
    "along": "/WM/along_object_set_with_prediction",
    "opposite": "/WM/oncoming_object_set_with_prediction",
    "crossing": "/WM/crossing_object_set_with_prediction",
    "other": "/WM/other_object_set_with_prediction",
}

# VSL-related object IDs (from trajectory_predictor_data_type.h / core)
VSL_STOPLINE_ID = 500001
VSL_CROSSING_MIN = 500100
VSL_ONCOMING_MIN = 500200


def _is_vsl_id(obj_id: int) -> bool:
    return obj_id == VSL_STOPLINE_ID or obj_id >= VSL_CROSSING_MIN


def _strip_validation_ns(topic: str) -> str:
    for ns in (
        VALIDATION_BASELINE_NS,
        VALIDATION_TEST_NS,
        VALIDATION_COMMON_NS,
        VALIDATION_DIFF_BASELINE_NS,
        VALIDATION_DIFF_TEST_NS,
    ):
        if topic.startswith(ns):
            return topic[len(ns):]
    return topic


def _lane_source_from_topic(topic: str) -> str:
    return LANE_TOPIC_TO_SOURCE.get(_strip_validation_ns(topic), "")


def _wm_source_from_topic(topic: str) -> str:
    return WM_TOPIC_TO_SOURCE.get(_strip_validation_ns(topic), "")


def _print_progress_line(prefix: str, done: int, total: int, state: dict) -> None:
    """Print single-line progress bar with carriage return, updating only when percent changes."""
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
    last_len = int(state.get("last_line_len", 0) or 0)
    pad = " " * max(0, last_len - len(line))
    state["last_line_len"] = len(line)
    print(
        f"\r{line}{pad}",
        end="",
        file=sys.stderr,
        flush=True,
    )
    if done >= total:
        state["last_line_len"] = 0
        print(file=sys.stderr, flush=True)


def _yaw_from_quat(q) -> float:
    """Extract yaw from geometry_msgs/Quaternion."""
    x = getattr(q, "x", 0) or 0
    y = getattr(q, "y", 0) or 0
    z = getattr(q, "z", 0) or 0
    w = getattr(q, "w", 1) or 1
    siny_cosp = 2 * (w * z + x * y)
    cosy_cosp = 1 - 2 * (y * y + z * z)
    return math.atan2(siny_cosp, cosy_cosp)


def _lane_curves_as_set(msg) -> set:
    """Int32MultiArraysStamped から「曲線の集合」を返す。
    - 1本の曲線 = lane id の並び（順序あり・つながりを持つ）→ tuple で保持し (1,2,3) と (3,2,1) は別物。
    - 曲線がメッセージ内のどの順で格納されているかは不問 → set で比較。"""
    arrays = getattr(msg, "arrays", []) or []
    return {tuple(getattr(a, "data", []) or []) for a in arrays}


def _lane_curves_sets_equal(msg1, msg2) -> bool:
    """2メッセージの「曲線の集合」が一致するか。各曲線は並びを保った tuple、曲線の格納順のみ不問。"""
    return _lane_curves_as_set(msg1) == _lane_curves_as_set(msg2)


def _collect_lane_messages(bag_path: Path, topics: list[str]) -> dict:
    """topic -> list of (t, msg)."""
    out = {t: [] for t in topics}
    if not HAS_ROSBAG or not bag_path.exists():
        return out
    with rosbag.Bag(str(bag_path), "r") as bag:
        for topic, msg, t in bag.read_messages(topics=topics):
            out[topic].append((t, msg))
    return out


def _collect_wm_messages(bag_path: Path, topics: list[str]) -> dict:
    out = {t: [] for t in topics}
    if not HAS_ROSBAG or not bag_path.exists():
        return out
    with rosbag.Bag(str(bag_path), "r") as bag:
        for topic, msg, t in bag.read_messages(topics=topics):
            out[topic].append((t, msg))
    return out


def _collect_traffic_messages(bag_path: Path, topic: str) -> list:
    out = []
    if not HAS_ROSBAG or not bag_path.exists():
        return out
    with rosbag.Bag(str(bag_path), "r") as bag:
        for _topic, msg, t in bag.read_messages(topics=[topic]):
            out.append((t, msg))
    return out


def _to_nsec(ts) -> int:
    """Normalize ROS Time or bag time to integer nanoseconds for alignment."""
    if ts is None:
        return 0
    if hasattr(ts, "to_nsec"):
        return int(ts.to_nsec())
    sec = getattr(ts, "secs", getattr(ts, "sec", 0)) or 0
    nsec = getattr(ts, "nsecs", getattr(ts, "nsec", getattr(ts, "nanosec", 0))) or 0
    return int(sec) * 10**9 + int(nsec)


def _msg_stamp(msg, bag_t) -> int:
    """Get canonical stamp (nsec) from message: header.stamp if present, else bag time."""
    if msg is None:
        return _to_nsec(bag_t)
    h = getattr(msg, "header", None)
    if h is not None:
        s = getattr(h, "stamp", None)
        if s is not None:
            return _to_nsec(s)
    return _to_nsec(bag_t)


def _list_by_stamp(items: list) -> dict:
    """[(t, msg), ...] -> {stamp_nsec: (t, msg)}. Duplicate stamps overwrite (last wins)."""
    out = {}
    for (t, msg) in items:
        out[_msg_stamp(msg, t)] = (t, msg)
    return out


def _object_ids_with_path(msg) -> set:
    """TrackedObjectSet2WithTrajectory から path を持つ object_id の集合を返す（並び・順序は無関係）。"""
    ids = set()
    for obj in getattr(msg, "objects", []) or []:
        oid = getattr(getattr(obj, "object", None), "object_id", None)
        if oid is None:
            continue
        traj_set = getattr(obj, "trajectory_set", []) or []
        has_path = False
        for path_msg in traj_set:
            if getattr(path_msg, "trajectory", []) or []:
                has_path = True
                break
        if has_path:
            ids.add(int(oid))
    return ids


def _object_ids_all(msg) -> set:
    """TrackedObjectSet2WithTrajectory から全 object_id の集合を返す。"""
    ids = set()
    for obj in getattr(msg, "objects", []) or []:
        oid = getattr(getattr(obj, "object", None), "object_id", None)
        if oid is None:
            continue
        ids.add(int(oid))
    return ids


def _vsl_pose_signature_from_obj(obj):
    """Return rounded (x, y, yaw) signature for VSL object, or None for non-VSL."""
    o = getattr(obj, "object", None)
    if o is None:
        return None
    oid = int(getattr(o, "object_id", -1))
    if not _is_vsl_id(oid):
        return None
    pose = getattr(o, "pose", None)
    if pose is None:
        return None
    pos = getattr(pose, "position", None)
    ori = getattr(pose, "orientation", None)
    x = getattr(pos, "x", 0) if pos else 0
    y = getattr(pos, "y", 0) if pos else 0
    yaw = _yaw_from_quat(ori) if ori else 0
    return (round(x, 5), round(y, 5), round(yaw, 5))


def _vsl_pose_counter(msg) -> Counter:
    """Multiset of VSL pose signatures (ID is intentionally ignored)."""
    out = Counter()
    for obj in getattr(msg, "objects", []) or []:
        sig = _vsl_pose_signature_from_obj(obj)
        if sig is not None:
            out[sig] += 1
    return out


def _trajectory_path_signature(path_msg) -> tuple:
    """Trajectory path signature as tuple[(t,x,y), ...].
    Path 内の点順序（時系列）は比較対象として保持する。"""
    sig = []
    for pt in getattr(path_msg, "trajectory", []) or []:
        t = getattr(pt, "t", 0)
        x = getattr(pt, "x", 0)
        y = getattr(pt, "y", 0)
        sig.append((round(t, 6), round(x, 6), round(y, 6)))
    return tuple(sig)


def _trajectory_counter(obj) -> Counter:
    """Multiset of path signatures in one object.
    - path 内の点順序は保持して比較
    - trajectory_set の path 並び順は不問（Counter 比較）"""
    cnt = Counter()
    for path_msg in getattr(obj, "trajectory_set", []) or []:
        if not (getattr(path_msg, "trajectory", []) or []):
            continue
        cnt[_trajectory_path_signature(path_msg)] += 1
    return cnt


def _object_path_entries_by_id(msg) -> dict:
    out = {}
    for obj in getattr(msg, "objects", []) or []:
        o = getattr(obj, "object", None)
        if o is None:
            continue
        oid = int(getattr(o, "object_id", -1))
        entries = []
        for path_msg in getattr(obj, "trajectory_set", []) or []:
            if not (getattr(path_msg, "trajectory", []) or []):
                continue
            entries.append((path_msg, _trajectory_path_signature(path_msg)))
        if entries:
            out[oid] = entries
    return out


def _filter_object_trajectories(obj, keep_counter: Counter):
    """Return deep-copied object that keeps only trajectory_set entries in keep_counter."""
    out = copy.deepcopy(obj)
    remaining = Counter(keep_counter)
    kept = []
    for path_msg in getattr(out, "trajectory_set", []) or []:
        sig = _trajectory_path_signature(path_msg)
        if remaining.get(sig, 0) > 0:
            kept.append(path_msg)
            remaining[sig] -= 1
    out.trajectory_set = kept
    return out


def _filter_object_trajectories_cached(obj, keep_counter: Counter, path_entries: list[tuple] | None = None):
    out = copy.deepcopy(obj)
    remaining = Counter(keep_counter)
    kept = []
    entries = path_entries
    if entries is None:
        entries = [
            (path_msg, _trajectory_path_signature(path_msg))
            for path_msg in getattr(obj, "trajectory_set", []) or []
            if getattr(path_msg, "trajectory", []) or []
        ]
    for path_msg, sig in entries:
        if remaining.get(sig, 0) > 0:
            kept.append(copy.deepcopy(path_msg))
            remaining[sig] -= 1
    out.trajectory_set = kept
    return out


def _objects_by_id(msg) -> dict:
    """object_id -> object in message. If duplicated IDs exist, last one wins."""
    out = {}
    for obj in getattr(msg, "objects", []) or []:
        oid = int(getattr(getattr(obj, "object", None), "object_id", -1))
        if oid >= 0:
            out[oid] = obj
    return out


def _object_trajectory_counters_by_id(msg) -> dict:
    """object_id -> Counter(path_signature)."""
    out = {}
    for obj in getattr(msg, "objects", []) or []:
        o = getattr(obj, "object", None)
        if o is None:
            continue
        oid = int(getattr(o, "object_id", -1))
        traj_counter = _trajectory_counter(obj)
        if traj_counter:
            out[oid] = traj_counter
    return out


def _traffic_state_signature(msg) -> list:
    """List of (stoplineid, state) for comparison, sorted by stoplineid for stable comparison."""
    sig = []
    for e in getattr(msg, "trafficlightlist", []) or []:
        stoplineid = getattr(e, "stoplineid", None)
        state = getattr(e, "state", None)
        sig.append((stoplineid, state))
    return sorted(sig, key=lambda x: (x[0] is None, x[0]))


def _count_clock_in_bags(
    bag_paths: list[Path],
    scene_timing: dict | None = None,
) -> int | None:
    """入力 bag 群の /clock メッセージ総数（再生時の発火回数＝全体フレーム数）。取れなければ None。"""
    if not HAS_ROSBAG or not bag_paths:
        return None
    total = 0
    for bag_path in bag_paths:
        if not bag_path.exists():
            continue
        try:
            with rosbag.Bag(str(bag_path), "r") as bag:
                info = bag.get_type_and_topic_info()
                topics = info[1] if isinstance(info, tuple) and len(info) >= 2 else getattr(info, "topics", {})
                if "/clock" not in topics:
                    continue
                for _topic, _msg, bag_t in bag.read_messages(topics=["/clock"]):
                    if not is_stamp_in_evaluation_range(_to_nsec(bag_t), scene_timing):
                        continue
                    total += 1
        except Exception:
            continue
    return total if total > 0 else None


def _build_compare_scene_cache(baseline_bag: Path, test_bag: Path) -> dict:
    def _bag_has_namespaced(bag_path: Path, ns: str) -> bool:
        if not HAS_ROSBAG:
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
                return any(topic.startswith(ns) for topic in topics)
        except Exception:
            return False

    baseline_use_ns = _bag_has_namespaced(baseline_bag, VALIDATION_BASELINE_NS)
    test_use_ns = _bag_has_namespaced(test_bag, VALIDATION_TEST_NS)
    baseline_lane = [f"{VALIDATION_BASELINE_NS}{t}" for t in LANE_TOPICS] if baseline_use_ns else list(LANE_TOPICS)
    baseline_wm = [f"{VALIDATION_BASELINE_NS}{t}" for t in WM_TOPICS] if baseline_use_ns else list(WM_TOPICS)
    baseline_traffic = f"{VALIDATION_BASELINE_NS}{TRAFFIC_TOPIC}" if baseline_use_ns else TRAFFIC_TOPIC
    test_lane = [f"{VALIDATION_TEST_NS}{t}" for t in LANE_TOPICS] if test_use_ns else list(LANE_TOPICS)
    test_wm = [f"{VALIDATION_TEST_NS}{t}" for t in WM_TOPICS] if test_use_ns else list(WM_TOPICS)
    test_traffic = f"{VALIDATION_TEST_NS}{TRAFFIC_TOPIC}" if test_use_ns else TRAFFIC_TOPIC

    bl_lane = _collect_lane_messages(baseline_bag, baseline_lane)
    te_lane = _collect_lane_messages(test_bag, test_lane)
    bl_wm = _collect_wm_messages(baseline_bag, baseline_wm)
    te_wm = _collect_wm_messages(test_bag, test_wm)
    bl_traffic = _collect_traffic_messages(baseline_bag, baseline_traffic)
    te_traffic = _collect_traffic_messages(test_bag, test_traffic)

    bl_wm_by = {topic: _list_by_stamp(bl_wm.get(topic, [])) for topic in baseline_wm}
    te_wm_by = {topic: _list_by_stamp(te_wm.get(topic, [])) for topic in test_wm}
    bl_lane_by = {topic: _list_by_stamp(bl_lane.get(topic, [])) for topic in baseline_lane}
    te_lane_by = {topic: _list_by_stamp(te_lane.get(topic, [])) for topic in test_lane}
    bl_traffic_by = _list_by_stamp(bl_traffic)
    te_traffic_by = _list_by_stamp(te_traffic)

    wm_features = {"baseline": {}, "test": {}}
    for side_name, topics, by_topic in (
        ("baseline", baseline_wm, bl_wm_by),
        ("test", test_wm, te_wm_by),
    ):
        for topic in topics:
            source = _wm_source_from_topic(topic)
            topic_map = {}
            for stamp, (t, msg) in by_topic.get(topic, {}).items():
                topic_map[stamp] = {
                    "t": t,
                    "msg": msg,
                    "source": source,
                    "object_ids_with_path": _object_ids_with_path(msg),
                    "object_ids_all": _object_ids_all(msg),
                    "objects_by_id": _objects_by_id(msg),
                    "path_entries_by_id": _object_path_entries_by_id(msg),
                    "trajectory_counters_by_id": _object_trajectory_counters_by_id(msg),
                    "vsl_pose_counter": _vsl_pose_counter(msg),
                }
            wm_features[side_name][topic] = topic_map

    lane_features = {"baseline": {}, "test": {}}
    for side_name, topics, by_topic in (
        ("baseline", baseline_lane, bl_lane_by),
        ("test", test_lane, te_lane_by),
    ):
        for topic in topics:
            topic_map = {}
            for stamp, (t, msg) in by_topic.get(topic, {}).items():
                topic_map[stamp] = {
                    "t": t,
                    "msg": msg,
                    "curves": _lane_curves_as_set(msg),
                }
            lane_features[side_name][topic] = topic_map

    traffic_features = {"baseline": {}, "test": {}}
    for side_name, by_stamp in (("baseline", bl_traffic_by), ("test", te_traffic_by)):
        for stamp, (t, msg) in by_stamp.items():
            traffic_features[side_name][stamp] = {
                "t": t,
                "msg": msg,
                "signature": _traffic_state_signature(msg),
            }

    return {
        "baseline_lane_topics": baseline_lane,
        "baseline_wm_topics": baseline_wm,
        "baseline_traffic_topic": baseline_traffic,
        "test_lane_topics": test_lane,
        "test_wm_topics": test_wm,
        "test_traffic_topic": test_traffic,
        "bl_wm_by": bl_wm_by,
        "te_wm_by": te_wm_by,
        "bl_lane_by": bl_lane_by,
        "te_lane_by": te_lane_by,
        "bl_traffic_by": bl_traffic_by,
        "te_traffic_by": te_traffic_by,
        "wm_features": wm_features,
        "lane_features": lane_features,
        "traffic_features": traffic_features,
    }


def _empty_compare_diff_plan(cache: dict, common_stamps_sorted: list[int]) -> dict:
    baseline_wm = cache["baseline_wm_topics"]
    test_wm = cache["test_wm_topics"]
    baseline_lane = cache["baseline_lane_topics"]
    test_lane = cache["test_lane_topics"]
    return {
        "common_stamps_sorted": list(common_stamps_sorted),
        "wm_common_obj": {topic: {} for topic in baseline_wm},
        "wm_common_traj_keep": {topic: {} for topic in baseline_wm},
        "wm_common_vsl_keep": {topic: {} for topic in baseline_wm},
        "wm_diff_b": {topic: {} for topic in baseline_wm},
        "wm_diff_t": {topic: {} for topic in test_wm},
        "wm_diff_b_traj_keep": {topic: {} for topic in baseline_wm},
        "wm_diff_t_traj_keep": {topic: {} for topic in test_wm},
        "wm_diff_b_vsl_keep": {topic: {} for topic in baseline_wm},
        "wm_diff_t_vsl_keep": {topic: {} for topic in test_wm},
        "lane_common": {topic: {} for topic in baseline_lane},
        "lane_diff_b": {topic: {} for topic in baseline_lane},
        "lane_diff_t": {topic: {} for topic in test_lane},
    }


def _build_diff_plan_from_cache(cache: dict) -> dict:
    baseline_lane = cache["baseline_lane_topics"]
    baseline_wm = cache["baseline_wm_topics"]
    test_lane = cache["test_lane_topics"]
    test_wm = cache["test_wm_topics"]
    bl_wm_by = cache["bl_wm_by"]
    te_wm_by = cache["te_wm_by"]
    wm_features = cache["wm_features"]
    lane_features = cache["lane_features"]
    common_stamps_sorted = sorted(set(bl_wm_by.get(baseline_wm[0], {}).keys()) & set(te_wm_by.get(test_wm[0], {}).keys()))
    diff_plan = _empty_compare_diff_plan(cache, common_stamps_sorted)

    for topic_b, topic_t in zip(baseline_lane, test_lane):
        bl_by = lane_features["baseline"].get(topic_b, {})
        te_by = lane_features["test"].get(topic_t, {})
        for stamp in common_stamps_sorted:
            if stamp not in bl_by or stamp not in te_by:
                continue
            bl_curves = bl_by[stamp]["curves"]
            te_curves = te_by[stamp]["curves"]
            diff_plan["lane_common"][topic_b][stamp] = bl_curves & te_curves
            diff_plan["lane_diff_b"][topic_b][stamp] = bl_curves - te_curves
            diff_plan["lane_diff_t"][topic_t][stamp] = te_curves - bl_curves

    for topic_b, topic_t in zip(baseline_wm, test_wm):
        source = _wm_source_from_topic(topic_b) or _wm_source_from_topic(topic_t)
        bl_by = wm_features["baseline"].get(topic_b, {})
        te_by = wm_features["test"].get(topic_t, {})
        for stamp in common_stamps_sorted:
            if stamp not in bl_by or stamp not in te_by:
                continue
            feat_b = bl_by[stamp]
            feat_t = te_by[stamp]
            if source == "base":
                ids_b = feat_b["object_ids_all"]
                ids_t = feat_t["object_ids_all"]
            else:
                ids_b = feat_b["object_ids_with_path"]
                ids_t = feat_t["object_ids_with_path"]
            common_ids = ids_b & ids_t
            diff_plan["wm_common_obj"][topic_b][stamp] = set()
            diff_plan["wm_common_traj_keep"][topic_b][stamp] = {}
            diff_plan["wm_common_vsl_keep"][topic_b][stamp] = Counter()
            diff_plan["wm_diff_b"][topic_b][stamp] = ids_b - ids_t
            diff_plan["wm_diff_t"][topic_t][stamp] = ids_t - ids_b
            diff_plan["wm_diff_b_traj_keep"][topic_b][stamp] = {}
            diff_plan["wm_diff_t_traj_keep"][topic_t][stamp] = {}
            diff_plan["wm_diff_b_vsl_keep"][topic_b][stamp] = Counter()
            diff_plan["wm_diff_t_vsl_keep"][topic_t][stamp] = Counter()

            b_map = feat_b["objects_by_id"]
            t_map = feat_t["objects_by_id"]
            for oid in common_ids:
                if oid not in b_map or oid not in t_map:
                    continue
                b_counter = feat_b["trajectory_counters_by_id"].get(oid, Counter())
                t_counter = feat_t["trajectory_counters_by_id"].get(oid, Counter())
                if not b_counter and not t_counter:
                    diff_plan["wm_common_obj"][topic_b][stamp].add(oid)
                    continue
                common_counter = b_counter & t_counter
                b_only_counter = b_counter - t_counter
                t_only_counter = t_counter - b_counter
                if b_only_counter or t_only_counter:
                    if b_only_counter:
                        diff_plan["wm_diff_b_traj_keep"][topic_b][stamp][oid] = b_only_counter
                    if t_only_counter:
                        diff_plan["wm_diff_t_traj_keep"][topic_t][stamp][oid] = t_only_counter
                    if common_counter:
                        diff_plan["wm_common_traj_keep"][topic_b][stamp][oid] = common_counter
                else:
                    diff_plan["wm_common_obj"][topic_b][stamp].add(oid)

            if source in VSL_SOURCES:
                b_vsl_counter = feat_b["vsl_pose_counter"]
                t_vsl_counter = feat_t["vsl_pose_counter"]
                diff_plan["wm_common_vsl_keep"][topic_b][stamp] = b_vsl_counter & t_vsl_counter
                diff_plan["wm_diff_b_vsl_keep"][topic_b][stamp] = b_vsl_counter - t_vsl_counter
                diff_plan["wm_diff_t_vsl_keep"][topic_t][stamp] = t_vsl_counter - b_vsl_counter

    return diff_plan


def _empty_direct_compare_result(rel: str) -> dict:
    return {
        "directory": rel,
        "lane_ids_ok": None,
        "lane_ids_detail": "",
        "vsl_ok": None,
        "vsl_detail": "",
        "object_ids_ok": None,
        "object_ids_detail": "",
        "path_and_traffic_ok": None,
        "path_and_traffic_detail": "",
        "path_ok": None,
        "traffic_ok": None,
        "overall_ok": False,
        "diff_by_source": {
            "lane": {k: False for k in LANE_SOURCES},
            "vsl": {k: False for k in VSL_SOURCES},
            "object_ids": {k: False for k in OBJECT_PATH_SOURCES},
            "path": {k: False for k in OBJECT_PATH_SOURCES},
        },
        "diff_counts_by_source": {
            "lane": {k: 0 for k in LANE_SOURCES},
            "vsl": {k: 0 for k in VSL_SOURCES},
            "object_ids": {k: 0 for k in OBJECT_PATH_SOURCES},
            "path": {k: 0 for k in OBJECT_PATH_SOURCES},
        },
    }


def _make_subscene_compare_entry(base_subscene: dict) -> dict:
    entry = _empty_direct_compare_result(str(base_subscene.get("label", "")))
    entry["index"] = int(base_subscene.get("index", 0) or 0)
    entry["label"] = str(base_subscene.get("label", ""))
    entry["start_offset_sec"] = float(base_subscene.get("start_offset_sec", 0.0) or 0.0)
    entry["end_offset_sec"] = float(base_subscene.get("end_offset_sec", entry["start_offset_sec"]) or entry["start_offset_sec"])
    entry["duration_sec"] = float(base_subscene.get("duration_sec", 0.0) or 0.0)
    entry["directory"] = None
    entry["lane_ids_ok"] = True
    entry["vsl_ok"] = True
    entry["object_ids_ok"] = True
    entry["path_and_traffic_ok"] = True
    entry["path_ok"] = True
    entry["traffic_ok"] = True
    entry["overall_ok"] = True
    return entry


def _finalize_compare_summary(summary: dict) -> None:
    lane_ok = bool(summary.get("lane_ids_ok"))
    vsl_ok = bool(summary.get("vsl_ok"))
    obj_ok = bool(summary.get("object_ids_ok"))
    path_ok = bool(summary.get("path_ok"))
    traffic_ok = bool(summary.get("traffic_ok"))
    summary["path_and_traffic_ok"] = bool(path_ok and traffic_ok)
    summary["overall_ok"] = bool(lane_ok and vsl_ok and obj_ok and path_ok and traffic_ok)


def _load_dt_summary(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _merge_dt_summary_into_result(result: dict, side: str, summary: dict | None) -> None:
    if not isinstance(summary, dict):
        return
    result[f"dt_status_{side}"] = str(summary.get("dt_status") or result.get(f"dt_status_{side}") or "valid")
    result[f"dt_max_{side}"] = str(summary.get("dt_max") or result.get(f"dt_max_{side}") or "-")

    sub_by_index: dict[int, dict] = {}
    for idx, sub in enumerate(result.get("subscenes", []) or []):
        if not isinstance(sub, dict):
            continue
        sub_by_index[int(sub.get("index", idx) or idx)] = sub

    for idx, sub_summary in enumerate(summary.get("subscenes", []) or []):
        if not isinstance(sub_summary, dict):
            continue
        sub_idx = int(sub_summary.get("index", idx) or idx)
        sub = sub_by_index.get(sub_idx)
        if sub is None:
            continue
        sub[f"dt_status_{side}"] = str(sub_summary.get("dt_status") or "valid")
        sub[f"dt_max_{side}"] = str(sub_summary.get("dt_max") or "-")


def compare_one(
    rel: str,
    baseline_bag: Path,
    test_bag: Path,
    input_bags: list[Path] | None = None,
    scene_cache: dict | None = None,
) -> dict:
    """Run all four checks for one directory. Returns result dict."""
    result = _empty_direct_compare_result(rel)
    scene_timing = build_scene_timing(input_bags or [])
    result["scene_timing"] = scene_timing
    result["subscenes"] = [
        _make_subscene_compare_entry(sub)
        for sub in ((scene_timing or {}).get("subscenes", []) or [])
        if isinstance(sub, dict)
    ]
    clock_stamps = collect_clock_stamps_in_bags(input_bags or []) if input_bags else []
    if clock_stamps:
        clock_stamps = [
            stamp for stamp in clock_stamps
            if is_stamp_in_evaluation_range(stamp, scene_timing)
        ]

    def _subscene_entry_for_stamp(stamp_ns: int) -> dict | None:
        bag_ref = bl_ref_by.get(stamp_ns) or te_ref_by.get(stamp_ns)
        stamp_for_scene = _to_nsec(bag_ref[0]) if bag_ref is not None else int(stamp_ns)
        subscene_index = find_subscene_index(stamp_for_scene, scene_timing)
        if subscene_index is None:
            return None
        if not (0 <= subscene_index < len(result["subscenes"])):
            return None
        return result["subscenes"][subscene_index]

    if not HAS_ROSBAG:
        result["lane_ids_detail"] = "rosbag not available (run in ROS env)"
        result["vsl_detail"] = result["object_ids_detail"] = result["path_and_traffic_detail"] = result["lane_ids_detail"]
        return result

    if not baseline_bag.exists():
        result["lane_ids_detail"] = f"baseline bag missing: {baseline_bag}"
        return result
    if not test_bag.exists():
        result["lane_ids_detail"] = f"test bag missing: {test_bag}"
        return result

    cache = scene_cache or _build_compare_scene_cache(baseline_bag, test_bag)
    baseline_lane = cache["baseline_lane_topics"]
    baseline_wm = cache["baseline_wm_topics"]
    baseline_traffic = cache["baseline_traffic_topic"]
    test_lane = cache["test_lane_topics"]
    test_wm = cache["test_wm_topics"]
    test_traffic = cache["test_traffic_topic"]
    bl_wm_by = cache["bl_wm_by"]
    te_wm_by = cache["te_wm_by"]
    bl_lane_by = cache["bl_lane_by"]
    te_lane_by = cache["te_lane_by"]
    bl_traffic_by = cache["bl_traffic_by"]
    te_traffic_by = cache["te_traffic_by"]
    wm_features = cache["wm_features"]
    lane_features = cache["lane_features"]
    traffic_features = cache["traffic_features"]

    ref_topic_b = baseline_wm[0]
    ref_topic_t = test_wm[0]
    bl_ref_by = bl_wm_by[ref_topic_b]
    te_ref_by = te_wm_by[ref_topic_t]
    bl_ref_stamps = {
        stamp for stamp, bag_ref in bl_ref_by.items()
        if is_stamp_in_evaluation_range(_to_nsec(bag_ref[0]), scene_timing)
    }
    te_ref_stamps = {
        stamp for stamp, bag_ref in te_ref_by.items()
        if is_stamp_in_evaluation_range(_to_nsec(bag_ref[0]), scene_timing)
    }
    common_ref_stamps = bl_ref_stamps & te_ref_stamps
    only_baseline_stamps = sorted(bl_ref_stamps - te_ref_stamps)
    only_test_stamps = sorted(te_ref_stamps - bl_ref_stamps)
    result["record_frames_baseline"] = len(bl_ref_stamps)
    result["record_frames_test"] = len(te_ref_stamps)
    result["valid_frames"] = len(common_ref_stamps)
    result["record_stamps_only_baseline_count"] = len(only_baseline_stamps)
    result["record_stamps_only_test_count"] = len(only_test_stamps)
    result["record_stamps_only_baseline"] = only_baseline_stamps[:100]
    result["record_stamps_only_test"] = only_test_stamps[:100]
    result["record_baseline_topic_counts"] = {t: len(bl_wm_by.get(t, {})) for t in baseline_wm}
    result["record_test_topic_counts"] = {t: len(te_wm_by.get(t, {})) for t in test_wm}
    # 全体 = publish 発火回数（記録メッセージ数）。入力 /clock が取れない場合は ref トピックのメッセージ数を使う
    total_from_input = _count_clock_in_bags(input_bags, scene_timing=scene_timing) if input_bags else None
    msg_count_ref = max(len(bl_ref_stamps), len(te_ref_stamps))
    result["total_frames"] = total_from_input if total_from_input is not None else (msg_count_ref if msg_count_ref > 0 else len(bl_ref_stamps | te_ref_stamps))
    for stamp in bl_ref_stamps:
        sub = _subscene_entry_for_stamp(stamp)
        if sub is not None:
            sub["record_frames_baseline"] = int(sub.get("record_frames_baseline", 0) or 0) + 1
    for stamp in te_ref_stamps:
        sub = _subscene_entry_for_stamp(stamp)
        if sub is not None:
            sub["record_frames_test"] = int(sub.get("record_frames_test", 0) or 0) + 1
    for stamp in common_ref_stamps:
        sub = _subscene_entry_for_stamp(stamp)
        if sub is not None:
            sub["valid_frames"] = int(sub.get("valid_frames", 0) or 0) + 1
    if clock_stamps:
        for stamp in clock_stamps:
            sub = _subscene_entry_for_stamp(stamp)
            if sub is not None:
                sub["total_frames"] = int(sub.get("total_frames", 0) or 0) + 1
    else:
        union_ref_stamps = bl_ref_stamps | te_ref_stamps
        for stamp in union_ref_stamps:
            sub = _subscene_entry_for_stamp(stamp)
            if sub is not None:
                sub["total_frames"] = int(sub.get("total_frames", 0) or 0) + 1

    # 比較は「共通スタンプ」のみ。lane/WM/traffic ともこのスタンプ集合で揃える（別トピックの stamp で別フレームを比較しない）
    common_sorted = sorted(common_ref_stamps)
    diff_plan = _empty_compare_diff_plan(cache, common_sorted)
    lane_steps_total = len(common_sorted) * len(baseline_lane)
    wm_steps_total = len(common_sorted) * len(baseline_wm)
    traffic_steps_total = len(common_sorted)
    compare_steps_total = lane_steps_total + wm_steps_total + traffic_steps_total
    compare_steps_done = 0
    compare_progress_state = {"last_percent": -1}

    def _tick_compare_progress() -> None:
        nonlocal compare_steps_done
        if compare_steps_total <= 0:
            return
        compare_steps_done += 1
        _print_progress_line(
            f"[compare] {rel} progress",
            compare_steps_done,
            compare_steps_total,
            compare_progress_state,
        )

    if compare_steps_total > 0:
        _print_progress_line(
            f"[compare] {rel} progress",
            0,
            compare_steps_total,
            compare_progress_state,
        )

    # ① Lane IDs: 共通スタンプのときだけ両方にあれば比較
    lane_ok = True
    for topic_b, topic_t in zip(baseline_lane, test_lane):
        source = _lane_source_from_topic(topic_b) or _lane_source_from_topic(topic_t)
        bl_by = lane_features["baseline"].get(topic_b, {})
        te_by = lane_features["test"].get(topic_t, {})
        for stamp in common_sorted:
            if stamp not in bl_by or stamp not in te_by:
                _tick_compare_progress()
                continue
            bl_curves = bl_by[stamp]["curves"]
            te_curves = te_by[stamp]["curves"]
            diff_plan["lane_common"][topic_b][stamp] = bl_curves & te_curves
            diff_plan["lane_diff_b"][topic_b][stamp] = bl_curves - te_curves
            diff_plan["lane_diff_t"][topic_t][stamp] = te_curves - bl_curves
            if bl_curves != te_curves:
                result["lane_ids_detail"] += f"{topic_b} at stamp {stamp} curve set differs. "
                lane_ok = False
                if source in LANE_SOURCES:
                    result["diff_by_source"]["lane"][source] = True
                    result["diff_counts_by_source"]["lane"][source] += 1
                    sub = _subscene_entry_for_stamp(stamp)
                    if sub is not None:
                        sub["lane_ids_ok"] = False
                        sub["diff_by_source"]["lane"][source] = True
                        sub["diff_counts_by_source"]["lane"][source] += 1
            _tick_compare_progress()
    result["lane_ids_ok"] = lane_ok
    if not result["lane_ids_detail"]:
        result["lane_ids_detail"] = "Unchanged"

    obj_ids_union_b = {k: set() for k in OBJECT_PATH_SOURCES}
    obj_ids_union_t = {k: set() for k in OBJECT_PATH_SOURCES}
    subscene_obj_ids_union_b = [{k: set() for k in OBJECT_PATH_SOURCES} for _ in result["subscenes"]]
    subscene_obj_ids_union_t = [{k: set() for k in OBJECT_PATH_SOURCES} for _ in result["subscenes"]]
    vsl_ok = True
    path_ok = True
    traffic_ok = True

    # ② ③ ④ WM: 共通スタンプのときだけ比較
    for topic_b, topic_t in zip(baseline_wm, test_wm):
        source = _wm_source_from_topic(topic_b) or _wm_source_from_topic(topic_t)
        bl_by = wm_features["baseline"].get(topic_b, {})
        te_by = wm_features["test"].get(topic_t, {})
        for stamp in common_sorted:
            if stamp not in bl_by or stamp not in te_by:
                _tick_compare_progress()
                continue
            feat_b = bl_by[stamp]
            feat_t = te_by[stamp]
            ids_b = feat_b["object_ids_with_path"]
            ids_t = feat_t["object_ids_with_path"]
            ids_all_b = feat_b["object_ids_all"]
            ids_all_t = feat_t["object_ids_all"]
            if source == "base":
                diff_ids_b = ids_all_b
                diff_ids_t = ids_all_t
            else:
                diff_ids_b = ids_b
                diff_ids_t = ids_t
            common_ids = diff_ids_b & diff_ids_t
            diff_plan["wm_common_obj"][topic_b][stamp] = set()
            diff_plan["wm_common_traj_keep"][topic_b][stamp] = {}
            diff_plan["wm_common_vsl_keep"][topic_b][stamp] = Counter()
            diff_plan["wm_diff_b"][topic_b][stamp] = diff_ids_b - diff_ids_t
            diff_plan["wm_diff_t"][topic_t][stamp] = diff_ids_t - diff_ids_b
            diff_plan["wm_diff_b_traj_keep"][topic_b][stamp] = {}
            diff_plan["wm_diff_t_traj_keep"][topic_t][stamp] = {}
            diff_plan["wm_diff_b_vsl_keep"][topic_b][stamp] = Counter()
            diff_plan["wm_diff_t_vsl_keep"][topic_t][stamp] = Counter()
            if source in OBJECT_PATH_SOURCES:
                obj_ids_union_b[source] |= ids_b
                obj_ids_union_t[source] |= ids_t
                sub_idx = find_subscene_index(stamp, scene_timing)
                if sub_idx is not None and 0 <= sub_idx < len(result["subscenes"]):
                    subscene_obj_ids_union_b[sub_idx][source] |= ids_b
                    subscene_obj_ids_union_t[sub_idx][source] |= ids_t
                if ids_b != ids_t:
                    result["diff_by_source"]["object_ids"][source] = True
                    result["diff_counts_by_source"]["object_ids"][source] += 1
                    result["object_ids_detail"] += (
                        f"{topic_b} object ID set differs (stamp={stamp}). "
                    )
                    sub = _subscene_entry_for_stamp(stamp)
                    if sub is not None:
                        sub["object_ids_ok"] = False
                        sub["diff_by_source"]["object_ids"][source] = True
                        sub["diff_counts_by_source"]["object_ids"][source] += 1
            vb = feat_b["vsl_pose_counter"]
            vt = feat_t["vsl_pose_counter"]
            if source in VSL_SOURCES:
                if vb != vt:
                    result["vsl_detail"] += f"{topic_b} VSL pose set differs (stamp={stamp}). "
                    vsl_ok = False
                    result["diff_by_source"]["vsl"][source] = True
                    result["diff_counts_by_source"]["vsl"][source] += 1
                    sub = _subscene_entry_for_stamp(stamp)
                    if sub is not None:
                        sub["vsl_ok"] = False
                        sub["diff_by_source"]["vsl"][source] = True
                        sub["diff_counts_by_source"]["vsl"][source] += 1
                diff_plan["wm_common_vsl_keep"][topic_b][stamp] = vb & vt
                diff_plan["wm_diff_b_vsl_keep"][topic_b][stamp] = vb - vt
                diff_plan["wm_diff_t_vsl_keep"][topic_t][stamp] = vt - vb
            tb = feat_b["trajectory_counters_by_id"]
            tt = feat_t["trajectory_counters_by_id"]
            for oid in common_ids:
                if oid not in feat_b["objects_by_id"] or oid not in feat_t["objects_by_id"]:
                    continue
                b_counter = tb.get(oid, Counter())
                t_counter = tt.get(oid, Counter())
                if not b_counter and not t_counter:
                    diff_plan["wm_common_obj"][topic_b][stamp].add(oid)
                    continue
                common_counter = b_counter & t_counter
                b_only_counter = b_counter - t_counter
                t_only_counter = t_counter - b_counter
                if b_only_counter or t_only_counter:
                    if b_only_counter:
                        diff_plan["wm_diff_b_traj_keep"][topic_b][stamp][oid] = b_only_counter
                    if t_only_counter:
                        diff_plan["wm_diff_t_traj_keep"][topic_t][stamp][oid] = t_only_counter
                    if common_counter:
                        diff_plan["wm_common_traj_keep"][topic_b][stamp][oid] = common_counter
                else:
                    diff_plan["wm_common_obj"][topic_b][stamp].add(oid)
            if source in PATH_COMPARE_SOURCES:
                for oid in set(tb) | set(tt):
                    # object ごとに path 集合（順不同）を比較。
                    # 各 path 内の点順序は signature に保持されるため順序一致必須。
                    if tb.get(oid, Counter()) != tt.get(oid, Counter()):
                        result["path_and_traffic_detail"] += (
                            f"{topic_b} object {oid} path set differs (stamp={stamp}). "
                        )
                        path_ok = False
                        result["diff_by_source"]["path"][source] = True
                        result["diff_counts_by_source"]["path"][source] += 1
                        sub = _subscene_entry_for_stamp(stamp)
                        if sub is not None:
                            sub["path_ok"] = False
                            sub["diff_by_source"]["path"][source] = True
                            sub["diff_counts_by_source"]["path"][source] += 1
            _tick_compare_progress()

    result["object_ids_ok"] = not any(result["diff_by_source"]["object_ids"].values())
    for source in OBJECT_PATH_SOURCES:
        if obj_ids_union_b[source] != obj_ids_union_t[source]:
            result["diff_by_source"]["object_ids"][source] = True
            if result["diff_counts_by_source"]["object_ids"][source] == 0:
                result["diff_counts_by_source"]["object_ids"][source] = 1
            result["object_ids_ok"] = False
        for sub_idx, sub in enumerate(result["subscenes"]):
            if subscene_obj_ids_union_b[sub_idx][source] != subscene_obj_ids_union_t[sub_idx][source]:
                sub["diff_by_source"]["object_ids"][source] = True
                if sub["diff_counts_by_source"]["object_ids"][source] == 0:
                    sub["diff_counts_by_source"]["object_ids"][source] = 1
                sub["object_ids_ok"] = False
    if not result["object_ids_ok"]:
        by_source = []
        for source in OBJECT_PATH_SOURCES:
            if result["diff_by_source"]["object_ids"][source]:
                by_source.append(source)
        result["object_ids_detail"] += f"Object ID mismatch sources: {by_source}. "
    result["vsl_ok"] = vsl_ok
    if not result["vsl_detail"]:
        result["vsl_detail"] = "Unchanged"
    if not result["object_ids_detail"]:
        result["object_ids_detail"] = "Unchanged"

    # Traffic light: 共通スタンプのときだけ両方にあれば比較
    for stamp in common_sorted:
        if stamp not in traffic_features["baseline"] or stamp not in traffic_features["test"]:
            _tick_compare_progress()
            continue
        if traffic_features["baseline"][stamp]["signature"] != traffic_features["test"][stamp]["signature"]:
            result["path_and_traffic_detail"] += f"traffic_light_state at stamp {stamp} differs. "
            traffic_ok = False
            sub = _subscene_entry_for_stamp(stamp)
            if sub is not None:
                sub["traffic_ok"] = False
        _tick_compare_progress()
    result["path_ok"] = path_ok
    result["traffic_ok"] = traffic_ok
    result["path_and_traffic_ok"] = path_ok and traffic_ok
    if not result["path_and_traffic_detail"]:
        result["path_and_traffic_detail"] = "Unchanged"

    _finalize_compare_summary(result)
    for sub in result["subscenes"]:
        if not sub.get("lane_ids_detail"):
            sub["lane_ids_detail"] = "Unchanged"
        if not sub.get("vsl_detail"):
            sub["vsl_detail"] = "Unchanged"
        if not sub.get("object_ids_detail"):
            sub["object_ids_detail"] = "Unchanged"
        if not sub.get("path_and_traffic_detail"):
            sub["path_and_traffic_detail"] = "Unchanged"
        _finalize_compare_summary(sub)
    cache["diff_plan"] = diff_plan
    return result


def _write_diff_bags(
    baseline_bag: Path,
    test_bag: Path,
    out_dir: Path,
    scene_cache: dict | None = None,
) -> None:
    """Write diff_baseline.bag (baseline-only content) and diff_test.bag (test-only content) to out_dir."""
    if not HAS_ROSBAG:
        print("[compare] diff bags: rosbag が import できません。diff_*.bag は作成されません。"
              " ROS の devel/setup.bash を source してから compare を実行してください。", file=sys.stderr)
        return
    if not baseline_bag.exists():
        print(f"[compare] diff bags: baseline がありません: {baseline_bag}", file=sys.stderr)
        return
    if not test_bag.exists():
        print(f"[compare] diff bags: test がありません: {test_bag}", file=sys.stderr)
        return
    try:
        with rosbag.Bag(str(baseline_bag), "r") as _:
            pass
    except Exception as e:
        print(f"[compare] diff bags: baseline bag を開けません: {e}", file=sys.stderr)
        return

    try:
        _write_diff_bags_impl(baseline_bag, test_bag, out_dir, scene_cache=scene_cache)
    except Exception as e:
        import traceback
        print(f"[compare] diff bags 作成失敗: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return

    out_dir.mkdir(parents=True, exist_ok=True)
    diff_baseline_path = out_dir / "diff_baseline.bag"
    diff_test_path = out_dir / "diff_test.bag"
    common_path = out_dir / "common.bag"
    print(f"[compare] diff bags: {diff_baseline_path.name}, {diff_test_path.name}, {common_path.name} を作成しました", file=sys.stderr)


def _write_diff_bags_impl(
    baseline_bag: Path,
    test_bag: Path,
    out_dir: Path,
    scene_cache: dict | None = None,
) -> None:
    def _open_out_bag(path: Path):
        # Keep chunks small to avoid occasional rosbag chunk header overflow on close.
        return rosbag.Bag(str(path), "w", chunk_threshold=64 * 1024)

    def _first_msg_from_by_stamp(by_stamp: dict) -> object | None:
        if not by_stamp:
            return None
        try:
            return next(iter(by_stamp.values()))[1]
        except Exception:
            return None

    def _set_msg_stamp(msg, stamp_nsec: int) -> None:
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
        # Fallback for environments where rospy is unavailable.
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

    def _empty_msg_like(template_msg, stamp_nsec: int, kind: str):
        if template_msg is None:
            return None
        template_cache = getattr(_empty_msg_like, "_template_cache", None)
        if template_cache is None:
            template_cache = {}
            setattr(_empty_msg_like, "_template_cache", template_cache)
        cache_key = (int(id(template_msg)), kind)
        msg = template_cache.get(cache_key)
        if msg is None:
            try:
                msg = copy.deepcopy(template_msg)
            except Exception:
                return None
            if kind == "wm":
                msg.objects = []
            elif kind == "lane":
                msg.arrays = []
            elif kind == "traffic":
                msg.trafficlightlist = []
            template_cache[cache_key] = msg
        msg = copy.deepcopy(msg)
        _set_msg_stamp(msg, stamp_nsec)
        return msg

    full_object_cache: dict[int, object] = {}
    full_lane_array_cache: dict[int, object] = {}
    filtered_object_cache: dict[tuple, object] = {}
    filtered_wm_msg_cache: dict[tuple, object] = {}
    filtered_lane_msg_cache: dict[tuple, object] = {}
    empty_materialized_msg_cache: dict[tuple, object] = {}

    def _copy_full_object_cached(obj):
        key = int(id(obj))
        cached = full_object_cache.get(key)
        if cached is None:
            cached = copy.deepcopy(obj)
            full_object_cache[key] = cached
        return cached

    def _copy_lane_array_cached(arr):
        key = int(id(arr))
        cached = full_lane_array_cache.get(key)
        if cached is None:
            cached = copy.deepcopy(arr)
            full_lane_array_cache[key] = cached
        return cached

    def _normalize_counter(counter_obj) -> tuple:
        if not counter_obj:
            return ()
        return tuple(sorted((sig, int(count)) for sig, count in Counter(counter_obj).items() if int(count) > 0))

    def _normalize_keep_traj(keep_traj_by_id) -> tuple:
        if not keep_traj_by_id:
            return ()
        items = []
        for oid, counter_obj in keep_traj_by_id.items():
            norm = _normalize_counter(counter_obj)
            if norm:
                items.append((int(oid), norm))
        return tuple(sorted(items))

    def _filter_object_trajectories_cached_local(obj, keep_counter: Counter, path_entries: list[tuple] | None = None):
        cache_key = (int(id(obj)), _normalize_counter(keep_counter))
        cached = filtered_object_cache.get(cache_key)
        if cached is not None:
            return cached
        out = copy.deepcopy(obj)
        remaining = Counter(keep_counter)
        kept = []
        entries = path_entries
        if entries is None:
            entries = [
                (path_msg, _trajectory_path_signature(path_msg))
                for path_msg in getattr(obj, "trajectory_set", []) or []
                if getattr(path_msg, "trajectory", []) or []
            ]
        for path_msg, sig in entries:
            if remaining.get(sig, 0) > 0:
                kept.append(copy.deepcopy(path_msg))
                remaining[sig] -= 1
        out.trajectory_set = kept
        filtered_object_cache[cache_key] = out
        return out

    def _empty_msg_like_cached(template_msg, stamp_nsec: int, kind: str):
        if template_msg is None:
            return None
        key = (int(id(template_msg)), int(stamp_nsec), str(kind))
        cached = empty_materialized_msg_cache.get(key)
        if cached is None:
            cached = _empty_msg_like(template_msg, stamp_nsec, kind)
            empty_materialized_msg_cache[key] = cached
        return cached

    def _filter_wm_msg(msg, feature: dict, stamp_nsec: int, keep_ids: set, keep_traj_by_id=None, keep_vsl_counter=None):
        keep_traj_by_id = keep_traj_by_id or {}
        keep_vsl_counter = Counter(keep_vsl_counter or {})
        cache_key = (
            int(id(msg)),
            int(stamp_nsec),
            tuple(sorted(int(oid) for oid in keep_ids or ())),
            _normalize_keep_traj(keep_traj_by_id),
            _normalize_counter(keep_vsl_counter),
        )
        cached = filtered_wm_msg_cache.get(cache_key)
        if cached is not None:
            return cached
        new_msg = _empty_msg_like(msg, stamp_nsec, "wm")
        if new_msg is None:
            return None
        if not keep_ids and not keep_traj_by_id and not keep_vsl_counter:
            filtered_wm_msg_cache[cache_key] = new_msg
            return new_msg
        objs = getattr(msg, "objects", []) or []
        path_entries_by_id = feature.get("path_entries_by_id", {}) if isinstance(feature, dict) else {}
        for o in objs:
            oid = int(getattr(getattr(o, "object", None), "object_id", -1))
            if oid in keep_ids:
                new_msg.objects.append(_copy_full_object_cached(o))
                vsl_sig = _vsl_pose_signature_from_obj(o)
                if vsl_sig is not None and keep_vsl_counter.get(vsl_sig, 0) > 0:
                    keep_vsl_counter[vsl_sig] -= 1
                continue
            if oid in keep_traj_by_id:
                part = _filter_object_trajectories_cached_local(o, keep_traj_by_id[oid], path_entries_by_id.get(oid))
                if getattr(part, "trajectory_set", []) or []:
                    new_msg.objects.append(part)
                    vsl_sig = _vsl_pose_signature_from_obj(o)
                    if vsl_sig is not None and keep_vsl_counter.get(vsl_sig, 0) > 0:
                        keep_vsl_counter[vsl_sig] -= 1
                continue
            vsl_sig = _vsl_pose_signature_from_obj(o)
            if vsl_sig is not None and keep_vsl_counter.get(vsl_sig, 0) > 0:
                new_msg.objects.append(_copy_full_object_cached(o))
                keep_vsl_counter[vsl_sig] -= 1
        filtered_wm_msg_cache[cache_key] = new_msg
        return new_msg

    def _filter_lane_msg(msg, stamp_nsec: int, keep_curves: set):
        cache_key = (
            int(id(msg)),
            int(stamp_nsec),
            tuple(sorted(tuple(curve) for curve in (keep_curves or ()))),
        )
        cached = filtered_lane_msg_cache.get(cache_key)
        if cached is not None:
            return cached
        new_msg = _empty_msg_like(msg, stamp_nsec, "lane")
        if new_msg is None:
            return None
        if not keep_curves:
            filtered_lane_msg_cache[cache_key] = new_msg
            return new_msg
        arrs = getattr(msg, "arrays", []) or []
        for arr in arrs:
            if tuple(getattr(arr, "data", []) or []) in keep_curves:
                new_msg.arrays.append(_copy_lane_array_cached(arr))
        filtered_lane_msg_cache[cache_key] = new_msg
        return new_msg

    def _fill_missing_templates(topic_to_template: dict[str, object | None]) -> dict[str, object | None]:
        fallback = None
        for t in topic_to_template.values():
            if t is not None:
                fallback = t
                break
        if fallback is None:
            return topic_to_template
        return {k: (v if v is not None else fallback) for k, v in topic_to_template.items()}

    diff_progress_state = {"last_percent": -1}
    diff_progress_total = 0
    diff_progress_done = 0

    def _init_diff_progress(total_steps: int) -> None:
        nonlocal diff_progress_total, diff_progress_done
        diff_progress_total = total_steps
        diff_progress_done = 0
        if diff_progress_total > 0:
            _print_progress_line(
                f"[compare] {out_dir.name} diff-bags progress",
                0,
                diff_progress_total,
                diff_progress_state,
            )

    def _tick_diff_progress(step_count: int = 1) -> None:
        nonlocal diff_progress_done
        if diff_progress_total <= 0:
            return
        diff_progress_done += step_count
        _print_progress_line(
            f"[compare] {out_dir.name} diff-bags progress",
            diff_progress_done,
            diff_progress_total,
            diff_progress_state,
        )

    cache = scene_cache or _build_compare_scene_cache(baseline_bag, test_bag)
    baseline_wm = cache["baseline_wm_topics"]
    test_wm = cache["test_wm_topics"]
    baseline_lane = cache["baseline_lane_topics"]
    test_lane = cache["test_lane_topics"]
    baseline_traffic = cache["baseline_traffic_topic"]
    test_traffic = cache["test_traffic_topic"]
    bl_wm_by = cache["bl_wm_by"]
    te_wm_by = cache["te_wm_by"]
    bl_lane_by = cache["bl_lane_by"]
    te_lane_by = cache["te_lane_by"]
    bl_traffic_by = cache["bl_traffic_by"]
    te_traffic_by = cache["te_traffic_by"]
    wm_features = cache["wm_features"]
    diff_plan = cache.get("diff_plan")
    if diff_plan is None:
        diff_plan = _build_diff_plan_from_cache(cache)
        cache["diff_plan"] = diff_plan
    common_stamps_sorted = list(diff_plan.get("common_stamps_sorted", []))
    bl_ref_by = bl_wm_by.get(baseline_wm[0], {})
    te_ref_by = te_wm_by.get(test_wm[0], {})

    common_n = len(common_stamps_sorted)
    clock_write_steps = (3 * common_n) if HAS_CLOCK_MSG else 0
    traffic_write_steps = 3 * common_n
    wm_write_steps = 3 * len(baseline_wm) * common_n
    lane_write_steps = 3 * len(baseline_lane) * common_n
    diff_progress_total_steps = clock_write_steps + traffic_write_steps + wm_write_steps + lane_write_steps
    _init_diff_progress(diff_progress_total_steps)

    def _write_clock_at_stamps(out_bag, stamps):
        if not HAS_CLOCK_MSG or not stamps:
            return
        for stamp_nsec in stamps:
            t = rospy.Time(stamp_nsec // 10**9, stamp_nsec % 10**9)
            msg = Clock(clock=t)
            out_bag.write("/clock", msg, t)
            _tick_diff_progress()
    wm_common_obj = diff_plan["wm_common_obj"]
    wm_common_traj_keep = diff_plan["wm_common_traj_keep"]
    wm_common_vsl_keep = diff_plan["wm_common_vsl_keep"]
    wm_diff_b = diff_plan["wm_diff_b"]
    wm_diff_t = diff_plan["wm_diff_t"]
    wm_diff_b_traj_keep = diff_plan["wm_diff_b_traj_keep"]
    wm_diff_t_traj_keep = diff_plan["wm_diff_t_traj_keep"]
    wm_diff_b_vsl_keep = diff_plan["wm_diff_b_vsl_keep"]
    wm_diff_t_vsl_keep = diff_plan["wm_diff_t_vsl_keep"]
    lane_common = diff_plan["lane_common"]
    lane_diff_b = diff_plan["lane_diff_b"]
    lane_diff_t = diff_plan["lane_diff_t"]

    bl_wm_templates = _fill_missing_templates({topic: _first_msg_from_by_stamp(bl_wm_by.get(topic, {})) for topic in baseline_wm})
    te_wm_templates = _fill_missing_templates({topic: _first_msg_from_by_stamp(te_wm_by.get(topic, {})) for topic in test_wm})
    bl_lane_templates = _fill_missing_templates({topic: _first_msg_from_by_stamp(bl_lane_by.get(topic, {})) for topic in baseline_lane})
    te_lane_templates = _fill_missing_templates({topic: _first_msg_from_by_stamp(te_lane_by.get(topic, {})) for topic in test_lane})
    bl_traffic_template = _first_msg_from_by_stamp(bl_traffic_by)
    te_traffic_template = _first_msg_from_by_stamp(te_traffic_by)

    out_dir.mkdir(parents=True, exist_ok=True)
    diff_baseline_path = out_dir / "diff_baseline.bag"
    diff_test_path = out_dir / "diff_test.bag"
    common_path = out_dir / "common.bag"

    # 表示で競合しないよう common / diff は別トピックで保存。common は baseline から 1 本のみ
    common_wm = [f"{VALIDATION_COMMON_NS}{t}" for t in WM_TOPICS]
    common_lane = [f"{VALIDATION_COMMON_NS}{t}" for t in LANE_TOPICS]
    common_traffic = f"{VALIDATION_COMMON_NS}{TRAFFIC_TOPIC}"
    diff_baseline_wm = [f"{VALIDATION_DIFF_BASELINE_NS}{t}" for t in WM_TOPICS]
    diff_baseline_lane = [f"{VALIDATION_DIFF_BASELINE_NS}{t}" for t in LANE_TOPICS]
    diff_baseline_traffic = f"{VALIDATION_DIFF_BASELINE_NS}{TRAFFIC_TOPIC}"
    diff_test_wm = [f"{VALIDATION_DIFF_TEST_NS}{t}" for t in WM_TOPICS]
    diff_test_lane = [f"{VALIDATION_DIFF_TEST_NS}{t}" for t in LANE_TOPICS]
    diff_test_traffic = f"{VALIDATION_DIFF_TEST_NS}{TRAFFIC_TOPIC}"

    # common.bag: 共通部分のみ → baseline 由来で 1 本だけ /validation/common へ
    with _open_out_bag(common_path) as out_bag:
        _write_clock_at_stamps(out_bag, common_stamps_sorted)
        for stamp in common_stamps_sorted:
            if stamp in bl_traffic_by:
                t, msg = bl_traffic_by[stamp]
            else:
                ref = bl_ref_by.get(stamp)
                if ref is None:
                    _tick_diff_progress()
                    continue
                t = ref[0]
                msg = _empty_msg_like_cached(bl_traffic_template, stamp, "traffic")
            if msg is not None:
                out_bag.write(common_traffic, msg, t)
            _tick_diff_progress()
        for topic_b, out_topic in zip(baseline_wm, common_wm):
            for stamp in common_stamps_sorted:
                keep_ids = wm_common_obj.get(topic_b, {}).get(stamp, set())
                keep_traj = wm_common_traj_keep.get(topic_b, {}).get(stamp, {})
                keep_vsl = wm_common_vsl_keep.get(topic_b, {}).get(stamp, Counter())
                if stamp in bl_wm_by.get(topic_b, {}):
                    t, msg = bl_wm_by[topic_b][stamp]
                    filtered = _filter_wm_msg(msg, wm_features["baseline"].get(topic_b, {}).get(stamp, {}), stamp, keep_ids, keep_traj, keep_vsl)
                else:
                    ref = bl_ref_by.get(stamp)
                    if ref is None:
                        _tick_diff_progress()
                        continue
                    t = ref[0]
                    filtered = _empty_msg_like_cached(bl_wm_templates.get(topic_b), stamp, "wm")
                if filtered is not None:
                    out_bag.write(out_topic, filtered, t)
                _tick_diff_progress()
        for topic_b, out_topic in zip(baseline_lane, common_lane):
            for stamp in common_stamps_sorted:
                keep_curves = lane_common.get(topic_b, {}).get(stamp, set())
                if stamp in bl_lane_by.get(topic_b, {}):
                    t, bl_msg = bl_lane_by[topic_b][stamp]
                    filtered = _filter_lane_msg(bl_msg, stamp, keep_curves)
                else:
                    ref = bl_ref_by.get(stamp)
                    if ref is None:
                        _tick_diff_progress()
                        continue
                    t = ref[0]
                    filtered = _empty_msg_like_cached(bl_lane_templates.get(topic_b), stamp, "lane")
                if filtered is not None:
                    out_bag.write(out_topic, filtered, t)
                _tick_diff_progress()

    # diff_baseline.bag → diff_baseline トピックへ（他 bag とずれないよう全 common スタンプでメッセージを書く。差分なしは空メッセージ）
    with _open_out_bag(diff_baseline_path) as out_bag:
        _write_clock_at_stamps(out_bag, common_stamps_sorted)
        for stamp in common_stamps_sorted:
            if stamp in bl_traffic_by:
                t, msg = bl_traffic_by[stamp]
            else:
                ref = bl_ref_by.get(stamp)
                if ref is None:
                    _tick_diff_progress()
                    continue
                t = ref[0]
                msg = _empty_msg_like_cached(bl_traffic_template, stamp, "traffic")
            if msg is not None:
                out_bag.write(diff_baseline_traffic, msg, t)
            _tick_diff_progress()
        for topic, out_topic in zip(baseline_wm, diff_baseline_wm):
            for stamp in common_stamps_sorted:
                keep_ids = wm_diff_b.get(topic, {}).get(stamp, set())
                keep_traj = wm_diff_b_traj_keep.get(topic, {}).get(stamp, {})
                keep_vsl = wm_diff_b_vsl_keep.get(topic, {}).get(stamp, Counter())
                if stamp in bl_wm_by.get(topic, {}):
                    t, msg = bl_wm_by[topic][stamp]
                    filtered = _filter_wm_msg(msg, wm_features["baseline"].get(topic, {}).get(stamp, {}), stamp, keep_ids, keep_traj, keep_vsl)
                else:
                    ref = bl_ref_by.get(stamp)
                    if ref is None:
                        _tick_diff_progress()
                        continue
                    t = ref[0]
                    filtered = _empty_msg_like_cached(bl_wm_templates.get(topic), stamp, "wm")
                if filtered is not None:
                    out_bag.write(out_topic, filtered, t)
                _tick_diff_progress()
        for topic_b, out_topic in zip(baseline_lane, diff_baseline_lane):
            for stamp in common_stamps_sorted:
                keep_curves = lane_diff_b.get(topic_b, {}).get(stamp, set())
                if stamp in bl_lane_by.get(topic_b, {}):
                    t, bl_msg = bl_lane_by[topic_b][stamp]
                    filtered = _filter_lane_msg(bl_msg, stamp, keep_curves)
                else:
                    ref = bl_ref_by.get(stamp)
                    if ref is None:
                        _tick_diff_progress()
                        continue
                    t = ref[0]
                    filtered = _empty_msg_like_cached(bl_lane_templates.get(topic_b), stamp, "lane")
                if filtered is not None:
                    out_bag.write(out_topic, filtered, t)
                _tick_diff_progress()

    # diff_test.bag → diff_test トピックへ（他 bag とずれないよう全 common スタンプでメッセージを書く。差分なしは空メッセージ）
    with _open_out_bag(diff_test_path) as out_bag:
        _write_clock_at_stamps(out_bag, common_stamps_sorted)
        for stamp in common_stamps_sorted:
            if stamp in te_traffic_by:
                t, msg = te_traffic_by[stamp]
            else:
                ref = te_ref_by.get(stamp)
                if ref is None:
                    _tick_diff_progress()
                    continue
                t = ref[0]
                msg = _empty_msg_like_cached(te_traffic_template, stamp, "traffic")
            if msg is not None:
                out_bag.write(diff_test_traffic, msg, t)
            _tick_diff_progress()
        for topic, out_topic in zip(test_wm, diff_test_wm):
            for stamp in common_stamps_sorted:
                keep_ids = wm_diff_t.get(topic, {}).get(stamp, set())
                keep_traj = wm_diff_t_traj_keep.get(topic, {}).get(stamp, {})
                keep_vsl = wm_diff_t_vsl_keep.get(topic, {}).get(stamp, Counter())
                if stamp in te_wm_by.get(topic, {}):
                    t, msg = te_wm_by[topic][stamp]
                    filtered = _filter_wm_msg(msg, wm_features["test"].get(topic, {}).get(stamp, {}), stamp, keep_ids, keep_traj, keep_vsl)
                else:
                    ref = te_ref_by.get(stamp)
                    if ref is None:
                        _tick_diff_progress()
                        continue
                    t = ref[0]
                    filtered = _empty_msg_like_cached(te_wm_templates.get(topic), stamp, "wm")
                if filtered is not None:
                    out_bag.write(out_topic, filtered, t)
                _tick_diff_progress()
        for topic_t, out_topic in zip(test_lane, diff_test_lane):
            for stamp in common_stamps_sorted:
                keep_curves = lane_diff_t.get(topic_t, {}).get(stamp, set())
                if stamp in te_lane_by.get(topic_t, {}):
                    t, te_msg = te_lane_by[topic_t][stamp]
                    filtered = _filter_lane_msg(te_msg, stamp, keep_curves)
                else:
                    ref = te_ref_by.get(stamp)
                    if ref is None:
                        _tick_diff_progress()
                        continue
                    t = ref[0]
                    filtered = _empty_msg_like_cached(te_lane_templates.get(topic_t), stamp, "lane")
                if filtered is not None:
                    out_bag.write(out_topic, filtered, t)
                _tick_diff_progress()


def main() -> int:
    root = get_tester_root()
    settings = load_settings(root)
    paths = settings["paths"]
    baseline_root = root / paths["baseline_results"]
    test_results_root = root / paths["test_results"]

    dirs = discover_bag_directories(root / paths["test_bags"])
    if not dirs:
        return 0

    # Load commit info for baseline and test (recorded when run_baseline/run_test was executed)
    baseline_commit = {}
    test_commit = {}
    if (baseline_root / "commit_info.json").exists():
        try:
            with open(baseline_root / "commit_info.json", "r", encoding="utf-8") as f:
                baseline_commit = json.load(f)
        except Exception:
            pass
    if (test_results_root / "commit_info.json").exists():
        try:
            with open(test_results_root / "commit_info.json", "r", encoding="utf-8") as f:
                test_commit = json.load(f)
        except Exception:
            pass

    test_bags_root = root / paths["test_bags"]
    all_results = []
    for rel, dir_path in dirs:
        baseline_bag = baseline_root / rel / "result_baseline.bag"
        test_bag = test_results_root / rel / "result_test.bag"
        input_bags = get_bag_files_in_dir(dir_path) if dir_path.is_dir() else []
        scene_cache = _build_compare_scene_cache(baseline_bag, test_bag) if (baseline_bag.exists() and test_bag.exists() and HAS_ROSBAG) else None
        res = compare_one(rel, baseline_bag, test_bag, input_bags=input_bags, scene_cache=scene_cache)
        res["baseline_commit"] = baseline_commit.get("commit", "")
        res["baseline_describe"] = baseline_commit.get("describe", "")
        res["test_commit"] = test_commit.get("commit", "")
        res["test_describe"] = test_commit.get("describe", "")
        res["segfault_baseline"] = (baseline_root / rel / "segfault").exists()
        res["segfault_test"] = (test_results_root / rel / "segfault").exists()
        _normalize_dt = lambda s: (
            "warning" if (s or "").strip().lower() == "warning" else
            "invalid" if (s or "").strip().lower() == "invalid" else
            "valid"
        )
        _read_dt = lambda p: _normalize_dt(p.read_text().strip()) if p.exists() else "valid"
        _read_dt_max = lambda p: p.read_text().strip() if p.exists() else "-"
        baseline_dt_summary = _load_dt_summary(baseline_root / rel / "dt_summary.json")
        test_dt_summary = _load_dt_summary(test_results_root / rel / "dt_summary.json")
        res["dt_status_baseline"] = _read_dt(baseline_root / rel / "dt_status")
        res["dt_status_test"] = _read_dt(test_results_root / rel / "dt_status")
        res["dt_max_baseline"] = _read_dt_max(baseline_root / rel / "dt_max")
        res["dt_max_test"] = _read_dt_max(test_results_root / rel / "dt_max")
        _merge_dt_summary_into_result(res, "baseline", baseline_dt_summary)
        _merge_dt_summary_into_result(res, "test", test_dt_summary)
        all_results.append(res)
        out_dir = test_results_root / rel
        out_dir.mkdir(parents=True, exist_ok=True)
        with open(out_dir / "comparison_direct.json", "w", encoding="utf-8") as f:
            json.dump(res, f, indent=2, ensure_ascii=False)
        try:
            _write_diff_bags(baseline_bag, test_bag, out_dir, scene_cache=scene_cache)
        except Exception as e:
            print(f"[compare] {rel}: diff bags 作成失敗: {e}", file=sys.stderr)
        status = "Unchanged" if res["overall_ok"] else "Changed"
        valid = res.get("valid_frames")
        total = res.get("total_frames")
        # 全体 = 和集合。欠けている場合は record_frames から算出
        if total is None and valid is not None:
            rb, rt = res.get("record_frames_baseline"), res.get("record_frames_test")
            if rb is not None and rt is not None:
                total = rb + rt - valid  # |A∪B| = |A| + |B| - |A∩B|
        valid_str = f"{valid}/{total}" if (valid is not None and total is not None) else "-/-"
        # 要約に全項目（Lane / VSL / Object / Path / Traffic）を常に表示
        _ok = lambda k: "OK" if res.get(k, True) else "NG"
        cat_str = f"  Lane:{_ok('lane_ids_ok')} VSL:{_ok('vsl_ok')} Obj:{_ok('object_ids_ok')} Path:{_ok('path_ok')} Traffic:{_ok('traffic_ok')}"
        failed = []
        if not res.get("lane_ids_ok", True):
            failed.append("lane_ids")
        if not res.get("vsl_ok", True):
            failed.append("vsl")
        if not res.get("object_ids_ok", True):
            failed.append("object_ids")
        if not res.get("path_ok", True):
            failed.append("path")
        if not res.get("traffic_ok", True):
            failed.append("traffic")
        if res.get("segfault_baseline") or res.get("segfault_test"):
            failed.append("segfault")
        if res.get("dt_status_baseline") == "invalid" or res.get("dt_status_test") == "invalid":
            failed.append("dt_invalid")
        elif res.get("dt_status_baseline") == "warning" or res.get("dt_status_test") == "warning":
            failed.append("dt_warning")
        suffix = f" ({', '.join(failed)})" if failed else ""
        seg_b = "Yes" if res.get("segfault_baseline") else "No"
        seg_t = "Yes" if res.get("segfault_test") else "No"
        rt_b = _normalize_dt(res.get("dt_status_baseline") or "valid")
        rt_t = _normalize_dt(res.get("dt_status_test") or "valid")
        max_b = res.get("dt_max_baseline") or "-"
        max_t = res.get("dt_max_test") or "-"
        print(f"[compare] {rel}: {valid_str}  {status}{suffix}{cat_str}  segfault: baseline={seg_b} test={seg_t}  runtime: baseline={rt_b} ({max_b}) test={rt_t} ({max_t})")
        # 詳細は TP_VERBOSE_COMPARE=1 のときだけ stderr に表示
        if not res["overall_ok"] and os.environ.get("TP_VERBOSE_COMPARE") in ("1", "true", "yes"):
            for key in ("lane_ids_detail", "vsl_detail", "object_ids_detail", "path_and_traffic_detail"):
                v = res.get(key, "")
                if v and v != "Unchanged":
                    print(f"  {key}: {v}", file=sys.stderr)

    n_ok = sum(1 for r in all_results if r["overall_ok"])
    n_ng = len(all_results) - n_ok
    print(f"[compare] Done: {n_ok} Unchanged, {n_ng} Changed. Details: test_results/<dir>/comparison_direct.json")
    return 0 if all(r["overall_ok"] for r in all_results) else 1


if __name__ == "__main__":
    sys.exit(main())
