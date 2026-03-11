#!/usr/bin/env python3
"""Baseline creation: sceneごとに trajectory_predictor_sim_offline を直接実行する。"""
from __future__ import annotations

import bisect
import copy
import json
import math
import os
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

from common import (
    VALIDATION_BASELINE_NS,
    discover_bag_directories,
    get_bag_files_in_dir,
    get_commit_info_for_run,
    get_tester_root,
    load_settings,
    run_catkin_build,
    run_trajectory_predictor_offline,
    select_bag_files_by_topics,
)

try:
    import rosbag
    HAS_ROSBAG = True
except ImportError:
    HAS_ROSBAG = False

try:
    from nrc_msgs.msg import (
        TrackedObject2WithTrajectory,
        TrackedObjectSet2WithTrajectory,
        TrajectoryState,
        TrajectoryStateSet,
    )
    HAS_NRC_MSGS = True
except Exception:
    HAS_NRC_MSGS = False


WM_TOPIC_SUFFIXES = [
    "/WM/tracked_object_set_with_prediction",
    "/WM/along_object_set_with_prediction",
    "/WM/crossing_object_set_with_prediction",
    "/WM/oncoming_object_set_with_prediction",
    "/WM/other_object_set_with_prediction",
]
VALIDATION_OBSERVED_BASELINE_NS = "/validation/observed_baseline"
TP_SIM_OFFLINE_INPUT_TOPICS = [
    "/dynamic_global_pose",
    "/target_tracker/tracked_object_set2",
    "/RX/EgoMotion_info",
    "/parked/tracked_object_set",
    "/route_plan/road_level_route",
    "/route_plan/lane_level_route",
    "/route_plan/lane_level_route_plan",
    "/LogicalPose",
    "/controller_state",
    "/CAN/OdometryInput",
    "/DM/stopline_position",
    "/traffic_light_state2",
    "/DM/stopline_decisions",
    "/ad_state",
]
OBSERVED_HORIZON_SEC = 10.0
OBSERVED_STEP_SEC = 0.5
# 10Hz 前提で 0.5s サンプル時の揺らぎ許容（若干の stamp ずれを吸収）
OBSERVED_SAMPLE_TOLERANCE_SEC = 0.26
OBSERVED_SOURCE_TOPIC_CANDIDATES = [
    "/target_tracker/tracked_object_set2",
    "/sensor_fusion/tracked_object_set2",
    "/local_model/tracked_object_set2",
]
OBSERVED_EGO_LOGICAL_POSE_TOPIC = "/LogicalPose"
OBSERVED_EGO_CONTROLLER_STATE_TOPIC = "/controller_state"
OBSERVED_EGO_ODOMETRY_INPUT_TOPIC = "/CAN/OdometryInput"
TP_EGO_INTERNAL_ID_DEFAULT = 500000
TP_EGO_FORWARD_OFFSET_M = 1.93
TP_EGO_LENGTH_M = 4.7
TP_EGO_WIDTH_M = 1.85
TP_EGO_CLASSIFICATION_CAR = 4  # TrackedObject2 classification: 4=Car


def _map_tp_internal_id_to_output_id(object_id: int) -> int:
    if object_id == 500000:
        return 0
    if object_id == 500001:
        return 1
    return object_id


def _extract_logical_pose_internal_ego_id(logical_pose) -> int:
    if logical_pose is None:
        return TP_EGO_INTERNAL_ID_DEFAULT
    for attr in ("id", "object_id", "ego_id"):
        if hasattr(logical_pose, attr):
            try:
                return int(getattr(logical_pose, attr))
            except (TypeError, ValueError):
                continue
    return TP_EGO_INTERNAL_ID_DEFAULT


def _extract_ego_state_from_components(
    logical_pose,
    controller_state=None,
    odometry_input=None,
) -> tuple[tuple[float, float, float, float] | None, int]:
    if logical_pose is None:
        return None, TP_EGO_INTERNAL_ID_DEFAULT

    yaw = float(getattr(logical_pose, "map_yaw", 0.0) or 0.0)
    x = float(getattr(logical_pose, "map_position_x", 0.0) or 0.0) + TP_EGO_FORWARD_OFFSET_M * math.cos(yaw)
    y = float(getattr(logical_pose, "map_position_y", 0.0) or 0.0) + TP_EGO_FORWARD_OFFSET_M * math.sin(yaw)

    speed = 0.0
    if controller_state is not None:
        speed = float(getattr(controller_state, "velocity", 0.0) or 0.0)
    elif odometry_input is not None:
        speed = float(getattr(odometry_input, "velocity_rear_axis", 0.0) or 0.0)
    speed = abs(speed)

    internal_id = _extract_logical_pose_internal_ego_id(logical_pose)
    return (x, y, yaw, speed), internal_id


def _extract_ego_state_from_source_msg(msg) -> tuple[tuple[float, float, float, float] | None, int]:
    if msg is None:
        return None, TP_EGO_INTERNAL_ID_DEFAULT

    logical_pose = None
    if hasattr(msg, "has_logical_pose"):
        if bool(getattr(msg, "has_logical_pose", False)):
            logical_pose = getattr(msg, "logical_pose", None)
    else:
        logical_pose = getattr(msg, "logical_pose", None)

    controller_state = None
    if hasattr(msg, "has_controller_state"):
        if bool(getattr(msg, "has_controller_state", False)):
            controller_state = getattr(msg, "controller_state", None)
    else:
        controller_state = getattr(msg, "controller_state", None)

    odometry_input = None
    if hasattr(msg, "has_odometry_input"):
        if bool(getattr(msg, "has_odometry_input", False)):
            odometry_input = getattr(msg, "odometry_input", None)
    else:
        odometry_input = getattr(msg, "odometry_input", None)

    return _extract_ego_state_from_components(logical_pose, controller_state, odometry_input)


def _collect_tracked_object_set2_topic_counts(input_bags: list[Path]) -> dict[str, int]:
    topic_counts: dict[str, int] = defaultdict(int)
    for bag_path in input_bags:
        if not bag_path.exists():
            continue
        with rosbag.Bag(str(bag_path), "r") as bag:
            info = bag.get_type_and_topic_info()
            topics_info = getattr(info, "topics", {}) or {}
            for topic, tinf in topics_info.items():
                msg_type = getattr(tinf, "msg_type", "")
                if msg_type in ("nrc_msgs/TrackedObjectSet2", "nrc_msgs/TrajectoryPredictorSimInputFrame"):
                    topic_counts[topic] += int(getattr(tinf, "message_count", 0) or 0)
    return dict(topic_counts)


def _to_nsec(ts) -> int:
    if ts is None:
        return 0
    if hasattr(ts, "to_nsec"):
        return int(ts.to_nsec())
    sec = getattr(ts, "secs", getattr(ts, "sec", 0)) or 0
    nsec = getattr(ts, "nsecs", getattr(ts, "nsec", getattr(ts, "nanosec", 0))) or 0
    return int(sec) * 10**9 + int(nsec)


def _msg_stamp_nsec(msg, bag_t) -> int:
    header = getattr(msg, "header", None)
    if header is not None:
        stamp = getattr(header, "stamp", None)
        if stamp is not None:
            return _to_nsec(stamp)
    return _to_nsec(bag_t)


def _extract_tracked_object_set2_from_source_msg(msg):
    if msg is None:
        return None
    if hasattr(msg, "objects") and hasattr(msg, "header"):
        return msg
    nested = getattr(msg, "tracked_object_set2", None)
    if nested is not None and hasattr(nested, "objects") and hasattr(nested, "header"):
        return nested
    return None


def _yaw_from_quat(q) -> float:
    x = getattr(q, "x", 0.0) if q is not None else 0.0
    y = getattr(q, "y", 0.0) if q is not None else 0.0
    z = getattr(q, "z", 0.0) if q is not None else 0.0
    w = getattr(q, "w", 1.0) if q is not None else 1.0
    siny_cosp = 2.0 * (w * z + x * y)
    cosy_cosp = 1.0 - 2.0 * (y * y + z * z)
    return math.atan2(siny_cosp, cosy_cosp)


def _speed_from_object(obj) -> float:
    vel = getattr(obj, "velocity", None)
    linear = getattr(vel, "linear", None) if vel is not None else None
    vx = float(getattr(linear, "x", 0.0) or 0.0) if linear is not None else 0.0
    vy = float(getattr(linear, "y", 0.0) or 0.0) if linear is not None else 0.0
    return math.hypot(vx, vy)


def _extract_object_state(obj) -> tuple[float, float, float, float]:
    pose = getattr(obj, "pose", None)
    pos = getattr(pose, "position", None) if pose is not None else None
    ori = getattr(pose, "orientation", None) if pose is not None else None
    x = float(getattr(pos, "x", 0.0) if pos is not None else 0.0)
    y = float(getattr(pos, "y", 0.0) if pos is not None else 0.0)
    yaw = _yaw_from_quat(ori)
    speed = float(_speed_from_object(obj))
    return x, y, yaw, speed


def _interpolate_state(
    prev_state: tuple[float, float, float, float],
    next_state: tuple[float, float, float, float],
    ratio: float,
) -> tuple[float, float, float, float]:
    x0, y0, yaw0, v0 = prev_state
    x1, y1, yaw1, v1 = next_state
    dyaw = (yaw1 - yaw0 + math.pi) % (2.0 * math.pi) - math.pi
    return (
        x0 + (x1 - x0) * ratio,
        y0 + (y1 - y0) * ratio,
        yaw0 + dyaw * ratio,
        v0 + (v1 - v0) * ratio,
    )


def _find_sample_index_for_object(
    records: list[dict],
    stamps: list[int],
    start_idx: int,
    target_ns: int,
    tolerance_ns: int,
    object_id: int,
) -> int | None:
    if start_idx >= len(stamps):
        return None
    lo = bisect.bisect_left(stamps, target_ns - tolerance_ns, lo=start_idx)
    hi = bisect.bisect_right(stamps, target_ns + tolerance_ns, lo=lo)
    best_idx = None
    best_delta = None
    for idx in range(lo, hi):
        if object_id not in records[idx]["obj_by_id"]:
            continue
        delta = abs(stamps[idx] - target_ns)
        if best_idx is None or delta < best_delta:
            best_idx = idx
            best_delta = delta
    return best_idx


def _find_sample_index_for_ego_state(
    records: list[dict],
    stamps: list[int],
    start_idx: int,
    target_ns: int,
    tolerance_ns: int,
) -> int | None:
    if start_idx >= len(stamps):
        return None
    lo = bisect.bisect_left(stamps, target_ns - tolerance_ns, lo=start_idx)
    hi = bisect.bisect_right(stamps, target_ns + tolerance_ns, lo=lo)
    best_idx = None
    best_delta = None
    for idx in range(lo, hi):
        if records[idx].get("ego_state") is None:
            continue
        delta = abs(stamps[idx] - target_ns)
        if best_idx is None or delta < best_delta:
            best_idx = idx
            best_delta = delta
    return best_idx


def _set_quat_from_yaw(quat, yaw: float) -> None:
    half = 0.5 * yaw
    quat.x = 0.0
    quat.y = 0.0
    quat.z = math.sin(half)
    quat.w = math.cos(half)


def _apply_state_to_traj_obj_object(out_obj, state: tuple[float, float, float, float], internal_id: int) -> None:
    x, y, yaw, speed = state
    out_obj.object_id = int(_map_tp_internal_id_to_output_id(internal_id))
    out_obj.pose.position.x = x
    out_obj.pose.position.y = y
    out_obj.pose.position.z = 0.0
    _set_quat_from_yaw(out_obj.pose.orientation, yaw)
    out_obj.velocity.linear.x = speed * math.cos(yaw)
    out_obj.velocity.linear.y = speed * math.sin(yaw)
    out_obj.velocity.angular.z = 0.0
    out_obj.acceleration.x = 0.0
    out_obj.acceleration.y = 0.0
    out_obj.dimensions.x = TP_EGO_LENGTH_M
    out_obj.dimensions.y = TP_EGO_WIDTH_M
    out_obj.dimensions.z = 1.0
    out_obj.classification = TP_EGO_CLASSIFICATION_CAR


def _resolve_observed_source_topic(
    input_bags: list[Path], preferred_topic: str | None = None, topic_counts: dict[str, int] | None = None
) -> str:
    if topic_counts is None:
        topic_counts = _collect_tracked_object_set2_topic_counts(input_bags)

    if preferred_topic:
        if topic_counts.get(preferred_topic, 0) > 0:
            return preferred_topic
        raise RuntimeError(f"preferred observed source topic not found in input bags: {preferred_topic}")

    for topic in OBSERVED_SOURCE_TOPIC_CANDIDATES:
        if topic_counts.get(topic, 0) > 0:
            return topic
    if topic_counts:
        return max(topic_counts.items(), key=lambda kv: kv[1])[0]
    raise RuntimeError("no nrc_msgs/TrackedObjectSet2 topic found in input bags")


def _collect_observed_source_records(input_bags: list[Path], source_topic: str) -> list[dict]:
    aux_topics = [
        OBSERVED_EGO_LOGICAL_POSE_TOPIC,
        OBSERVED_EGO_CONTROLLER_STATE_TOPIC,
        OBSERVED_EGO_ODOMETRY_INPUT_TOPIC,
    ]
    aux_records: dict[str, list[tuple[int, object]]] = {topic: [] for topic in aux_topics}

    for bag_path in input_bags:
        if not bag_path.exists():
            continue
        with rosbag.Bag(str(bag_path), "r") as bag:
            for topic, msg, t in bag.read_messages(topics=aux_topics):
                aux_records[topic].append((_msg_stamp_nsec(msg, t), msg))

    aux_index: dict[str, tuple[list[int], list[object]]] = {}
    for topic, items in aux_records.items():
        items.sort(key=lambda x: x[0])
        aux_index[topic] = ([s for s, _ in items], [m for _, m in items])

    def latest_aux(topic: str, target_stamp_ns: int):
        stamps, msgs = aux_index.get(topic, ([], []))
        if not stamps:
            return None
        idx = bisect.bisect_right(stamps, target_stamp_ns) - 1
        if idx < 0:
            return None
        return msgs[idx]

    records = []
    for bag_path in input_bags:
        if not bag_path.exists():
            continue
        with rosbag.Bag(str(bag_path), "r") as bag:
            for _, msg, t in bag.read_messages(topics=[source_topic]):
                tracked = _extract_tracked_object_set2_from_source_msg(msg)
                if tracked is None:
                    continue
                stamp_ns = _msg_stamp_nsec(tracked, t)
                ego_state, ego_internal_id = _extract_ego_state_from_source_msg(msg)
                if ego_state is None:
                    logical_pose = latest_aux(OBSERVED_EGO_LOGICAL_POSE_TOPIC, stamp_ns)
                    controller_state = latest_aux(OBSERVED_EGO_CONTROLLER_STATE_TOPIC, stamp_ns)
                    odometry_input = latest_aux(OBSERVED_EGO_ODOMETRY_INPUT_TOPIC, stamp_ns)
                    ego_state, ego_internal_id = _extract_ego_state_from_components(
                        logical_pose,
                        controller_state=controller_state,
                        odometry_input=odometry_input,
                    )
                obj_by_id = {}
                for obj in getattr(tracked, "objects", []) or []:
                    oid = int(getattr(obj, "object_id", -1))
                    if oid >= 0:
                        obj_by_id[oid] = obj
                records.append(
                    {
                        "stamp_ns": stamp_ns,
                        "bag_t_ns": _to_nsec(t),
                        "msg": tracked,
                        "t": t,
                        "obj_by_id": obj_by_id,
                        "ego_state": ego_state,
                        "ego_internal_id": ego_internal_id,
                    }
                )
    records.sort(key=lambda r: (r["stamp_ns"], r["bag_t_ns"]))
    return records


def _extract_object_id_from_traj_obj(obj) -> int:
    base_obj = getattr(obj, "object", obj)
    try:
        return int(getattr(base_obj, "object_id", -1))
    except Exception:
        return -1


def _resolve_existing_topic(topics_info: dict, candidates: list[str]) -> str | None:
    for topic in candidates:
        topic_info = topics_info.get(topic)
        if topic_info is not None and int(getattr(topic_info, "message_count", 0) or 0) > 0:
            return topic
    return None


def _collect_group_ids_by_stamp_from_result(
    result_bag: Path, result_ns: str
) -> dict[str, dict[int, set[int]]]:
    group_suffix = {
        "along": WM_TOPIC_SUFFIXES[1],
        "crossing": WM_TOPIC_SUFFIXES[2],
        "oncoming": WM_TOPIC_SUFFIXES[3],
        "other": WM_TOPIC_SUFFIXES[4],
    }
    group_ids: dict[str, dict[int, set[int]]] = {k: defaultdict(set) for k in group_suffix}
    with rosbag.Bag(str(result_bag), "r") as bag:
        info = bag.get_type_and_topic_info()
        topics_info = getattr(info, "topics", {}) or {}
        resolved_topics: dict[str, str] = {}
        for group, suffix in group_suffix.items():
            topic = _resolve_existing_topic(topics_info, [f"{result_ns}{suffix}", suffix])
            if topic:
                resolved_topics[group] = topic

        for group, topic in resolved_topics.items():
            for _, msg, t in bag.read_messages(topics=[topic]):
                stamp_ns = _msg_stamp_nsec(msg, t)
                id_set = group_ids[group][stamp_ns]
                for obj in getattr(msg, "objects", []) or []:
                    oid = _extract_object_id_from_traj_obj(obj)
                    if oid >= 0:
                        id_set.add(oid)
    return group_ids


def _build_observed_bag_from_input(
    input_bags: list[Path],
    observed_bag: Path,
    result_bag: Path,
    observed_ns: str,
    result_ns: str,
    preferred_source_topic: str | None = None,
) -> str:
    if not HAS_ROSBAG:
        raise RuntimeError("python rosbag is not available")
    if not HAS_NRC_MSGS:
        raise RuntimeError("nrc_msgs python messages are not available")
    if not input_bags:
        raise RuntimeError("no input bags to build observed bag")
    if not result_bag.exists():
        raise RuntimeError(f"result bag not found: {result_bag}")

    source_topic = _resolve_observed_source_topic(input_bags, preferred_source_topic)
    records = _collect_observed_source_records(input_bags, source_topic)
    if not records:
        raise RuntimeError(f"no messages found on observed source topic: {source_topic}")

    step_ns = int(round(OBSERVED_STEP_SEC * 1.0e9))
    horizon_steps = int(round(OBSERVED_HORIZON_SEC / OBSERVED_STEP_SEC))
    tolerance_ns = int(round(OBSERVED_SAMPLE_TOLERANCE_SEC * 1.0e9))
    stamps = [r["stamp_ns"] for r in records]
    group_ids_by_stamp = _collect_group_ids_by_stamp_from_result(result_bag, result_ns)

    observed_base_records: list[tuple] = []
    for i, rec in enumerate(records):
        anchor_msg = rec["msg"]
        anchor_stamp_ns = rec["stamp_ns"]
        ego_anchor_state = rec.get("ego_state")
        ego_internal_id = int(rec.get("ego_internal_id", TP_EGO_INTERNAL_ID_DEFAULT))
        ego_output_id = _map_tp_internal_id_to_output_id(ego_internal_id)
        replace_existing_ego = ego_anchor_state is not None

        observed_msg = TrackedObjectSet2WithTrajectory()
        observed_msg.header = copy.deepcopy(anchor_msg.header)
        if hasattr(observed_msg, "status") and hasattr(anchor_msg, "status"):
            observed_msg.status = anchor_msg.status
        if hasattr(observed_msg, "status_message") and hasattr(anchor_msg, "status_message"):
            observed_msg.status_message = anchor_msg.status_message
        observed_msg.objects = []
        existing_output_ids: set[int] = set()

        for anchor_obj in getattr(anchor_msg, "objects", []) or []:
            anchor_oid = int(getattr(anchor_obj, "object_id", -1))
            if anchor_oid < 0:
                continue
            mapped_anchor_oid = _map_tp_internal_id_to_output_id(anchor_oid)
            if replace_existing_ego and mapped_anchor_oid == ego_output_id:
                # ego は observed 生成時に logical_pose(+offset) ベースで作り直す。
                continue
            existing_output_ids.add(mapped_anchor_oid)

            out_obj = TrackedObject2WithTrajectory()
            out_obj.object = copy.deepcopy(anchor_obj)
            out_obj.trajectory_set = []

            traj_set = TrajectoryStateSet()
            if hasattr(traj_set, "prediction_mode"):
                traj_set.prediction_mode = 0
            traj_set.trajectory = []

            anchor_state = _extract_object_state(anchor_obj)
            sampled_states_by_step: dict[int, tuple[float, float, float, float]] = {0: anchor_state}
            last_known_step = 0
            last_known_state = anchor_state
            search_start_idx = i

            step_i = 1
            while step_i <= horizon_steps:
                found_step = None
                found_idx = None
                found_state = None
                # 欠損時は +1.0s ではなく、起点から horizon(10s) 末尾まで再接続を探索する。
                # 実際の探索は records 末尾で自然に頭打ちになる。
                search_end_step = horizon_steps
                for cand_step in range(step_i, search_end_step + 1):
                    target_ns = anchor_stamp_ns + cand_step * step_ns
                    sample_idx = _find_sample_index_for_object(
                        records,
                        stamps,
                        search_start_idx,
                        target_ns,
                        tolerance_ns,
                        anchor_oid,
                    )
                    if sample_idx is None:
                        continue
                    sample_obj = records[sample_idx]["obj_by_id"].get(anchor_oid)
                    if sample_obj is None:
                        continue
                    found_step = cand_step
                    found_idx = sample_idx
                    found_state = _extract_object_state(sample_obj)
                    break

                if found_step is None or found_state is None:
                    # 残り horizon 区間内で再接続不能なら、この object の observed trajectory はここで終端。
                    break

                if found_step > last_known_step + 1:
                    denom = float(found_step - last_known_step)
                    for fill_step in range(last_known_step + 1, found_step):
                        ratio = float(fill_step - last_known_step) / denom
                        sampled_states_by_step[fill_step] = _interpolate_state(last_known_state, found_state, ratio)

                sampled_states_by_step[found_step] = found_state
                last_known_step = found_step
                last_known_state = found_state
                search_start_idx = max(search_start_idx, found_idx)
                step_i = found_step + 1

            for sampled_step in sorted(sampled_states_by_step.keys()):
                x, y, yaw, speed = sampled_states_by_step[sampled_step]
                pt = TrajectoryState()
                if hasattr(pt, "t"):
                    pt.t = float(sampled_step * OBSERVED_STEP_SEC)
                if hasattr(pt, "x"):
                    pt.x = x
                if hasattr(pt, "y"):
                    pt.y = y
                if hasattr(pt, "yaw"):
                    pt.yaw = yaw
                if hasattr(pt, "velocity"):
                    pt.velocity = speed
                if hasattr(pt, "covariance"):
                    pt.covariance = [0.0] * 16
                traj_set.trajectory.append(pt)

            if traj_set.trajectory:
                out_obj.trajectory_set = [traj_set]
            observed_msg.objects.append(out_obj)

        if ego_anchor_state is not None:

            out_obj = TrackedObject2WithTrajectory()
            out_obj.trajectory_set = []
            _apply_state_to_traj_obj_object(out_obj.object, ego_anchor_state, ego_internal_id)

            traj_set = TrajectoryStateSet()
            if hasattr(traj_set, "prediction_mode"):
                traj_set.prediction_mode = 0
            traj_set.trajectory = []

            sampled_states_by_step: dict[int, tuple[float, float, float, float]] = {0: ego_anchor_state}
            last_known_step = 0
            last_known_state = ego_anchor_state
            search_start_idx = i

            step_i = 1
            while step_i <= horizon_steps:
                found_step = None
                found_idx = None
                found_state = None
                search_end_step = horizon_steps
                for cand_step in range(step_i, search_end_step + 1):
                    target_ns = anchor_stamp_ns + cand_step * step_ns
                    sample_idx = _find_sample_index_for_ego_state(
                        records,
                        stamps,
                        search_start_idx,
                        target_ns,
                        tolerance_ns,
                    )
                    if sample_idx is None:
                        continue
                    sample_state = records[sample_idx].get("ego_state")
                    if sample_state is None:
                        continue
                    found_step = cand_step
                    found_idx = sample_idx
                    found_state = sample_state
                    break

                if found_step is None or found_state is None:
                    break

                if found_step > last_known_step + 1:
                    denom = float(found_step - last_known_step)
                    for fill_step in range(last_known_step + 1, found_step):
                        ratio = float(fill_step - last_known_step) / denom
                        sampled_states_by_step[fill_step] = _interpolate_state(last_known_state, found_state, ratio)

                sampled_states_by_step[found_step] = found_state
                last_known_step = found_step
                last_known_state = found_state
                search_start_idx = max(search_start_idx, found_idx)
                step_i = found_step + 1

            for sampled_step in sorted(sampled_states_by_step.keys()):
                x, y, yaw, speed = sampled_states_by_step[sampled_step]
                pt = TrajectoryState()
                if hasattr(pt, "t"):
                    pt.t = float(sampled_step * OBSERVED_STEP_SEC)
                if hasattr(pt, "x"):
                    pt.x = x
                if hasattr(pt, "y"):
                    pt.y = y
                if hasattr(pt, "yaw"):
                    pt.yaw = yaw
                if hasattr(pt, "velocity"):
                    pt.velocity = speed
                if hasattr(pt, "covariance"):
                    pt.covariance = [0.0] * 16
                traj_set.trajectory.append(pt)

            if traj_set.trajectory:
                out_obj.trajectory_set = [traj_set]
            observed_msg.objects.append(out_obj)

        observed_base_records.append((rec["t"], observed_msg))

    observed_topics = {
        "base": f"{observed_ns}{WM_TOPIC_SUFFIXES[0]}",
        "along": f"{observed_ns}{WM_TOPIC_SUFFIXES[1]}",
        "crossing": f"{observed_ns}{WM_TOPIC_SUFFIXES[2]}",
        "oncoming": f"{observed_ns}{WM_TOPIC_SUFFIXES[3]}",
        "other": f"{observed_ns}{WM_TOPIC_SUFFIXES[4]}",
    }

    observed_bag.parent.mkdir(parents=True, exist_ok=True)
    with rosbag.Bag(str(observed_bag), "w") as out_bag:
        for t, base_msg in observed_base_records:
            out_bag.write(observed_topics["base"], base_msg, t)

            header = getattr(base_msg, "header", None)
            stamp_ns = _to_nsec(getattr(header, "stamp", None))
            for group in ("along", "crossing", "oncoming", "other"):
                group_msg = TrackedObjectSet2WithTrajectory()
                group_msg.header = copy.deepcopy(base_msg.header)
                if hasattr(group_msg, "status") and hasattr(base_msg, "status"):
                    group_msg.status = base_msg.status
                if hasattr(group_msg, "status_message") and hasattr(base_msg, "status_message"):
                    group_msg.status_message = base_msg.status_message
                keep_ids = group_ids_by_stamp.get(group, {}).get(stamp_ns, set())
                group_msg.objects = []
                if keep_ids:
                    for obj in getattr(base_msg, "objects", []) or []:
                        oid = _extract_object_id_from_traj_obj(obj)
                        if oid in keep_ids:
                            group_msg.objects.append(copy.deepcopy(obj))
                out_bag.write(observed_topics[group], group_msg, t)

    return source_topic


def main() -> int:
    root = get_tester_root()
    settings = load_settings(root)
    paths = settings["paths"]
    test_bags_root = root / paths["test_bags"]
    baseline_root = root / paths["baseline_results"]
    rosparam = settings.get("rosparam", {})
    node_cfg = settings.get("node", {})
    pkg = node_cfg.get("trajectory_predictor_pkg", "nrc_wm_svcs")
    node_name = node_cfg.get("trajectory_predictor_node", "trajectory_predictor")
    observed_source_topic = settings.get("observed_source_topic")
    use_sim_offline = (node_name == "trajectory_predictor_sim_offline")

    # 0. Build
    if os.environ.get("TP_SKIP_BUILD", "").strip().lower() in ("1", "true", "yes"):
        print("[baseline] TP_SKIP_BUILD=1: ビルドをスキップ")
    else:
        print("[baseline] building nrc_wm_svcs...")
        if not run_catkin_build(settings):
            print("catkin_make --pkg nrc_wm_svcs failed.", file=sys.stderr)
            return 1
        print("[baseline] build OK")

    # 1. Clear baseline_results（.gitkeep は残す）
    if baseline_root.exists():
        for p in baseline_root.rglob("*"):
            if p.is_file() and p.name != ".gitkeep":
                p.unlink()
        for p in sorted(baseline_root.rglob("*"), key=lambda x: -len(x.parts)):
            if p.is_dir():
                p.rmdir()
    baseline_root.mkdir(parents=True, exist_ok=True)

    commit_info = get_commit_info_for_run(settings, "TP_BASELINE_COMMIT")
    with open(baseline_root / "commit_info.json", "w", encoding="utf-8") as f:
        json.dump(commit_info, f, indent=2, ensure_ascii=False)
    print(f"[baseline] commit: {commit_info.get('commit', 'unknown')} ({commit_info.get('describe', '')})")

    dirs = discover_bag_directories(test_bags_root)
    if not dirs:
        print("No bag directories found under", test_bags_root, file=sys.stderr)
        return 1

    # 2. use_sim_time / map_name はノード起動前に1回だけ設定（use_sim_time は ros::init() 前でないと効かない）
    use_sim = str(rosparam.get("use_sim_time", True)).lower()
    subprocess.run(
        ["rosparam", "set", "/use_sim_time", use_sim],
        check=True, capture_output=True,
    )
    subprocess.run(
        ["rosparam", "set", "/map_name", rosparam.get("map_name", "new_MM_map")],
        check=True, capture_output=True,
    )
    r = subprocess.run(["rosparam", "get", "/use_sim_time"], capture_output=True, text=True, timeout=5)
    print(f"[baseline] /use_sim_time = {r.stdout.strip() if r.returncode == 0 else 'get failed'} (offline executable will use this)")

    if not use_sim_offline:
        print(
            f"[baseline] node.trajectory_predictor_node={node_name} は未対応です。"
            " trajectory_predictor_sim_offline を設定してください。",
            file=sys.stderr,
        )
        return 1

    for rel, dir_path in dirs:
        bags = get_bag_files_in_dir(dir_path)
        if not bags:
            continue

        selected_bags, matched_topics = select_bag_files_by_topics(
            bags, TP_SIM_OFFLINE_INPUT_TOPICS
        )
        if "/target_tracker/tracked_object_set2" not in matched_topics:
            print(
                f"[baseline] {rel}: /target_tracker/tracked_object_set2 を含む bag が見つかりません",
                file=sys.stderr,
            )
            return 1
        if len(selected_bags) != len(bags):
            print(
                f"[baseline] {rel}: using {len(selected_bags)}/{len(bags)} bag(s)"
                f" (matched topics={sorted(matched_topics)})"
            )

        out_dir = baseline_root / rel
        out_dir.mkdir(parents=True, exist_ok=True)
        out_bag = out_dir / "result_baseline.bag"
        if out_bag.exists():
            out_bag.unlink()

        print(f"[baseline] {rel}: offline processing {len(selected_bags)} bag(s) -> {out_bag}")
        rc, dt_values, run_log = run_trajectory_predictor_offline(
            pkg=pkg,
            node_name=node_name,
            input_bags=selected_bags,
            output_bag=out_bag,
            output_ns=VALIDATION_BASELINE_NS,
        )
        if run_log:
            (out_dir / "tp_run.log").write_text(run_log, encoding="utf-8")

        max_dt = max(dt_values) if dt_values else None
        if any(v >= 100 for v in dt_values):
            (out_dir / "dt_status").write_text("invalid")
        elif any(v >= 70 for v in dt_values):
            (out_dir / "dt_status").write_text("warning")
        else:
            (out_dir / "dt_status").write_text("valid")
        (out_dir / "dt_max").write_text(str(max_dt) if max_dt is not None else "-")

        if rc != 0:
            (out_dir / "segfault").touch()
            (out_dir / "tp_returncode").write_text(str(rc), encoding="utf-8")
            print(
                f"[baseline] {rel}: trajectory_predictor_sim_offline failed (returncode={rc})",
                file=sys.stderr,
            )
            continue

        if not out_bag.exists() or out_bag.stat().st_size <= 0:
            print(f"[baseline] {rel}: result_baseline.bag が作成されていません: {out_bag}", file=sys.stderr)
            return 1

        observed_bag = out_dir / "observed_baseline.bag"
        try:
            used_topic = _build_observed_bag_from_input(
                selected_bags,
                observed_bag,
                result_bag=out_bag,
                observed_ns=VALIDATION_OBSERVED_BASELINE_NS,
                result_ns=VALIDATION_BASELINE_NS,
                preferred_source_topic=observed_source_topic,
            )
            print(f"[baseline] {rel}: observed trajectories saved to {observed_bag} (source: {used_topic})")
        except Exception as e:
            print(f"[baseline] {rel}: observed_baseline.bag 作成に失敗: {e}", file=sys.stderr)
            return 1

    print("[baseline] Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
