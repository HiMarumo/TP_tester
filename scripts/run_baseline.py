#!/usr/bin/env python3
"""
Baseline creation: trajectory_predictor を1回起動し、全 test_bags ディレクトリを順次再生・記録。
segfault 等で不正終了した場合は該当 dir に segfault タグを付け、次の dir の前に再起動する。
"""
from __future__ import annotations

import bisect
import copy
import json
import math
import os
import signal
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path

from common import (
    VALIDATION_BASELINE_NS,
    discover_bag_directories,
    get_bag_files_in_dir,
    get_bag_start_time,
    get_commit_info_for_run,
    get_tester_root,
    kill_rosnodes_matching,
    load_settings,
    run_catkin_build,
    run_clock_preroll,
    start_clock_publisher,
    start_trajectory_predictor_and_wait_ready,
    wait_for_node_ready,
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
TP_SIM_INPUT_FRAME_TOPIC = "/tp_sim/input_frame"
OBSERVED_HORIZON_SEC = 10.0
OBSERVED_STEP_SEC = 0.5
# 10Hz 前提で 0.5s サンプル時の揺らぎ許容（若干の stamp ずれを吸収）
OBSERVED_SAMPLE_TOLERANCE_SEC = 0.26
OBSERVED_SOURCE_TOPIC_CANDIDATES = [
    TP_SIM_INPUT_FRAME_TOPIC,
    "/target_tracker/tracked_object_set2",
    "/sensor_fusion/tracked_object_set2",
    "/local_model/tracked_object_set2",
]


def run_cmd(cmd: list[str], env: dict | None = None, timeout: float | None = None) -> subprocess.Popen:
    return subprocess.Popen(
        cmd,
        env=env or os.environ,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        preexec_fn=os.setsid if os.name != "nt" else None,
    )


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


def _resolve_wait_publish_count_target(
    input_bags: list[Path], preferred_topic: str | None = None
) -> tuple[str, int]:
    if not HAS_ROSBAG:
        raise RuntimeError("python rosbag is not available")
    topic_counts = _collect_tracked_object_set2_topic_counts(input_bags)
    source_topic = _resolve_observed_source_topic(input_bags, preferred_topic, topic_counts=topic_counts)
    expected_count = int(topic_counts.get(source_topic, 0) or 0)
    if expected_count <= 0:
        raise RuntimeError(f"expected_count is zero for source topic: {source_topic}")
    return source_topic, expected_count


def _count_topic_messages_in_bag(bag_path: Path, topic: str) -> int:
    if not bag_path.exists():
        return 0
    with rosbag.Bag(str(bag_path), "r") as bag:
        info = bag.get_type_and_topic_info()
        topics_info = getattr(info, "topics", {}) or {}
        tinf = topics_info.get(topic)
        if tinf is None:
            return 0
        return int(getattr(tinf, "message_count", 0) or 0)


def _start_create_tp_sim_input(pkg: str) -> subprocess.Popen:
    kill_rosnodes_matching("create_tp_sim_input")
    time.sleep(0.2)

    create_cmd = [
        "rosrun",
        pkg,
        "create_tp_sim_input",
        f"_framed_output_topic_str:={TP_SIM_INPUT_FRAME_TOPIC}",
    ]
    create_proc, ready_ok, _ = start_trajectory_predictor_and_wait_ready(create_cmd, timeout=20.0)
    if create_proc is None:
        create_proc = subprocess.Popen(
            create_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=os.environ,
            preexec_fn=os.setsid if os.name != "nt" else None,
        )
        ready_ok = wait_for_node_ready(create_proc, timeout=20.0)
    if not ready_ok:
        if create_proc is not None and create_proc.poll() is None:
            try:
                os.killpg(os.getpgid(create_proc.pid), signal.SIGTERM)
            except (ProcessLookupError, AttributeError, OSError):
                create_proc.terminate()
            try:
                create_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
        raise RuntimeError("create_tp_sim_input did not become ready")
    return create_proc


def _stop_process(proc: subprocess.Popen | None) -> None:
    if proc is None or proc.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except (ProcessLookupError, AttributeError, OSError):
        proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass


def _create_input_bag_for_scene(
    root: Path,
    source_bags: list[Path],
    input_bag: Path,
    preferred_source_topic: str | None = None,
) -> tuple[str, int]:
    source_topic, expected_count = _resolve_wait_publish_count_target(source_bags, preferred_source_topic)
    input_bag.parent.mkdir(parents=True, exist_ok=True)
    record_cmd = ["rosbag", "record", "-O", str(input_bag), TP_SIM_INPUT_FRAME_TOPIC]
    rec_proc = run_cmd(record_cmd)
    time.sleep(1.0)

    bag_start = get_bag_start_time(source_bags[0])
    if bag_start is not None:
        run_clock_preroll(bag_start, duration_sec=2.0)

    script_dir = root / "scripts"
    wait_count_proc = None
    wait_count_started = False
    wait_count_script = script_dir / "wait_publish_count.py"
    if expected_count > 0 and wait_count_script.exists():
        wait_cmd = [
            sys.executable,
            str(wait_count_script),
            TP_SIM_INPUT_FRAME_TOPIC,
            str(expected_count),
            "--timeout-sec",
            "120",
            "--settle-sec",
            "0.3",
        ]
        wait_count_proc = subprocess.Popen(
            wait_cmd,
            env=os.environ,
            preexec_fn=os.setsid if os.name != "nt" else None,
        )
        wait_count_started = True

    play_cmd = ["rosbag", "play"] + [str(b) for b in source_bags] + ["--clock"]
    subprocess.run(play_cmd)

    waited = False
    if wait_count_started and wait_count_proc is not None:
        try:
            rc = wait_count_proc.wait(timeout=130.0)
        except subprocess.TimeoutExpired:
            rc = -1
            try:
                os.killpg(os.getpgid(wait_count_proc.pid), signal.SIGTERM)
            except (ProcessLookupError, AttributeError, OSError):
                wait_count_proc.terminate()
            try:
                wait_count_proc.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                pass
        waited = (rc == 0)
        if not waited:
            print(
                f"[baseline] input.bag wait_publish_count failed (rc={rc})"
                " -> silence待ちにフォールバック",
                file=sys.stderr,
            )
    if not waited:
        wait_script = script_dir / "wait_publish_silence.py"
        wait_cmd = [sys.executable, str(wait_script), TP_SIM_INPUT_FRAME_TOPIC, "--silence-sec", "1"]
        subprocess.run(wait_cmd)

    if rec_proc.poll() is None:
        try:
            os.killpg(os.getpgid(rec_proc.pid), signal.SIGINT)
        except (ProcessLookupError, AttributeError):
            rec_proc.terminate()
        rec_proc.wait(timeout=5)

    for _ in range(30):
        if input_bag.exists() and input_bag.stat().st_size > 0:
            break
        time.sleep(0.1)
    if not input_bag.exists() or input_bag.stat().st_size <= 0:
        raise RuntimeError(f"input bag not created: {input_bag}")

    input_count = _count_topic_messages_in_bag(input_bag, TP_SIM_INPUT_FRAME_TOPIC)
    if input_count <= 0:
        raise RuntimeError(f"input bag has no frame messages on {TP_SIM_INPUT_FRAME_TOPIC}: {input_bag}")

    return source_topic, input_count


def _collect_observed_source_records(input_bags: list[Path], source_topic: str) -> list[dict]:
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

        observed_msg = TrackedObjectSet2WithTrajectory()
        observed_msg.header = copy.deepcopy(anchor_msg.header)
        if hasattr(observed_msg, "status") and hasattr(anchor_msg, "status"):
            observed_msg.status = anchor_msg.status
        if hasattr(observed_msg, "status_message") and hasattr(anchor_msg, "status_message"):
            observed_msg.status_message = anchor_msg.status_message
        observed_msg.objects = []

        for anchor_obj in getattr(anchor_msg, "objects", []) or []:
            anchor_oid = int(getattr(anchor_obj, "object_id", -1))
            if anchor_oid < 0:
                continue

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
    record_topics = settings["record_topics"]
    rosparam = settings.get("rosparam", {})
    node_cfg = settings.get("node", {})
    pkg = node_cfg.get("trajectory_predictor_pkg", "nrc_wm_svcs")
    node_name = node_cfg.get("trajectory_predictor_node", "trajectory_predictor")
    create_input_pkg = node_cfg.get("trajectory_predictor_pkg", pkg)
    observed_source_topic = settings.get("observed_source_topic")

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
    print(f"[baseline] /use_sim_time = {r.stdout.strip() if r.returncode == 0 else 'get failed'} (node will use this when started)")

    remap_args = [f"{t}:={VALIDATION_BASELINE_NS}{t}" for t in record_topics]
    baseline_record_topics = [f"{VALIDATION_BASELINE_NS}{t}" for t in record_topics]
    tp_cmd = ["rosrun", pkg, node_name] + remap_args
    tp_proc = None
    capture_state = None
    create_input_proc = None
    sim_input_meta: dict[str, dict[str, object]] = {}

    try:
        if node_name == "trajectory_predictor_sim":
            try:
                create_input_proc = _start_create_tp_sim_input(create_input_pkg)
                print("[baseline] create_tp_sim_input ready (persistent mode).")
            except Exception as e:
                print(f"[baseline] create_tp_sim_input 起動に失敗: {e}", file=sys.stderr)
                return 1

            # Phase 1: create input.bag for all scenes first.
            for rel, dir_path in dirs:
                bags = get_bag_files_in_dir(dir_path)
                if not bags:
                    continue
                out_dir = baseline_root / rel
                out_dir.mkdir(parents=True, exist_ok=True)
                input_bag = out_dir / "input.bag"
                try:
                    expected_source_topic, expected_output_count = _create_input_bag_for_scene(
                        root=root,
                        source_bags=bags,
                        input_bag=input_bag,
                        preferred_source_topic=observed_source_topic,
                    )
                    sim_input_meta[rel] = {
                        "input_bag": input_bag,
                        "expected_output_count": expected_output_count,
                        "expected_source_topic": expected_source_topic,
                    }
                    print(
                        f"[baseline] {rel}: input.bag ready ({TP_SIM_INPUT_FRAME_TOPIC}={expected_output_count})"
                        f" source={expected_source_topic}"
                    )
                except Exception as e:
                    print(f"[baseline] {rel}: input.bag 作成に失敗: {e}", file=sys.stderr)
                    return 1

            _stop_process(create_input_proc)
            create_input_proc = None
            print("[baseline] input.bag generation phase completed for all scenes.")

        for rel, dir_path in dirs:
            bags = get_bag_files_in_dir(dir_path)
            if not bags:
                continue
            out_dir = baseline_root / rel
            out_dir.mkdir(parents=True, exist_ok=True)
            out_bag = out_dir / "result_baseline.bag"
            input_bag = out_dir / "input.bag"

            play_bags = bags
            expected_output_count = None
            if node_name == "trajectory_predictor_sim":
                meta = sim_input_meta.get(rel)
                if not meta:
                    print(f"[baseline] {rel}: input.bag メタ情報がありません。", file=sys.stderr)
                    return 1
                input_bag = Path(str(meta["input_bag"]))
                expected_output_count = int(meta["expected_output_count"])
                play_bags = [input_bag]

            # 前の再生で trajectory_predictor が落ちていたら再起動
            if tp_proc is None or tp_proc.poll() is not None:
                kill_rosnodes_matching(node_name)
                time.sleep(0.5)
                clock_proc = None
                if os.environ.get("TP_PUBLISH_CLOCK", "").strip().lower() in ("1", "true", "yes"):
                    clock_proc = start_clock_publisher()
                    if clock_proc is not None:
                        time.sleep(1.0)
                tp_proc, ready_ok, cap = start_trajectory_predictor_and_wait_ready(tp_cmd, timeout=30.0)
                if cap is not None:
                    capture_state = cap
                if tp_proc is None:
                    tp_proc = subprocess.Popen(
                        tp_cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        bufsize=1,
                        env=os.environ,
                        preexec_fn=os.setsid if os.name != "nt" else None,
                    )
                    ready_ok = wait_for_node_ready(tp_proc, timeout=30.0)
                if not ready_ok:
                    if clock_proc is not None and clock_proc.poll() is None:
                        clock_proc.terminate()
                        clock_proc.wait(timeout=2)
                    if tp_proc.poll() is None:
                        try:
                            os.killpg(os.getpgid(tp_proc.pid), signal.SIGTERM)
                        except (ProcessLookupError, AttributeError, OSError):
                            tp_proc.terminate()
                        tp_proc.wait(timeout=5)
                    print(f"[baseline] trajectory_predictor が 'Ready. Waiting for data' を出さずタイムアウト。", file=sys.stderr)
                    return 1
                if clock_proc is not None and clock_proc.poll() is None:
                    clock_proc.terminate()
                    clock_proc.wait(timeout=2)
                print(f"[baseline] trajectory_predictor ready (start/restart).")
                if capture_state is not None:
                    capture_state.forward_to_terminal = False

            if capture_state is not None:
                capture_state.clear_dt()
            print(f"[baseline] {rel}: playing {len(play_bags)} bag(s), recording to {out_bag}")
            record_cmd = ["rosbag", "record", "-O", str(out_bag)] + baseline_record_topics
            rec_proc = run_cmd(record_cmd)
            time.sleep(1.0)
            # タイマー位相を揃える: bag の先頭時刻で /clock を 2s 分 publish してから再生（10Hz×2s=20 ティックで位相を安定）
            bag_start = get_bag_start_time(play_bags[0])
            if bag_start is not None:
                run_clock_preroll(bag_start, duration_sec=2.0)
            script_dir = root / "scripts"
            wait_count_proc = None
            wait_count_started = False
            if node_name == "trajectory_predictor_sim":
                wait_count_script = script_dir / "wait_publish_count.py"
                if expected_output_count is not None and expected_output_count > 0 and wait_count_script.exists():
                    wait_cmd = [
                        sys.executable,
                        str(wait_count_script),
                        baseline_record_topics[0],
                        str(expected_output_count),
                        "--timeout-sec",
                        "120",
                        "--settle-sec",
                        "0.3",
                    ]
                    wait_count_proc = subprocess.Popen(
                        wait_cmd,
                        env=os.environ,
                        preexec_fn=os.setsid if os.name != "nt" else None,
                    )
                    wait_count_started = True
            play_cmd = ["rosbag", "play"] + [str(b) for b in play_bags] + ["--clock"]
            subprocess.run(play_cmd)
            # trajectory_predictor_sim は件数待ちを優先し、失敗時のみ silence 待ちへフォールバック
            if node_name == "trajectory_predictor_sim":
                waited = False
                if wait_count_started and wait_count_proc is not None:
                    try:
                        rc = wait_count_proc.wait(timeout=130.0)
                    except subprocess.TimeoutExpired:
                        rc = -1
                        try:
                            os.killpg(os.getpgid(wait_count_proc.pid), signal.SIGTERM)
                        except (ProcessLookupError, AttributeError, OSError):
                            wait_count_proc.terminate()
                        try:
                            wait_count_proc.wait(timeout=2.0)
                        except subprocess.TimeoutExpired:
                            pass
                    waited = (rc == 0)
                    if not waited:
                        print(
                            f"[baseline] {rel}: wait_publish_count failed (rc={rc})"
                            " -> silence待ちにフォールバック",
                            file=sys.stderr,
                        )
                if not waited:
                    wait_script = script_dir / "wait_publish_silence.py"
                    wait_cmd = [sys.executable, str(wait_script), baseline_record_topics[0], "--silence-sec", "1"]
                    subprocess.run(wait_cmd)
            else:
                time.sleep(0.5)
            if rec_proc.poll() is None:
                try:
                    os.killpg(os.getpgid(rec_proc.pid), signal.SIGINT)
                except (ProcessLookupError, AttributeError):
                    rec_proc.terminate()
                rec_proc.wait(timeout=5)

            for _ in range(30):
                if out_bag.exists() and out_bag.stat().st_size > 0:
                    break
                time.sleep(0.1)
            if not out_bag.exists() or out_bag.stat().st_size <= 0:
                print(f"[baseline] {rel}: result_baseline.bag が作成されていません: {out_bag}", file=sys.stderr)
                return 1

            if capture_state is not None:
                values = capture_state.take_dt_values()
                max_dt = max(values) if values else None
                if any(v >= 100 for v in values):
                    (out_dir / "dt_status").write_text("invalid")
                elif any(v >= 70 for v in values):
                    (out_dir / "dt_status").write_text("warning")
                else:
                    (out_dir / "dt_status").write_text("valid")
                (out_dir / "dt_max").write_text(str(max_dt) if max_dt is not None else "-")

            observed_bag = out_dir / "observed_baseline.bag"
            try:
                used_topic = _build_observed_bag_from_input(
                    [input_bag] if node_name == "trajectory_predictor_sim" else bags,
                    observed_bag,
                    result_bag=out_bag,
                    observed_ns=VALIDATION_OBSERVED_BASELINE_NS,
                    result_ns=VALIDATION_BASELINE_NS,
                    preferred_source_topic=TP_SIM_INPUT_FRAME_TOPIC if node_name == "trajectory_predictor_sim" else observed_source_topic,
                )
                print(f"[baseline] {rel}: observed trajectories saved to {observed_bag} (source: {used_topic})")
            except Exception as e:
                print(f"[baseline] {rel}: observed_baseline.bag 作成に失敗: {e}", file=sys.stderr)
                return 1

            # 不正終了していたら該当 dir に segfault タグを付け（次のループで再起動する）
            if tp_proc.poll() is not None:
                (out_dir / "segfault").touch()
                print(f"[baseline] {rel}: trajectory_predictor が不正終了 (returncode={tp_proc.returncode})。segfault タグを付け、次回再生前に再起動します。", file=sys.stderr)
    finally:
        _stop_process(tp_proc)
        _stop_process(create_input_proc)

    print("[baseline] Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
