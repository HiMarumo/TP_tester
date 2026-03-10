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
VALIDATION_OBSERVED_NS = "/validation/observed"
OBSERVED_HORIZON_SEC = 10.0
OBSERVED_STEP_SEC = 0.5
# 10Hz 前提で 0.5s サンプル時の揺らぎ許容（若干の stamp ずれを吸収）
OBSERVED_SAMPLE_TOLERANCE_SEC = 0.26
OBSERVED_SOURCE_TOPIC_CANDIDATES = [
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


def _find_sample_index(stamps: list[int], start_idx: int, target_ns: int, tolerance_ns: int) -> int | None:
    idx = bisect.bisect_left(stamps, target_ns, lo=start_idx)
    candidates = []
    if idx < len(stamps):
        candidates.append(idx)
    if idx - 1 >= start_idx:
        candidates.append(idx - 1)
    if not candidates:
        return None
    best = min(candidates, key=lambda i: abs(stamps[i] - target_ns))
    if abs(stamps[best] - target_ns) > tolerance_ns:
        return None
    return best


def _resolve_observed_source_topic(input_bags: list[Path], preferred_topic: str | None = None) -> str:
    topic_counts: dict[str, int] = defaultdict(int)
    for bag_path in input_bags:
        if not bag_path.exists():
            continue
        with rosbag.Bag(str(bag_path), "r") as bag:
            info = bag.get_type_and_topic_info()
            topics_info = getattr(info, "topics", {}) or {}
            for topic, tinf in topics_info.items():
                msg_type = getattr(tinf, "msg_type", "")
                if msg_type == "nrc_msgs/TrackedObjectSet2":
                    topic_counts[topic] += int(getattr(tinf, "message_count", 0) or 0)

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
    records = []
    for bag_path in input_bags:
        if not bag_path.exists():
            continue
        with rosbag.Bag(str(bag_path), "r") as bag:
            for _, msg, t in bag.read_messages(topics=[source_topic]):
                stamp_ns = _msg_stamp_nsec(msg, t)
                obj_by_id = {}
                for obj in getattr(msg, "objects", []) or []:
                    oid = int(getattr(obj, "object_id", -1))
                    if oid >= 0:
                        obj_by_id[oid] = obj
                records.append(
                    {
                        "stamp_ns": stamp_ns,
                        "bag_t_ns": _to_nsec(t),
                        "msg": msg,
                        "t": t,
                        "obj_by_id": obj_by_id,
                    }
                )
    records.sort(key=lambda r: (r["stamp_ns"], r["bag_t_ns"]))
    return records


def _build_observed_bag_from_input(
    input_bags: list[Path], observed_bag: Path, preferred_source_topic: str | None = None
) -> str:
    if not HAS_ROSBAG:
        raise RuntimeError("python rosbag is not available")
    if not HAS_NRC_MSGS:
        raise RuntimeError("nrc_msgs python messages are not available")
    if not input_bags:
        raise RuntimeError("no input bags to build observed.bag")

    source_topic = _resolve_observed_source_topic(input_bags, preferred_source_topic)
    records = _collect_observed_source_records(input_bags, source_topic)
    if not records:
        raise RuntimeError(f"no messages found on observed source topic: {source_topic}")

    step_ns = int(round(OBSERVED_STEP_SEC * 1.0e9))
    horizon_steps = int(round(OBSERVED_HORIZON_SEC / OBSERVED_STEP_SEC))
    tolerance_ns = int(round(OBSERVED_SAMPLE_TOLERANCE_SEC * 1.0e9))
    stamps = [r["stamp_ns"] for r in records]

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

            for step_i in range(horizon_steps + 1):
                target_ns = anchor_stamp_ns + step_i * step_ns
                sample_idx = _find_sample_index(stamps, i, target_ns, tolerance_ns)
                if sample_idx is None:
                    break

                sample_obj = records[sample_idx]["obj_by_id"].get(anchor_oid)
                if sample_obj is None:
                    break

                pose = getattr(sample_obj, "pose", None)
                pos = getattr(pose, "position", None) if pose is not None else None
                ori = getattr(pose, "orientation", None) if pose is not None else None
                x = float(getattr(pos, "x", 0.0) if pos is not None else 0.0)
                y = float(getattr(pos, "y", 0.0) if pos is not None else 0.0)
                yaw = _yaw_from_quat(ori)

                pt = TrajectoryState()
                if hasattr(pt, "t"):
                    pt.t = float(step_i * OBSERVED_STEP_SEC)
                if hasattr(pt, "x"):
                    pt.x = x
                if hasattr(pt, "y"):
                    pt.y = y
                if hasattr(pt, "yaw"):
                    pt.yaw = yaw
                if hasattr(pt, "velocity"):
                    pt.velocity = float(_speed_from_object(sample_obj))
                if hasattr(pt, "covariance"):
                    pt.covariance = [0.0] * 16
                traj_set.trajectory.append(pt)

            if traj_set.trajectory:
                out_obj.trajectory_set = [traj_set]
            observed_msg.objects.append(out_obj)

        observed_base_records.append((rec["t"], observed_msg))

    observed_topics = {
        "base": f"{VALIDATION_OBSERVED_NS}{WM_TOPIC_SUFFIXES[0]}",
        "along": f"{VALIDATION_OBSERVED_NS}{WM_TOPIC_SUFFIXES[1]}",
        "crossing": f"{VALIDATION_OBSERVED_NS}{WM_TOPIC_SUFFIXES[2]}",
        "oncoming": f"{VALIDATION_OBSERVED_NS}{WM_TOPIC_SUFFIXES[3]}",
        "other": f"{VALIDATION_OBSERVED_NS}{WM_TOPIC_SUFFIXES[4]}",
    }

    observed_bag.parent.mkdir(parents=True, exist_ok=True)
    with rosbag.Bag(str(observed_bag), "w") as out_bag:
        for t, base_msg in observed_base_records:
            out_bag.write(observed_topics["base"], base_msg, t)

            # 観測はカテゴリ分類を持たないため、群別トピックは同stampの空メッセージを配置して同期だけ揃える
            empty_msg = TrackedObjectSet2WithTrajectory()
            empty_msg.header = copy.deepcopy(base_msg.header)
            if hasattr(empty_msg, "status") and hasattr(base_msg, "status"):
                empty_msg.status = base_msg.status
            if hasattr(empty_msg, "status_message") and hasattr(base_msg, "status_message"):
                empty_msg.status_message = base_msg.status_message
            empty_msg.objects = []
            out_bag.write(observed_topics["along"], empty_msg, t)
            out_bag.write(observed_topics["crossing"], empty_msg, t)
            out_bag.write(observed_topics["oncoming"], empty_msg, t)
            out_bag.write(observed_topics["other"], empty_msg, t)

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

    for rel, dir_path in dirs:
        bags = get_bag_files_in_dir(dir_path)
        if not bags:
            continue
        out_dir = baseline_root / rel
        out_dir.mkdir(parents=True, exist_ok=True)
        out_bag = out_dir / "result_baseline.bag"

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
        print(f"[baseline] {rel}: playing {len(bags)} bag(s), recording to {out_bag}")
        record_cmd = ["rosbag", "record", "-O", str(out_bag)] + baseline_record_topics
        rec_proc = run_cmd(record_cmd)
        time.sleep(1.0)
        # タイマー位相を揃える: bag の先頭時刻で /clock を 2s 分 publish してから再生（10Hz×2s=20 ティックで位相を安定）
        bag_start = get_bag_start_time(bags[0])
        if bag_start is not None:
            run_clock_preroll(bag_start, duration_sec=2.0)
        play_cmd = ["rosbag", "play"] + [str(b) for b in bags] + ["--clock"]
        subprocess.run(play_cmd)
        # trajectory_predictor_sim は逐次実行のため、1秒間 publish がなければ記録終了
        if node_name == "trajectory_predictor_sim":
            script_dir = root / "scripts"
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
                (out_dir / "dt_status").write_text("normal")
            (out_dir / "dt_max").write_text(str(max_dt) if max_dt is not None else "-")

        observed_bag = out_dir / "observed.bag"
        try:
            used_topic = _build_observed_bag_from_input(
                bags, observed_bag, preferred_source_topic=observed_source_topic
            )
            print(f"[baseline] {rel}: observed trajectories saved to {observed_bag} (source: {used_topic})")
        except Exception as e:
            print(f"[baseline] {rel}: observed.bag 作成に失敗: {e}", file=sys.stderr)
            return 1

        # 不正終了していたら該当 dir に segfault タグを付け（次のループで再起動する）
        if tp_proc.poll() is not None:
            (out_dir / "segfault").touch()
            print(f"[baseline] {rel}: trajectory_predictor が不正終了 (returncode={tp_proc.returncode})。segfault タグを付け、次回再生前に再起動します。", file=sys.stderr)

    if tp_proc is not None and tp_proc.poll() is None:
        tp_proc.terminate()
        tp_proc.wait(timeout=5)

    print("[baseline] Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
