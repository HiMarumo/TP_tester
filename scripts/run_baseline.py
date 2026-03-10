#!/usr/bin/env python3
"""
Baseline creation: trajectory_predictor を1回起動し、全 test_bags ディレクトリを順次再生・記録。
segfault 等で不正終了した場合は該当 dir に segfault タグを付け、次の dir の前に再起動する。
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
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


def run_cmd(cmd: list[str], env: dict | None = None, timeout: float | None = None) -> subprocess.Popen:
    return subprocess.Popen(
        cmd,
        env=env or os.environ,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        preexec_fn=os.setsid if os.name != "nt" else None,
    )


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
