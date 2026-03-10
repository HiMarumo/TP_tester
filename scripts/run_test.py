#!/usr/bin/env python3
"""
Test run: trajectory_predictor を1回起動し、全 test_bags ディレクトリを順次再生・記録。
segfault 等で不正終了した場合は該当 dir に segfault タグを付け、次の dir の前に再起動する。
最後に compare_baseline_test.py で比較。
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
    VALIDATION_TEST_NS,
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


def run_cmd(cmd: list[str], env: dict | None = None) -> subprocess.Popen:
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
    test_results_root = root / paths["test_results"]
    record_topics = settings["record_topics"]
    rosparam = settings.get("rosparam", {})
    node_cfg = settings.get("node", {})
    pkg = node_cfg.get("trajectory_predictor_pkg", "nrc_wm_svcs")
    node_name = node_cfg.get("trajectory_predictor_node", "trajectory_predictor")

    if os.environ.get("TP_SKIP_BUILD", "").strip().lower() in ("1", "true", "yes"):
        print("[test] TP_SKIP_BUILD=1: ビルドをスキップ")
    else:
        print("[test] building nrc_wm_svcs...")
        if not run_catkin_build(settings):
            print("catkin_make --pkg nrc_wm_svcs failed.", file=sys.stderr)
            return 1
        print("[test] build OK")

    dirs = discover_bag_directories(test_bags_root)
    if not dirs:
        print("No bag directories found under", test_bags_root, file=sys.stderr)
        return 1

    if test_results_root.exists():
        for p in test_results_root.rglob("*"):
            if p.is_file() and p.name != ".gitkeep":
                p.unlink()
        for p in sorted(test_results_root.rglob("*"), key=lambda x: -len(x.parts)):
            if p.is_dir():
                p.rmdir()
    test_results_root.mkdir(parents=True, exist_ok=True)

    commit_info = get_commit_info_for_run(settings, "TP_TEST_COMMIT")
    with open(test_results_root / "commit_info.json", "w", encoding="utf-8") as f:
        json.dump(commit_info, f, indent=2, ensure_ascii=False)
    print(f"[test] commit: {commit_info.get('commit', 'unknown')} ({commit_info.get('describe', '')})")

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
    print(f"[test] /use_sim_time = {r.stdout.strip() if r.returncode == 0 else 'get failed'} (node will use this when started)")

    remap_args = [f"{t}:={VALIDATION_TEST_NS}{t}" for t in record_topics]
    test_record_topics = [f"{VALIDATION_TEST_NS}{t}" for t in record_topics]
    tp_cmd = ["rosrun", pkg, node_name] + remap_args
    tp_proc = None
    capture_state = None

    for rel, dir_path in dirs:
        bags = get_bag_files_in_dir(dir_path)
        if not bags:
            continue
        out_dir = test_results_root / rel
        out_dir.mkdir(parents=True, exist_ok=True)
        out_bag = out_dir / "result_test.bag"

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
                print(f"[test] trajectory_predictor が 'Ready. Waiting for data' を出さずタイムアウト。", file=sys.stderr)
                return 1
            if clock_proc is not None and clock_proc.poll() is None:
                clock_proc.terminate()
                clock_proc.wait(timeout=2)
            print(f"[test] trajectory_predictor ready (start/restart).")
            if capture_state is not None:
                capture_state.forward_to_terminal = False

        if capture_state is not None:
            capture_state.clear_dt()
        print(f"[test] {rel}: playing {len(bags)} bag(s), recording to {out_bag}")
        record_cmd = ["rosbag", "record", "-O", str(out_bag)] + test_record_topics
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
            wait_cmd = [sys.executable, str(wait_script), test_record_topics[0], "--silence-sec", "1"]
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

        if tp_proc.poll() is not None:
            (out_dir / "segfault").touch()
            print(f"[test] {rel}: trajectory_predictor が不正終了 (returncode={tp_proc.returncode})。segfault タグを付け、次回再生前に再起動します。", file=sys.stderr)

    if tp_proc is not None and tp_proc.poll() is None:
        tp_proc.terminate()
        tp_proc.wait(timeout=5)

    script_dir = root / "scripts"
    compare_script = script_dir / "compare_baseline_test.py"
    if compare_script.exists():
        print("[test] Running comparison...")
        env = os.environ.copy()
        env["TP_TESTER_ROOT"] = str(root)
        subprocess.run([sys.executable, str(compare_script)], cwd=root, env=env, check=False)
    else:
        print("[test] Comparison script not found, skipping.", file=sys.stderr)

    print("[test] Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
