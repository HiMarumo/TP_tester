#!/usr/bin/env python3
"""Test run: sceneごとに trajectory_predictor_sim_offline を直接実行し、最後に比較する。"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from common import (
    VALIDATION_TEST_NS,
    build_scene_timing,
    build_subscene_dt_summary,
    check_offline_binary_supports_map_name,
    collect_clock_timeline_in_bags,
    discover_bag_directories,
    get_bag_files_in_dir,
    get_commit_info_for_run,
    get_tester_root,
    load_settings,
    run_catkin_build,
    run_trajectory_predictor_offline,
    select_bag_files_by_topics,
)
from run_baseline import (
    _build_observed_bag_from_input,
    TP_SIM_OFFLINE_INPUT_TOPICS,
)
from evaluate_observed_validation import evaluate_validation_for_side


VALIDATION_OBSERVED_TEST_NS = "/validation/observed_test"


def main() -> int:
    root = get_tester_root()
    settings = load_settings(root)
    paths = settings["paths"]
    test_bags_root = root / paths["test_bags"]
    test_results_root = root / paths["test_results"]
    offline_cfg = settings.get("offline", {})
    map_name = str(
        offline_cfg.get("map_name")
        or settings.get("rosparam", {}).get("map_name")
        or "new_MM_map"
    )
    node_cfg = settings.get("node", {})
    pkg = node_cfg.get("trajectory_predictor_pkg", "nrc_wm_svcs")
    node_name = node_cfg.get("trajectory_predictor_node", "trajectory_predictor")
    observed_source_topic = settings.get("observed_source_topic")
    use_sim_offline = (node_name == "trajectory_predictor_sim_offline")

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

    print(f"[test] map_name = {map_name} (--map-name)")

    if not use_sim_offline:
        print(
            f"[test] node.trajectory_predictor_node={node_name} は未対応です。"
            " trajectory_predictor_sim_offline を設定してください。",
            file=sys.stderr,
        )
        return 1
    supports_map_name, detail = check_offline_binary_supports_map_name(
        pkg,
        node_name,
        progress_prefix="[test] offline binary check",
    )
    if not supports_map_name:
        print(
            "[test] 実行バイナリが必須オプション（--map-name / --runtime-file）非対応です。"
            " 新実装が反映されていないため、TP_SKIP_BUILD=0 で再ビルドしてください。",
            file=sys.stderr,
        )
        if detail:
            print(f"[test] detail: {detail}", file=sys.stderr)
        return 1

    for rel, dir_path in dirs:
        bags = get_bag_files_in_dir(dir_path)
        if not bags:
            continue
        scene_timing = build_scene_timing(
            bags,
            progress_prefix=f"[test] {rel} scene-timing",
        )
        selected_bags, matched_topics = select_bag_files_by_topics(
            bags,
            TP_SIM_OFFLINE_INPUT_TOPICS,
            progress_prefix=f"[test] {rel} input-bag-select",
        )
        if "/target_tracker/tracked_object_set2" not in matched_topics:
            print(
                f"[test] {rel}: /target_tracker/tracked_object_set2 を含む bag が見つかりません",
                file=sys.stderr,
            )
            return 1
        if len(selected_bags) != len(bags):
            print(
                f"[test] {rel}: using {len(selected_bags)}/{len(bags)} bag(s)"
                f" (matched topics={sorted(matched_topics)})"
            )
        out_dir = test_results_root / rel
        out_dir.mkdir(parents=True, exist_ok=True)
        if scene_timing is not None:
            (out_dir / "scene_timing.json").write_text(
                json.dumps(scene_timing, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
        out_bag = out_dir / "result_test.bag"

        if out_bag.exists():
            out_bag.unlink()
        print(f"[test] {rel}: offline processing {len(selected_bags)} bag(s) -> {out_bag}")
        rc, dt_values, run_log = run_trajectory_predictor_offline(
            pkg=pkg,
            node_name=node_name,
            input_bags=selected_bags,
            output_bag=out_bag,
            output_ns=VALIDATION_TEST_NS,
            map_name=map_name,
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
        dt_summary = build_subscene_dt_summary(
            dt_values=dt_values,
            clock_timeline_ns=collect_clock_timeline_in_bags(
                bags,
                progress_prefix=f"[test] {rel} dt-summary /clock",
            ),
            scene_timing=scene_timing,
        )
        (out_dir / "dt_summary.json").write_text(
            json.dumps(dt_summary, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        if rc != 0:
            (out_dir / "segfault").touch()
            (out_dir / "tp_returncode").write_text(str(rc), encoding="utf-8")
            print(
                f"[test] {rel}: trajectory_predictor_sim_offline failed (returncode={rc})",
                file=sys.stderr,
            )
            continue

        if not out_bag.exists() or out_bag.stat().st_size <= 0:
            print(f"[test] {rel}: result_test.bag が作成されていません: {out_bag}", file=sys.stderr)
            return 1

        observed_bag = out_dir / "observed_test.bag"
        try:
            used_topic = _build_observed_bag_from_input(
                selected_bags,
                observed_bag,
                result_bag=out_bag,
                observed_ns=VALIDATION_OBSERVED_TEST_NS,
                result_ns=VALIDATION_TEST_NS,
                preferred_source_topic=observed_source_topic,
            )
            print(f"[test] {rel}: observed trajectories saved to {observed_bag} (source: {used_topic})")
        except Exception as e:
            print(f"[test] {rel}: observed_test.bag 作成に失敗: {e}", file=sys.stderr)
            return 1

        validation_summary_path = out_dir / "validation_test_summary.json"
        try:
            validation_summary = evaluate_validation_for_side(
                rel=rel,
                out_dir=out_dir,
                side="test",
                result_bag=out_bag,
                observed_bag=observed_bag,
                result_ns=VALIDATION_TEST_NS,
                observed_ns=VALIDATION_OBSERVED_TEST_NS,
                scene_timing=scene_timing,
            )
            validation_summary_path.write_text(
                json.dumps(validation_summary, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
        except Exception as e:
            print(f"[test] {rel}: test observed評価に失敗: {e}", file=sys.stderr)
            return 1

    script_dir = root / "scripts"
    compare_script = script_dir / "compare_baseline_test.py"
    aggregate_script = script_dir / "aggregate_comparison_results.py"
    if compare_script.exists():
        print("[test] Running comparison...")
        env = os.environ.copy()
        env["TP_TESTER_ROOT"] = str(root)
        subprocess.run([sys.executable, str(compare_script)], cwd=root, env=env, check=False)
    else:
        print("[test] Comparison script not found, skipping.", file=sys.stderr)

    if aggregate_script.exists():
        print("[test] Aggregating comparison/evaluation JSON...")
        env = os.environ.copy()
        env["TP_TESTER_ROOT"] = str(root)
        subprocess.run([sys.executable, str(aggregate_script)], cwd=root, env=env, check=True)
    else:
        print("[test] Aggregation script not found, skipping.", file=sys.stderr)

    print("[test] Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
