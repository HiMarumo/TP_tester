#!/usr/bin/env python3
"""Evaluate existing baseline/test result bags and aggregate comparison results."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from common import (
    VALIDATION_BASELINE_NS,
    VALIDATION_TEST_NS,
    get_tester_root,
    load_settings,
)
from evaluate_observed_validation import evaluate_validation_for_side
from run_baseline import VALIDATION_OBSERVED_BASELINE_NS
from run_test import VALIDATION_OBSERVED_TEST_NS


def _iter_scene_dirs(root: Path, result_bag_name: str) -> list[tuple[str, Path]]:
    scenes: list[tuple[str, Path]] = []
    if not root.exists():
        return scenes
    for bag_path in sorted(root.rglob(result_bag_name)):
        out_dir = bag_path.parent
        rel = out_dir.relative_to(root).as_posix()
        scenes.append((rel, out_dir))
    return scenes


def _load_scene_timing(out_dir: Path) -> dict | None:
    scene_timing_path = out_dir / "scene_timing.json"
    if not scene_timing_path.exists():
        return None
    try:
        return json.loads(scene_timing_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _evaluate_side(
    *,
    side: str,
    results_root: Path,
    result_bag_name: str,
    observed_bag_name: str,
    result_ns: str,
    observed_ns: str,
) -> int:
    scenes = _iter_scene_dirs(results_root, result_bag_name)
    if not scenes:
        print(f"[evaluation] {side}: result bag が見つかりません: {results_root}", file=sys.stderr)
        return 1

    for rel, out_dir in scenes:
        result_bag = out_dir / result_bag_name
        observed_bag = out_dir / observed_bag_name
        if not result_bag.exists() or result_bag.stat().st_size <= 0:
            print(f"[evaluation] {side} {rel}: result bag が不正です: {result_bag}", file=sys.stderr)
            return 1
        if not observed_bag.exists() or observed_bag.stat().st_size <= 0:
            print(f"[evaluation] {side} {rel}: observed bag が不正です: {observed_bag}", file=sys.stderr)
            return 1
        scene_timing = _load_scene_timing(out_dir)
        summary_path = out_dir / f"validation_{side}_summary.json"
        if summary_path.exists():
            summary_path.unlink()
        print(f"[evaluation] {side} {rel}: evaluating {result_bag_name} vs {observed_bag_name}")
        try:
            validation_summary = evaluate_validation_for_side(
                rel=rel,
                out_dir=out_dir,
                side=side,
                result_bag=result_bag,
                observed_bag=observed_bag,
                result_ns=result_ns,
                observed_ns=observed_ns,
                scene_timing=scene_timing,
            )
        except Exception as e:
            print(f"[evaluation] {side} {rel}: observed評価に失敗: {e}", file=sys.stderr)
            return 1
        summary_path.write_text(
            json.dumps(validation_summary, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    return 0


def _run_script(root: Path, script_name: str, label: str) -> int:
    script_path = root / "scripts" / script_name
    if not script_path.exists():
        print(f"[evaluation] {label} script not found: {script_path}", file=sys.stderr)
        return 1
    print(f"[evaluation] Running {label}...")
    env = os.environ.copy()
    env["TP_TESTER_ROOT"] = str(root)
    completed = subprocess.run([sys.executable, str(script_path)], cwd=root, env=env, check=False)
    return int(completed.returncode)


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate existing TP_tester result bags.")
    parser.add_argument(
        "--side",
        choices=("baseline", "test", "all"),
        default="all",
        help="Which side to evaluate. default: all",
    )
    compare_group = parser.add_mutually_exclusive_group()
    compare_group.add_argument(
        "--compare",
        dest="compare",
        action="store_true",
        help="Run baseline/test comparison and aggregation after evaluation.",
    )
    compare_group.add_argument(
        "--skip-compare",
        dest="compare",
        action="store_false",
        help="Skip baseline/test comparison and aggregation.",
    )
    parser.add_argument(
        "--compare-only",
        dest="compare_only",
        action="store_true",
        help="Skip evaluation (observed validation). Run only compare and aggregation. Use when evaluation already finished and only comparison is needed (e.g. after OOM at compare).",
    )
    parser.set_defaults(compare=True, compare_only=False)
    args = parser.parse_args()

    root = get_tester_root()
    settings = load_settings(root)
    paths = settings["paths"]
    baseline_root = root / paths["baseline_results"]
    test_root = root / paths["test_results"]

    if not args.compare_only:
        sides = ("baseline", "test") if args.side == "all" else (args.side,)
        for side in sides:
            if side == "baseline":
                rc = _evaluate_side(
                    side="baseline",
                    results_root=baseline_root,
                    result_bag_name="result_baseline.bag",
                    observed_bag_name="observed_baseline.bag",
                    result_ns=VALIDATION_BASELINE_NS,
                    observed_ns=VALIDATION_OBSERVED_BASELINE_NS,
                )
            else:
                rc = _evaluate_side(
                    side="test",
                    results_root=test_root,
                    result_bag_name="result_test.bag",
                    observed_bag_name="observed_test.bag",
                    result_ns=VALIDATION_TEST_NS,
                    observed_ns=VALIDATION_OBSERVED_TEST_NS,
                )
            if rc != 0:
                return rc

    if args.compare or args.compare_only:
        compare_rc = _run_script(root, "compare_baseline_test.py", "comparison")
        if compare_rc not in (0, 1):
            return 1
        if compare_rc == 1:
            print("[evaluation] comparison found changed scenes; continuing to aggregation.")
        if _run_script(root, "aggregate_comparison_results.py", "aggregation") != 0:
            return 1

    print("[evaluation] Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
