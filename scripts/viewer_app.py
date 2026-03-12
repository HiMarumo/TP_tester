#!/usr/bin/env python3
"""
Verification result viewer GUI.
Left: list of data (date/time) with ①~④ diff status.
Right: TrajectoryPredictorViewer の可視化エリア（Launch で埋め込み）＋ Detail ＋ Play/Stop。
Viewer は別ウィンドウで起動します（同一アプリ内への埋め込みは行いません）。
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
    VALIDATION_TEST_NS,
    get_bag_files_in_dir,
    get_tester_root,
    load_settings,
)

# PyQt5 前提（Docker で python3-pyqt5 を入れている）
from PyQt5.QtWidgets import (
    QApplication,
    QComboBox,
    QFrame,
    QHeaderView,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QStyledItemDelegate,
    QStyleOptionViewItem,
    QStyle,
    QVBoxLayout,
    QWidget,
)
from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtGui import QColor, QBrush, QPalette


# Column headers for comparison table (row 0 = what each column means)
TABLE_HEADERS = ["", "Directory", "Valid/Total", "Lane IDs", "VSL", "Object IDs", "Path", "Traffic", "Seg fault", "DT status"]
COL_STATUS_MARK = 0
COL_DIRECTORY = 1
STATUS_CHANGED = "Changed"
STATUS_UNCHANGED = "Unchanged"
COLOR_GREEN = QColor(0, 140, 0)
COLOR_RED = QColor(190, 0, 0)
COLOR_ORANGE = QColor(210, 130, 0)
STATUS_COLOR = {
    STATUS_UNCHANGED: COLOR_GREEN,
    STATUS_CHANGED: COLOR_RED,
}

DIFF_SOURCE_COLUMNS = ("along", "opposite", "crossing", "other", "base")
DIFF_CATEGORY_ROWS = (
    ("lane", "Lane IDs"),
    ("object_ids", "Object IDs"),
    ("path", "Path"),
    ("vsl", "VSL"),
)
DIFF_SUPPORTED_SOURCES = {
    "lane": {"along", "crossing", "opposite"},
    "object_ids": {"along", "opposite", "crossing", "other", "base"},
    "path": {"along", "opposite", "crossing", "other", "base"},
    "vsl": {"along", "opposite", "crossing"},
}
SOURCE_ALIASES = {
    "oncoming": "opposite",
}
WM_SOURCE_PATTERNS = (
    ("along", "along_object_set_with_prediction"),
    ("opposite", "oncoming_object_set_with_prediction"),
    ("crossing", "crossing_object_set_with_prediction"),
    ("other", "other_object_set_with_prediction"),
    ("base", "tracked_object_set_with_prediction"),
)
LANE_SOURCE_PATTERNS = (
    ("along", "multi_lane_ids_set"),
    ("crossing", "crossing_lane_ids_set"),
    ("opposite", "opposite_lane_ids_set"),
)
DISPLAY_MODE_LABELS = [
    "Baseline",
    "Test",
    "Diff overlay",
    "Diff along lane",
    "Diff opposite lane",
    "Diff crossing lane",
    "Diff along path",
    "Diff opposite path",
    "Diff crossing path",
    "Diff other path",
    "Observed",
    "Baseline strict validation along",
    "Baseline strict validation opposite",
    "Baseline strict validation crossing",
    "Baseline strict validation other",
    "Baseline loose validation along",
    "Baseline loose validation opposite",
    "Baseline loose validation crossing",
    "Baseline loose validation other",
    "Test strict validation along",
    "Test strict validation opposite",
    "Test strict validation crossing",
    "Test strict validation other",
    "Test loose validation along",
    "Test loose validation opposite",
    "Test loose validation crossing",
    "Test loose validation other",
]
VALIDATION_OBSERVED_BASELINE_NS = "/validation/observed_baseline"
VALIDATION_OBSERVED_TEST_NS = "/validation/observed_test"
VALIDATION_THRESHOLDS = ("strict", "loose")
VALIDATION_GROUPS = ("along", "opposite", "crossing", "other")
VALIDATION_SIDES = ("baseline", "test")
VALIDATION_STATUSES = ("optimal", "ignore", "fail", "observed_ok", "observed_ng")
VALIDATION_SUMMARY_METRIC_WIDTH = 220
VALIDATION_SUMMARY_VALUE_COL_RATIO = (1, 1)  # Baseline : Test


class KeepSelectionColorDelegate(QStyledItemDelegate):
    """Keep item foreground color even while row is selected (blue highlight stays)."""

    def paint(self, painter, option, index):
        opt = QStyleOptionViewItem(option)
        if opt.state & QStyle.State_Selected:
            fg = index.data(Qt.ForegroundRole)
            color = None
            if isinstance(fg, QBrush):
                color = fg.color()
            elif isinstance(fg, QColor):
                color = fg
            if color is not None and color.isValid():
                opt.palette.setBrush(QPalette.HighlightedText, QBrush(color))
        super().paint(painter, opt, index)

# TrajectoryPredictorViewer_validation のトピックパラメータ名（record_topics のキーと対応）
# bag は /validation/baseline または /validation/test の名前空間で記録されているため、viewer 起動時に渡す
RECORD_TOPIC_TO_VIEWER_PARAM = {
    "/WM/tracked_object_set_with_prediction": "base_objects_topic",
    "/WM/along_object_set_with_prediction": "along_objects_topic",
    "/WM/crossing_object_set_with_prediction": "crossing_objects_topic",
    "/WM/oncoming_object_set_with_prediction": "oncoming_objects_topic",
    "/WM/other_object_set_with_prediction": "other_objects_topic",
    "/viz/su/multi_lane_ids_set": "multi_lane_ids_topic",
    "/viz/su/crossing_lane_ids_set": "crossing_lane_ids_topic",
    "/viz/su/opposite_lane_ids_set": "opposite_lane_ids_topic",
    "/viz/su/ym0_converted_traffic_light_state": "traffic_light_state_topic",
}


def _dt_status_display(status: str | None) -> str:
    s = (status or "valid").strip().lower()
    if s == "normal":
        return "valid"
    if s in ("valid", "warning", "invalid"):
        return s
    return "valid"


def _summary_state(comp: dict | None) -> str | None:
    if not comp:
        return None
    path_ok = comp.get("path_ok") if "path_ok" in comp else comp.get("path_and_traffic_ok")
    traffic_ok = comp.get("traffic_ok") if "traffic_ok" in comp else comp.get("path_and_traffic_ok")
    if bool(comp.get("lane_ids_ok")) and bool(comp.get("vsl_ok")) and bool(comp.get("object_ids_ok")) and bool(path_ok) and bool(traffic_ok):
        return STATUS_UNCHANGED
    return STATUS_CHANGED


def _row_from_comp(rel: str, comp: dict | None) -> list[str]:
    """Return [status_mark, rel, valid_frames, lane_ids, vsl, object_ids, path, traffic, segfault, dt_status] for table row."""
    if not comp:
        return ["-", rel, "-", "-", "-", "-", "-", "-", "-", "-"]
    valid = comp.get("valid_frames")
    total = comp.get("total_frames")
    # 全体 = 和集合。古い JSON 用に total が無い場合は record_frames から算出
    if total is None and valid is not None:
        rb = comp.get("record_frames_baseline")
        rt = comp.get("record_frames_test")
        if rb is not None and rt is not None:
            total = rb + rt - valid  # |A∪B| = |A| + |B| - |A∩B|
    valid_str = f"{valid}/{total}" if (valid is not None and total is not None) else "-"
    a = STATUS_UNCHANGED if comp.get("lane_ids_ok") else STATUS_CHANGED
    b = STATUS_UNCHANGED if comp.get("vsl_ok") else STATUS_CHANGED
    c = STATUS_UNCHANGED if comp.get("object_ids_ok") else STATUS_CHANGED
    path_ok = comp.get("path_ok") if "path_ok" in comp else comp.get("path_and_traffic_ok")
    traffic_ok = comp.get("traffic_ok") if "traffic_ok" in comp else comp.get("path_and_traffic_ok")
    d = STATUS_UNCHANGED if path_ok else STATUS_CHANGED
    e = STATUS_UNCHANGED if traffic_ok else STATUS_CHANGED
    seg = "Yes" if comp.get("segfault_baseline") or comp.get("segfault_test") else "No"
    sb = _dt_status_display(comp.get("dt_status_baseline"))
    st = _dt_status_display(comp.get("dt_status_test"))
    if sb == "invalid" or st == "invalid":
        dt_status_label = "Invalid"
    elif sb == "warning" or st == "warning":
        dt_status_label = "Warning"
    else:
        dt_status_label = "Valid"
    max_b = comp.get("dt_max_baseline") or "-"
    max_t = comp.get("dt_max_test") or "-"
    try:
        max_val = max_b if max_b != "-" and (max_t == "-" or float(max_b) >= float(max_t)) else max_t
    except (ValueError, TypeError):
        max_val = max_b if max_b != "-" else max_t
    if max_val == "-":
        dt_status = dt_status_label
    else:
        dt_status = f"{dt_status_label} ({max_val})"
    return ["■", rel, valid_str, a, b, c, d, e, seg, dt_status]


def _validation_rate_stats(node: dict | None) -> tuple[str, float | None]:
    if not isinstance(node, dict):
        return "-", None
    try:
        ok = int(node.get("ok", 0) or 0)
        total = int(node.get("total", 0) or 0)
    except Exception:
        return "-", None
    if total <= 0:
        return "-", None
    rate = 100.0 * float(ok) / float(total)
    return f"{rate:.1f}% ({ok}/{total})", rate


def _validation_summary_rows(comp: dict | None) -> list[tuple[str, str, str, QColor | None, QColor | None]]:
    validation = comp.get("validation", {}) if isinstance(comp, dict) else {}
    baseline = validation.get("baseline", {}) if isinstance(validation, dict) else {}
    test = validation.get("test", {}) if isinstance(validation, dict) else {}
    rows: list[tuple[str, str, str, QColor | None, QColor | None]] = []
    for threshold in ("strict", "loose"):
        b_text, b_rate = _validation_rate_stats(baseline.get(threshold))
        t_text, t_rate = _validation_rate_stats(test.get(threshold))
        b_color = None
        t_color = None
        if b_rate is not None and t_rate is not None and abs(b_rate - t_rate) > 1e-9:
            if b_rate > t_rate:
                b_color = COLOR_GREEN
                t_color = COLOR_RED
            else:
                b_color = COLOR_RED
                t_color = COLOR_GREEN
        rows.append((f"{threshold} success rate", b_text, t_text, b_color, t_color))
    return rows


def _empty_diff_map(default_value):
    return {
        cat: {src: default_value for src in DIFF_SUPPORTED_SOURCES[cat]}
        for cat, _label in DIFF_CATEGORY_ROWS
    }


def _extract_sources(text: str, patterns: tuple[tuple[str, str], ...]) -> set[str]:
    out = set()
    if not text:
        return out
    lower = text.lower()
    for source, token in patterns:
        if token in lower:
            out.add(source)
    return out


def _extract_diff_maps(comp: dict | None):
    by_source = _empty_diff_map(False)
    counts = _empty_diff_map(0)
    if not comp:
        return by_source, counts

    raw_by_source = comp.get("diff_by_source")
    raw_counts = comp.get("diff_counts_by_source")
    if isinstance(raw_by_source, dict):
        for cat, _label in DIFF_CATEGORY_ROWS:
            raw_cat = raw_by_source.get(cat, {})
            if not isinstance(raw_cat, dict):
                continue
            for raw_src, raw_val in raw_cat.items():
                src = SOURCE_ALIASES.get(raw_src, raw_src)
                if src in DIFF_SUPPORTED_SOURCES[cat]:
                    by_source[cat][src] = by_source[cat][src] or bool(raw_val)
    if isinstance(raw_counts, dict):
        for cat, _label in DIFF_CATEGORY_ROWS:
            raw_cat = raw_counts.get(cat, {})
            if not isinstance(raw_cat, dict):
                continue
            for raw_src, raw_val in raw_cat.items():
                src = SOURCE_ALIASES.get(raw_src, raw_src)
                if src not in DIFF_SUPPORTED_SOURCES[cat]:
                    continue
                try:
                    counts[cat][src] += max(0, int(raw_val))
                except Exception:
                    continue

    if isinstance(raw_by_source, dict):
        return by_source, counts

    lane_sources = _extract_sources(comp.get("lane_ids_detail", ""), LANE_SOURCE_PATTERNS)
    for src in lane_sources:
        by_source["lane"][src] = True
        counts["lane"][src] += 1
    if not comp.get("lane_ids_ok", True) and not lane_sources:
        for src in DIFF_SUPPORTED_SOURCES["lane"]:
            by_source["lane"][src] = True

    vsl_sources = _extract_sources(comp.get("vsl_detail", ""), WM_SOURCE_PATTERNS)
    for src in vsl_sources:
        if src in DIFF_SUPPORTED_SOURCES["vsl"]:
            by_source["vsl"][src] = True
            counts["vsl"][src] += 1
    if not comp.get("vsl_ok", True) and not vsl_sources:
        for src in DIFF_SUPPORTED_SOURCES["vsl"]:
            by_source["vsl"][src] = True

    path_sources = _extract_sources(comp.get("path_and_traffic_detail", ""), WM_SOURCE_PATTERNS)
    for src in path_sources:
        if src in DIFF_SUPPORTED_SOURCES["path"]:
            by_source["path"][src] = True
            counts["path"][src] += 1
    if not comp.get("path_ok", True) and not path_sources:
        for src in DIFF_SUPPORTED_SOURCES["path"]:
            by_source["path"][src] = True

    object_sources = _extract_sources(comp.get("object_ids_detail", ""), WM_SOURCE_PATTERNS)
    for src in object_sources:
        if src in DIFF_SUPPORTED_SOURCES["object_ids"]:
            by_source["object_ids"][src] = True
            counts["object_ids"][src] += 1
    if not comp.get("object_ids_ok", True) and not object_sources:
        for src in DIFF_SUPPORTED_SOURCES["object_ids"]:
            by_source["object_ids"][src] = True

    return by_source, counts


def _collect_results(root: Path, test_results_path: str) -> list[tuple[str, Path, dict | None]]:
    """List of (rel, comparison.json path, comp_dict or None). Supports {日付}/{時刻} and {名前} (one-level)."""
    test_root = root / test_results_path
    out = []
    if not test_root.is_dir():
        return out
    for top in sorted(test_root.iterdir()):
        if not top.is_dir():
            continue
        cmp_path = top / "comparison.json"
        if cmp_path.exists():
            # One-level: top/comparison.json (e.g. C28-06_OR_日付-時間)
            comp = None
            try:
                with open(cmp_path, "r", encoding="utf-8") as f:
                    comp = json.load(f)
            except Exception:
                pass
            out.append((top.name, cmp_path, comp))
            continue
        # Two-level: top/sub/comparison.json (e.g. 日付/時刻)
        for sub in sorted(top.iterdir()):
            if not sub.is_dir():
                continue
            cmp_path = sub / "comparison.json"
            comp = None
            if cmp_path.exists():
                try:
                    with open(cmp_path, "r", encoding="utf-8") as f:
                        comp = json.load(f)
                except Exception:
                    pass
            rel = f"{top.name}/{sub.name}"
            out.append((rel, cmp_path, comp))
    return out


def set_viewer_rosparams(settings: dict) -> None:
    """settings の rosparam（use_sim_time, map_name）を ROS に設定する。失敗時は stderr に出力する。"""
    rosparam = settings.get("rosparam", {})
    env = os.environ.copy()
    for param, value in [
        ("/use_sim_time", str(rosparam.get("use_sim_time", True)).lower()),
        ("/map_name", rosparam.get("map_name", "new_MM_map")),
    ]:
        r = subprocess.run(
            ["rosparam", "set", param, value],
            env=env, capture_output=True, timeout=2, text=True
        )
        if r.returncode != 0:
            print(f"rosparam set {param} failed: {r.stderr or r.stdout or r.returncode}", file=sys.stderr)


class ViewerAppPyQt(QMainWindow):
    def __init__(self):
        super().__init__()
        self.root = get_tester_root()
        self.settings = load_settings(self.root)
        self.paths = self.settings["paths"]
        self.baseline_root = self.root / self.paths["baseline_results"]
        self.test_results_root = self.root / self.paths["test_results"]
        self.current_rel = None
        self.play_proc = None
        self.viewer_proc = None
        self.viewer_embed = None
        self._play_finished_timer = None
        self._play_paused = False
        self._playing_rel = None
        self._play_start_time = None
        self._viewer_launch_timer = QTimer(self)
        self._viewer_launch_timer.timeout.connect(self._check_viewer_launched)
        self._viewer_launch_start = None
        self._baseline_commit_info = self._load_commit_info(self.baseline_root / "commit_info.json")
        self._test_commit_info = self._load_commit_info(self.test_results_root / "commit_info.json")
        self._build_ui()
        self._refresh_list()
        QTimer.singleShot(0, self._update_validation_table_column_widths)
        QTimer.singleShot(0, self._launch_viewer)

    @staticmethod
    def _terminate_process_tree(proc: subprocess.Popen | None, timeout_sec: float = 2.0) -> None:
        if proc is None or proc.poll() is not None:
            return
        try:
            if os.name != "nt":
                try:
                    os.killpg(proc.pid, signal.SIGTERM)
                except Exception:
                    proc.terminate()
            else:
                proc.terminate()
            proc.wait(timeout=timeout_sec)
            return
        except Exception:
            pass
        try:
            if os.name != "nt":
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except Exception:
                    proc.kill()
            else:
                proc.kill()
            proc.wait(timeout=1.0)
        except Exception:
            pass

    @staticmethod
    def _load_commit_info(path: Path) -> dict:
        if not path.exists():
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                obj = json.load(f)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _commit_text(info: dict | None) -> str:
        info = info or {}
        return str(info.get("commit") or info.get("describe") or "(not recorded)")

    def _build_ui(self):
        self.setWindowTitle("TP_tester – Verification Results")
        central = QWidget()
        self.setCentralWidget(central)
        layout = QHBoxLayout(central)
        splitter = QSplitter(Qt.Horizontal)

        # Left: table (header row + one row per result)
        left = QFrame()
        left.setFrameStyle(QFrame.StyledPanel)
        left_layout = QVBoxLayout(left)
        left_layout.addWidget(QLabel("Results"))
        self.table = QTableWidget()
        self.table.setColumnCount(len(TABLE_HEADERS))
        self.table.setHorizontalHeaderLabels(TABLE_HEADERS)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSelectionMode(QTableWidget.SingleSelection)
        self.table.setItemDelegate(KeepSelectionColorDelegate(self.table))
        self.table.setStyleSheet(
            "QTableWidget::item:selected {"
            " background-color: rgba(90, 150, 255, 90);"
            "}"
        )
        self.table.itemSelectionChanged.connect(self._on_select)
        left_layout.addWidget(self.table)
        splitter.addWidget(left)

        # Right: detail + controls
        right = QFrame()
        right.setFrameStyle(QFrame.StyledPanel)
        right_layout = QVBoxLayout(right)
        # 上段: 差分マトリクス表示（viewer は別ウィンドウ）
        self.viewer_host = QFrame()
        self.viewer_host.setFrameStyle(QFrame.StyledPanel)
        self.viewer_host.setMinimumSize(400, 300)
        viewer_host_layout = QVBoxLayout(self.viewer_host)
        viewer_host_layout.addWidget(QLabel("Validation summary"))
        self.validation_table = QTableWidget()
        self.validation_table.setRowCount(2)
        self.validation_table.setColumnCount(3)
        self.validation_table.setHorizontalHeaderLabels(["Metric", "Baseline", "Test"])
        self.validation_table.setVerticalHeaderLabels(["", ""])
        self.validation_table.setSelectionMode(QTableWidget.NoSelection)
        self.validation_table.setEditTriggers(QTableWidget.NoEditTriggers)
        # Metric は固定幅、Baseline/Test は残り幅を固定比率で配分する。
        self.validation_table.horizontalHeader().setSectionResizeMode(QHeaderView.Fixed)
        self.validation_table.verticalHeader().setVisible(False)
        self.validation_table.setMaximumHeight(130)
        viewer_host_layout.addWidget(self.validation_table)
        viewer_host_layout.addWidget(QLabel("Diff matrix"))
        self.diff_table = QTableWidget()
        self.diff_table.setRowCount(len(DIFF_CATEGORY_ROWS))
        self.diff_table.setColumnCount(len(DIFF_SOURCE_COLUMNS))
        self.diff_table.setVerticalHeaderLabels([label for _key, label in DIFF_CATEGORY_ROWS])
        self.diff_table.setHorizontalHeaderLabels([s.capitalize() for s in DIFF_SOURCE_COLUMNS])
        self.diff_table.setSelectionMode(QTableWidget.NoSelection)
        self.diff_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.diff_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.diff_table.verticalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        viewer_host_layout.addWidget(self.diff_table)
        self.viewer_status = QLabel("Viewer: Launching...")
        self.viewer_status.setWordWrap(True)
        viewer_host_layout.addWidget(self.viewer_status)
        right_layout.addWidget(self.viewer_host, 1)
        self.baseline_commit_label = QLabel(f"Baseline commit: {self._commit_text(self._baseline_commit_info)}")
        self.test_commit_label = QLabel(f"Test commit: {self._commit_text(self._test_commit_info)}")
        self.baseline_commit_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self.test_commit_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        right_layout.addWidget(self.baseline_commit_label)
        right_layout.addWidget(self.test_commit_label)
        # 下段: メタ情報テーブル
        right_layout.addWidget(QLabel("Detail"))
        self.detail_table = QTableWidget()
        self.detail_table.setColumnCount(2)
        self.detail_table.setHorizontalHeaderLabels(["Item", "Value"])
        self.detail_table.setSelectionMode(QTableWidget.NoSelection)
        self.detail_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.detail_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.detail_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.detail_table.setMaximumHeight(180)
        right_layout.addWidget(self.detail_table)

        right_layout.addWidget(QLabel("Playback mode"))
        self.mode_combo = QComboBox()
        self.mode_combo.addItems(DISPLAY_MODE_LABELS)
        self.mode_combo.currentIndexChanged.connect(self._publish_display_mode)
        right_layout.addWidget(self.mode_combo)

        btn_layout = QHBoxLayout()
        self.btn_play = QPushButton("Play")
        self.btn_play.clicked.connect(self._play)
        btn_layout.addWidget(self.btn_play)
        self.btn_stop = QPushButton("Stop")
        self.btn_stop.setEnabled(False)
        self.btn_stop.clicked.connect(self._on_stop_or_resume)
        btn_layout.addWidget(self.btn_stop)
        self.btn_end_play = QPushButton("End playback")
        self.btn_end_play.clicked.connect(self._end_playback)
        self.btn_end_play.setEnabled(False)
        btn_layout.addWidget(self.btn_end_play)
        right_layout.addLayout(btn_layout)
        splitter.addWidget(right)

        layout.addWidget(splitter)
        self.resize(900, 600)
        self._set_validation_table_rows(_validation_summary_rows(None))
        self._set_diff_table_empty()
        self._set_detail_table_rows([])

    @staticmethod
    def _set_status_color(item: QTableWidgetItem) -> None:
        text = (item.text() or "").strip()
        if not text:
            return
        lower = text.lower()
        color = STATUS_COLOR.get(text)
        if color is None:
            if lower == "no":
                color = COLOR_GREEN
            elif lower == "yes":
                color = COLOR_RED
            elif lower.startswith("valid") or "valid" in lower:
                color = COLOR_GREEN
            elif lower.startswith("normal") or ("normal" in lower and "warning" not in lower and "invalid" not in lower):
                color = COLOR_GREEN
            elif lower.startswith("warning") or "warning" in lower:
                color = COLOR_ORANGE
            elif lower.startswith("invalid") or "invalid" in lower:
                color = COLOR_RED
        if color is not None:
            item.setForeground(color)

    def _set_diff_table_empty(self):
        for r, (cat, _label) in enumerate(DIFF_CATEGORY_ROWS):
            supported = DIFF_SUPPORTED_SOURCES.get(cat, set())
            for c, source in enumerate(DIFF_SOURCE_COLUMNS):
                text = "-" if source not in supported else STATUS_UNCHANGED
                item = QTableWidgetItem(text)
                item.setTextAlignment(Qt.AlignCenter)
                self._set_status_color(item)
                self.diff_table.setItem(r, c, item)

    def _set_diff_table_no_data(self):
        for r, (_cat, _label) in enumerate(DIFF_CATEGORY_ROWS):
            for c, _source in enumerate(DIFF_SOURCE_COLUMNS):
                text = "-"
                item = QTableWidgetItem(text)
                item.setTextAlignment(Qt.AlignCenter)
                self.diff_table.setItem(r, c, item)

    def _set_validation_table_rows(self, rows: list[tuple[str, str, str, QColor | None, QColor | None]]):
        self.validation_table.setRowCount(len(rows))
        for i, (name, baseline_text, test_text, baseline_color, test_color) in enumerate(rows):
            metric_item = QTableWidgetItem(name)
            metric_item.setBackground(QBrush(QColor(236, 236, 236)))
            metric_item.setForeground(QBrush(QColor(0, 0, 0)))
            metric_item.setTextAlignment(Qt.AlignCenter)
            metric_font = metric_item.font()
            metric_font.setBold(True)
            metric_item.setFont(metric_font)
            baseline_item = QTableWidgetItem(baseline_text)
            test_item = QTableWidgetItem(test_text)
            baseline_item.setTextAlignment(Qt.AlignCenter)
            test_item.setTextAlignment(Qt.AlignCenter)
            if baseline_color is not None:
                baseline_item.setForeground(QBrush(baseline_color))
            if test_color is not None:
                test_item.setForeground(QBrush(test_color))
            self.validation_table.setItem(i, 0, metric_item)
            self.validation_table.setItem(i, 1, baseline_item)
            self.validation_table.setItem(i, 2, test_item)
        self.validation_table.verticalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self._update_validation_table_column_widths()

    def _update_validation_table_column_widths(self):
        table = getattr(self, "validation_table", None)
        if table is None:
            return
        vp_w = int(table.viewport().width())
        if vp_w <= 0:
            return
        w0 = min(VALIDATION_SUMMARY_METRIC_WIDTH, vp_w)
        remain = max(0, vp_w - w0)
        r1, r2 = VALIDATION_SUMMARY_VALUE_COL_RATIO
        total_r = max(1, r1 + r2)
        w1 = int((remain * r1) / total_r)
        w2 = max(0, remain - w1)
        table.setColumnWidth(0, w0)
        table.setColumnWidth(1, w1)
        table.setColumnWidth(2, w2)

    def _set_detail_table_rows(self, rows: list[tuple[str, str]]):
        self.detail_table.setRowCount(len(rows))
        for i, (name, value) in enumerate(rows):
            left = QTableWidgetItem(name)
            right = QTableWidgetItem(value)
            self._set_status_color(right)
            self.detail_table.setItem(i, 0, left)
            self.detail_table.setItem(i, 1, right)
        self.detail_table.verticalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)

    def _refresh_list(self):
        rows = _collect_results(self.root, self.paths["test_results"])
        self.table.setRowCount(len(rows))
        for i, (rel, _cmp_path, comp) in enumerate(rows):
            cells = _row_from_comp(rel, comp)
            for j, text in enumerate(cells):
                item = QTableWidgetItem(text)
                if j == COL_STATUS_MARK:
                    item.setTextAlignment(Qt.AlignCenter)
                    marker_state = _summary_state(comp)
                    if marker_state in STATUS_COLOR:
                        item.setForeground(STATUS_COLOR[marker_state])
                else:
                    self._set_status_color(item)
                self.table.setItem(i, j, item)
        self.table.resizeColumnsToContents()
        self.table.setColumnWidth(COL_STATUS_MARK, 24)

    def _on_select(self):
        sel = self.table.selectedIndexes()
        if not sel:
            return
        row = sel[0].row()
        item0 = self.table.item(row, COL_DIRECTORY)
        self.current_rel = item0.text() if item0 else None
        if not self.current_rel:
            return
        if self.play_proc and self.play_proc.poll() is None and self._playing_rel is not None and self.current_rel != self._playing_rel:
            self._end_playback()
        cmp_path = self.test_results_root / self.current_rel / "comparison.json"
        comp = None
        if cmp_path.exists():
            try:
                with open(cmp_path, "r", encoding="utf-8") as f:
                    comp = json.load(f)
            except Exception:
                comp = None

        if comp is None:
            self._set_validation_table_rows(_validation_summary_rows(None))
            self._set_diff_table_no_data()
            baseline_bag = self.baseline_root / self.current_rel / "result_baseline.bag"
            test_bag = self.test_results_root / self.current_rel / "result_test.bag"
            self._set_detail_table_rows([
                ("Directory", self.current_rel),
                ("Baseline bag", str(baseline_bag)),
                ("Test bag", str(test_bag)),
                ("comparison.json", "not found or parse error"),
            ])
            return

        self._set_validation_table_rows(_validation_summary_rows(comp))
        diff_map, diff_counts = _extract_diff_maps(comp)
        for r, (cat, _label) in enumerate(DIFF_CATEGORY_ROWS):
            supported = DIFF_SUPPORTED_SOURCES.get(cat, set())
            for c, source in enumerate(DIFF_SOURCE_COLUMNS):
                if source not in supported:
                    text = "-"
                else:
                    changed = bool(diff_map.get(cat, {}).get(source, False))
                    count = int(diff_counts.get(cat, {}).get(source, 0) or 0)
                    if changed:
                        text = f"{STATUS_CHANGED} ({count})" if count > 0 else STATUS_CHANGED
                    else:
                        text = STATUS_UNCHANGED
                item = QTableWidgetItem(text)
                item.setTextAlignment(Qt.AlignCenter)
                if text.startswith(STATUS_CHANGED):
                    item.setForeground(STATUS_COLOR[STATUS_CHANGED])
                elif text == STATUS_UNCHANGED:
                    item.setForeground(STATUS_COLOR[STATUS_UNCHANGED])
                self.diff_table.setItem(r, c, item)

        baseline_bag = self.baseline_root / self.current_rel / "result_baseline.bag"
        test_bag = self.test_results_root / self.current_rel / "result_test.bag"
        valid = comp.get("valid_frames") if comp else None
        total = comp.get("total_frames") if comp else None
        if total is None and comp and valid is not None:
            rb = comp.get("record_frames_baseline")
            rt = comp.get("record_frames_test")
            if rb is not None and rt is not None:
                total = rb + rt - valid
        valid_text = f"{valid}/{total}" if valid is not None and total is not None else "-"
        lane_text = STATUS_UNCHANGED if (comp and comp.get("lane_ids_ok")) else (STATUS_CHANGED if comp else "-")
        vsl_text = STATUS_UNCHANGED if (comp and comp.get("vsl_ok")) else (STATUS_CHANGED if comp else "-")
        object_text = STATUS_UNCHANGED if (comp and comp.get("object_ids_ok")) else (STATUS_CHANGED if comp else "-")
        path_text = STATUS_UNCHANGED if (comp and comp.get("path_ok")) else (STATUS_CHANGED if comp else "-")
        traffic_text = STATUS_UNCHANGED if (comp and comp.get("traffic_ok")) else (STATUS_CHANGED if comp else "-")
        seg_text = "-"
        dt_text = "-"
        if comp:
            seg_text = (
                "Yes"
                if comp.get("segfault_baseline") or comp.get("segfault_test")
                else "No"
            )
            dt_b = _dt_status_display(comp.get("dt_status_baseline"))
            dt_t = _dt_status_display(comp.get("dt_status_test"))
            dt_max_b = comp.get("dt_max_baseline") or "-"
            dt_max_t = comp.get("dt_max_test") or "-"
            dt_text = f"B:{dt_b}({dt_max_b}) / T:{dt_t}({dt_max_t})"

        detail_rows = [
            ("Directory", self.current_rel),
            ("Valid/Total", valid_text),
            ("Lane IDs", lane_text),
            ("VSL", vsl_text),
            ("Object IDs", object_text),
            ("Path", path_text),
            ("Traffic", traffic_text),
            ("Seg fault", seg_text),
            ("DT status", dt_text),
            ("Baseline bag", str(baseline_bag)),
            ("Test bag", str(test_bag)),
        ]
        self._set_detail_table_rows(detail_rows)

    def _publish_display_mode(self, mode=None):
        try:
            if mode is None:
                mode = self.mode_combo.currentIndex()
            subprocess.run(
                ["rostopic", "pub", "-1", "/validation/display_mode", "std_msgs/Int32", str(mode)],
                env=os.environ.copy(), capture_output=True, timeout=2
            )
        except Exception:
            pass

    def _launch_viewer(self):
        if self.viewer_proc and self.viewer_proc.poll() is None:
            return
        self.viewer_status.setText("Viewer: Launching...")
        set_viewer_rosparams(self.settings)
        node_cfg = self.settings.get("node", {})
        pkg = node_cfg.get("viewer_pkg", "nrc_svcs")
        node = node_cfg.get("viewer_validation_node", "TrajectoryPredictorViewer_validation")
        record_topics = self.settings.get("record_topics", [])
        # bag は /validation/baseline と /validation/test で記録されているので、viewer にそのトピック名を渡す
        viewer_args = ["rosrun", pkg, node, "-m"]
        for topic in record_topics:
            param = RECORD_TOPIC_TO_VIEWER_PARAM.get(topic)
            if param:
                viewer_args.append(f"_{param}:={VALIDATION_BASELINE_NS}{topic}")
                viewer_args.append(f"_test_{param}:={VALIDATION_TEST_NS}{topic}")
                if topic.startswith("/WM/"):
                    viewer_args.append(f"_observed_{param}:={VALIDATION_OBSERVED_BASELINE_NS}{topic}")
                    viewer_args.append(f"_observed_test_{param}:={VALIDATION_OBSERVED_TEST_NS}{topic}")
        self.viewer_proc = subprocess.Popen(
            viewer_args,
            env=os.environ.copy(),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        self._viewer_launch_start = time.monotonic()
        self._viewer_launch_timer.start(400)

    def _process_has_window(self, pid: int) -> bool:
        """プロセスがウィンドウを持っているか（Linux: wmctrl 使用）。"""
        try:
            r = subprocess.run(
                ["wmctrl", "-l", "-p"],
                capture_output=True,
                text=True,
                timeout=2,
                env=os.environ.copy(),
            )
            if r.returncode != 0:
                return False
            # 3番目が PID (0xid desktop pid client title)
            for line in r.stdout.splitlines():
                parts = line.split(None, 3)
                if len(parts) >= 3 and parts[2] == str(pid):
                    return True
            return False
        except Exception:
            return False

    def _check_viewer_launched(self):
        """ウィンドウが立ち上がるまでポーリング。立ち上がったか失敗かタイムアウトで「Launching」を解除。"""
        if not self.viewer_proc:
            self._viewer_launch_timer.stop()
            self._viewer_launch_start = None
            return
        if self.viewer_proc.poll() is not None:
            self._viewer_launch_timer.stop()
            self._viewer_launch_start = None
            err = (self.viewer_proc.stderr and self.viewer_proc.stderr.read()) or b""
            msg = err.decode("utf-8", errors="replace").strip() or "Process exited with code %s" % self.viewer_proc.returncode
            self.viewer_status.setText("Viewer: Failed to start.")
            QMessageBox.warning(self, "Viewer failed", msg)
            self.viewer_proc = None
            return
        elapsed = time.monotonic() - self._viewer_launch_start if self._viewer_launch_start else 0
        if self._process_has_window(self.viewer_proc.pid) or elapsed > 10:
            self._viewer_launch_timer.stop()
            self._viewer_launch_start = None
            self.viewer_status.setText("Viewer: Running in a separate window. Use Play to start rosbag playback.")

    def _get_selected_rel(self):
        """Currently selected table row's directory (rel), or None."""
        sel = self.table.selectedIndexes()
        if not sel:
            return None
        row = sel[0].row()
        item0 = self.table.item(row, COL_DIRECTORY)
        return item0.text() if item0 else None

    def _play(self):
        rel = self._get_selected_rel()
        if not rel:
            QMessageBox.warning(self, "Play", "Select a data row first.")
            return
        self.btn_play.setText("Preparing...")
        self.btn_play.setEnabled(False)
        QApplication.processEvents()  # 即座に描画し、準備中は押せないように見せる（連打防止）
        self._stop()
        self.current_rel = rel
        set_viewer_rosparams(self.settings)
        baseline_bag = self.baseline_root / rel / "result_baseline.bag"
        observed_baseline_bag = self.baseline_root / rel / "observed_baseline.bag"
        test_bag = self.test_results_root / rel / "result_test.bag"
        observed_test_bag = self.test_results_root / rel / "observed_test.bag"
        out_dir = self.test_results_root / rel
        common_bag = out_dir / "common.bag"
        diff_baseline_bag = out_dir / "diff_baseline.bag"
        diff_test_bag = out_dir / "diff_test.bag"
        if not baseline_bag.exists():
            self.btn_play.setText("Play")
            self.btn_play.setEnabled(True)
            QMessageBox.warning(self, "Play", f"Baseline bag not found: {baseline_bag}")
            return
        # どのモードで再生開始しても常に全 bag を再生し、表示側でモード切り替えだけする
        test_bags_root = self.root / self.paths["test_bags"]
        test_bags_dir = test_bags_root / rel
        extra_bags = get_bag_files_in_dir(test_bags_dir) if test_bags_dir.is_dir() else []
        play_bags = [str(b) for b in extra_bags]
        play_bags.append(str(baseline_bag))
        if observed_baseline_bag.exists():
            play_bags.append(str(observed_baseline_bag))
        if test_bag.exists():
            play_bags.append(str(test_bag))
        if observed_test_bag.exists():
            play_bags.append(str(observed_test_bag))
        if common_bag.exists():
            play_bags.append(str(common_bag))
        if diff_baseline_bag.exists():
            play_bags.append(str(diff_baseline_bag))
        if diff_test_bag.exists():
            play_bags.append(str(diff_test_bag))
        for threshold in VALIDATION_THRESHOLDS:
            for group in VALIDATION_GROUPS:
                for side in VALIDATION_SIDES:
                    for status in VALIDATION_STATUSES:
                        validation_root = self.baseline_root if side == "baseline" else self.test_results_root
                        validation_bag = validation_root / rel / f"{threshold}_{group}_{side}_{status}.bag"
                        if validation_bag.exists():
                            play_bags.append(str(validation_bag))
        self._playing_rel = rel
        self._publish_display_mode(self.mode_combo.currentIndex())
        self._start_rosbag_play(play_bags)

    def _start_rosbag_play(self, play_bags: list[str]):
        self._publish_clear_buffers()
        try:
            self.play_proc = subprocess.Popen(
                ["rosbag", "play"] + play_bags + ["--clock", "--loop"],
                env=os.environ.copy(),
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
        except Exception as e:
            self._set_stopped_state()
            QMessageBox.warning(self, "Play", f"rosbag play の起動に失敗しました:\n{e}")
            return
        self._play_start_time = time.monotonic()
        self._play_paused = False
        self.btn_play.setEnabled(False)
        self.btn_play.setText("Playing...")
        self.btn_stop.setEnabled(True)
        self.btn_stop.setText("Stop")
        self.btn_end_play.setEnabled(True)
        if self._play_finished_timer is None:
            self._play_finished_timer = QTimer(self)
            self._play_finished_timer.timeout.connect(self._check_play_finished)
        self._play_finished_timer.start(500)

    def _check_play_finished(self):
        if not self.play_proc:
            self._play_finished_timer.stop()
            self._set_stopped_state()
            return
        if self.play_proc.poll() is not None:
            proc = self.play_proc
            self.play_proc = None
            self._play_paused = False
            self._play_finished_timer.stop()
            try:
                elapsed = time.monotonic() - getattr(self, "_play_start_time", time.monotonic())
                if proc.returncode != 0 or elapsed < 0.5:
                    try:
                        err = (proc.stderr.read() if proc.stderr else b"").decode("utf-8", errors="replace").strip()
                        out = (proc.stdout.read() if proc.stdout else b"").decode("utf-8", errors="replace").strip()
                    except Exception:
                        err, out = "", ""
                    msg = f"rosbag play が終了しました (終了コード: {proc.returncode}, 経過: {elapsed:.1f}s)"
                    if err or out:
                        msg += "\n\n" + (err or out)
                    QMessageBox.warning(self, "再生エラー", msg)
            finally:
                self._set_stopped_state()

    def _set_stopped_state(self):
        self._playing_rel = None
        self.btn_play.setEnabled(True)
        self.btn_play.setText("Play")
        self.btn_stop.setEnabled(False)
        self.btn_stop.setText("Stop")
        self.btn_end_play.setEnabled(False)
        self._publish_clear_buffers()

    def _publish_clear_buffers(self):
        """Viewer にバッファクリアを指示し、再 Play で表示されるようにする。"""
        try:
            subprocess.run(
                ["rostopic", "pub", "-1", "/validation/clear_buffers", "std_msgs/Empty", "{}"],
                env=os.environ.copy(), capture_output=True, timeout=2
            )
        except Exception:
            pass

    def _end_playback(self):
        """rosbag play に Ctrl+C (SIGINT) を送って再生終了。"""
        if not self.play_proc or self.play_proc.poll() is not None:
            return
        if self._play_finished_timer and self._play_finished_timer.isActive():
            self._play_finished_timer.stop()
        try:
            if os.name != "nt":
                try:
                    os.killpg(self.play_proc.pid, signal.SIGINT)
                except Exception:
                    self.play_proc.send_signal(signal.SIGINT)
            else:
                self.play_proc.send_signal(signal.SIGINT)
            self.play_proc.wait(timeout=3)
        except Exception:
            self._terminate_process_tree(self.play_proc, timeout_sec=2.0)
        self.play_proc = None
        self._play_paused = False
        self._set_stopped_state()

    def _send_rosbag_space(self):
        """動作中の rosbag play に Space を送る（一時停止/再開のトグル）。"""
        if not self.play_proc or self.play_proc.poll() is not None or self.play_proc.stdin is None:
            return
        try:
            self.play_proc.stdin.write(b" ")
            self.play_proc.stdin.flush()
        except Exception:
            pass

    def _on_stop_or_resume(self):
        """rosbag play の Space 扱い: 再生中＝Space で一時停止、停止中＝Space で再開（プロセスはそのまま）。"""
        if self.play_proc and self.play_proc.poll() is None:
            self._send_rosbag_space()
            self._play_paused = not self._play_paused
            self.btn_stop.setText("Resume" if self._play_paused else "Stop")
        else:
            self._play()

    def _stop(self):
        """再生プロセスを終了（新規 Play や終了時に使用）。"""
        self._play_paused = False
        if self.play_proc and self.play_proc.poll() is None:
            self._terminate_process_tree(self.play_proc, timeout_sec=2.0)
            self.play_proc = None
        if self._play_finished_timer and self._play_finished_timer.isActive():
            self._play_finished_timer.stop()
        self._set_stopped_state()

    def closeEvent(self, event):
        self._stop()
        if self.viewer_proc and self.viewer_proc.poll() is None:
            self._terminate_process_tree(self.viewer_proc, timeout_sec=2.0)
            self.viewer_proc = None
        event.accept()

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._update_validation_table_column_widths()


def main() -> int:
    try:
        root = get_tester_root()
        if not (root / "settings.json").exists():
            print("Run from TP_tester or set TP_TESTER_ROOT.", file=sys.stderr)
            return 1
        os.chdir(root)
        if os.name != "nt" and not os.environ.get("DISPLAY"):
            print("DISPLAY is not set. GUI cannot start (e.g. use ssh -X or set DISPLAY).", file=sys.stderr)
            return 1
        app = QApplication(sys.argv)
        win = ViewerAppPyQt()
        win.show()
        win.raise_()
        win.activateWindow()
        return app.exec_() if hasattr(app, "exec_") else app.exec()
    except Exception as e:
        print(f"Viewer failed to start: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
