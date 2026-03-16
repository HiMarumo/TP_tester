#!/usr/bin/env python3
"""
Verification result viewer GUI.
Left: list of data (date/time) with ①~④ diff status.
Right: TrajectoryPredictorViewer の可視化エリア（Launch で埋め込み）＋ Detail ＋ Play/Stop。
Viewer は別ウィンドウで起動します（同一アプリ内への埋め込みは行いません）。
"""
from __future__ import annotations

import html
import json
import os
import re
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
    QAbstractItemView,
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
    QToolButton,
    QStyledItemDelegate,
    QStyleOptionViewItem,
    QStyleOptionComboBox,
    QStylePainter,
    QStyle,
    QVBoxLayout,
    QWidget,
)
from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtGui import QColor, QBrush, QPalette, QTextDocument, QAbstractTextDocumentLayout


# Column headers for comparison table (leftmost column is scene expand/collapse toggle)
TABLE_HEADERS = ["", "", "C", "L", "O", "P", "Directory", "Valid/Total", "Lane IDs", "VSL", "Object IDs", "Path", "Traffic", "Seg fault", "DT status"]
COL_TOGGLE = 0
COL_HALF_DELTA = 1
COL_COLLISION = 2
COL_MARK_L = 3
COL_MARK_O = 4
COL_MARK_P = 5
COL_DIRECTORY = 6
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
VALIDATION_OBSERVED_BASELINE_NS = "/validation/observed_baseline"
VALIDATION_OBSERVED_TEST_NS = "/validation/observed_test"
VALIDATION_DISPLAY_HORIZONS = ("half-time-relaxed",)
VALIDATION_MODE_LAYOUT_HORIZONS = ("half-time-relaxed", "time-relaxed")
VALIDATION_MODE_HORIZON_CODE = {
    "half-time-relaxed": 0,
    "time-relaxed": 1,
}
VALIDATION_SUMMARY_HORIZONS = ("half-time-relaxed", "time-relaxed")
VALIDATION_THRESHOLDS = ("approximate", "strict")
COLLISION_KINDS = ("hard", "soft")
VALIDATION_SUMMARY_ROWS = (
    ("half-time-relaxed", "approximate"),
    ("half-time-relaxed", "strict"),
    ("time-relaxed", "approximate"),
    ("time-relaxed", "strict"),
)
VALIDATION_GROUPS = ("along", "opposite", "crossing", "other")
VALIDATION_MODE_GROUPS = ("all", "along", "opposite", "crossing", "other")
VALIDATION_MODE_GROUP_CODE = {
    "along": 0,
    "opposite": 1,
    "crossing": 2,
    "other": 3,
    "all": 4,
}
VALIDATION_SIDES = ("baseline", "test")
VALIDATION_STATUSES = ("optimal", "ignore", "fail", "observed_ok", "observed_ng")
_DISPLAY_MODE_BASE_ITEMS = [
    ("Baseline", 0),
    ("Test", 1),
    ("Diff overlay", 2),
    ("Diff along lane", 3),
    ("Diff opposite lane", 4),
    ("Diff crossing lane", 5),
    ("Diff along path", 6),
    ("Diff along path with observed", 10),
    ("Diff opposite path", 7),
    ("Diff opposite path with observed", 11),
    ("Diff crossing path", 8),
    ("Diff crossing path with observed", 12),
    ("Diff other path", 9),
    ("Diff other path with observed", 13),
    ("Observed", 14),
]
DISPLAY_MODE_VALIDATION_BEGIN = 15
DISPLAY_MODE_COLLISION_HARD_BASELINE = 55
DISPLAY_MODE_COLLISION_HARD_TEST = 56
DISPLAY_MODE_COLLISION_SOFT_BASELINE = 57
DISPLAY_MODE_COLLISION_SOFT_TEST = 58
DISPLAY_MODE_LABELS = [label for (label, _value) in _DISPLAY_MODE_BASE_ITEMS]
DISPLAY_MODE_VALUES = [value for (_label, value) in _DISPLAY_MODE_BASE_ITEMS]
_validation_group_count_for_mode_value = len(VALIDATION_MODE_GROUP_CODE)
_validation_groups_per_horizon = len(VALIDATION_THRESHOLDS) * _validation_group_count_for_mode_value
_validation_modes_per_side = len(VALIDATION_MODE_LAYOUT_HORIZONS) * _validation_groups_per_horizon
for horizon_index, _horizon in enumerate(VALIDATION_DISPLAY_HORIZONS):
    for threshold_index, _threshold in enumerate(VALIDATION_THRESHOLDS):
        for _group in VALIDATION_MODE_GROUPS:
            for _side_label, side_index in (("Baseline", 0), ("Test", 1)):
                if _threshold == "approximate":
                    threshold_label = "approximate"
                else:
                    threshold_label = "strict"
                if _horizon == "half-time-relaxed":
                    label = f"{_side_label} half-time-relaxed {threshold_label} validation {_group}"
                else:
                    label = f"{_side_label} time-relaxed {threshold_label} validation {_group}"
                DISPLAY_MODE_LABELS.append(label)
                mode_value = (
                    DISPLAY_MODE_VALIDATION_BEGIN
                    + side_index * _validation_modes_per_side
                    + VALIDATION_MODE_HORIZON_CODE[_horizon] * _validation_groups_per_horizon
                    + threshold_index * _validation_group_count_for_mode_value
                    + VALIDATION_MODE_GROUP_CODE[_group]
                )
                DISPLAY_MODE_VALUES.append(mode_value)
DISPLAY_MODE_LABELS.extend(
    [
        "Baseline hard collision",
        "Test hard collision",
        "Baseline soft collision",
        "Test soft collision",
    ]
)
DISPLAY_MODE_VALUES.extend(
    [
        DISPLAY_MODE_COLLISION_HARD_BASELINE,
        DISPLAY_MODE_COLLISION_HARD_TEST,
        DISPLAY_MODE_COLLISION_SOFT_BASELINE,
        DISPLAY_MODE_COLLISION_SOFT_TEST,
    ]
)

VALIDATION_SUMMARY_METRIC_WIDTH = 220
VALIDATION_SUMMARY_VALUE_COL_RATIO = (1, 1)  # Baseline : Test
OVERALL_LABEL = "overall"


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


class StableHScrollTableWidget(QTableWidget):
    """Keep horizontal scroll position stable on item selection/current-index changes."""

    def scrollTo(self, index, hint=QAbstractItemView.EnsureVisible):
        hbar = self.horizontalScrollBar()
        hval = hbar.value() if hbar is not None else 0
        super().scrollTo(index, hint)
        if hbar is not None:
            hbar.setValue(hval)


def _styled_mode_label_html(text: str) -> str:
    s = html.escape(text or "")
    color_map = {
        "along": "#1E8E3E",
        "opposite": "#D93025",
        "crossing": "#1A73E8",
        "other": "#F57C00",
    }
    for token, color in color_map.items():
        pat = re.compile(rf"\b({re.escape(token)})\b", re.IGNORECASE)
        s = pat.sub(lambda m: f'<span style="color:{color};">{m.group(1)}</span>', s)
    s = re.sub(r"\b(Baseline)\b", r"<b>\1</b>", s)
    s = re.sub(r"\b(Test)\b", r"<b>\1</b>", s)
    return s


class RichTextComboItemDelegate(QStyledItemDelegate):
    def paint(self, painter, option, index):
        opt = QStyleOptionViewItem(option)
        self.initStyleOption(opt, index)
        raw_text = opt.text
        opt.text = ""
        style = opt.widget.style() if opt.widget else QApplication.style()
        style.drawControl(QStyle.CE_ItemViewItem, opt, painter, opt.widget)

        text_rect = style.subElementRect(QStyle.SE_ItemViewItemText, opt, opt.widget)
        doc = QTextDocument()
        doc.setDefaultFont(opt.font)
        doc.setHtml(_styled_mode_label_html(raw_text))
        doc.setTextWidth(max(0.0, float(text_rect.width())))

        painter.save()
        painter.setClipRect(text_rect)
        y = text_rect.top() + (text_rect.height() - doc.size().height()) * 0.5
        painter.translate(text_rect.left(), y)
        ctx = QAbstractTextDocumentLayout.PaintContext()
        doc.documentLayout().draw(painter, ctx)
        painter.restore()


class RichTextComboBox(QComboBox):
    def paintEvent(self, event):
        painter = QStylePainter(self)
        opt = QStyleOptionComboBox()
        self.initStyleOption(opt)
        raw_text = opt.currentText
        opt.currentText = ""
        painter.drawComplexControl(QStyle.CC_ComboBox, opt)
        painter.drawControl(QStyle.CE_ComboBoxLabel, opt)

        text_rect = self.style().subControlRect(
            QStyle.CC_ComboBox, opt, QStyle.SC_ComboBoxEditField, self
        )
        doc = QTextDocument()
        doc.setDefaultFont(self.font())
        doc.setHtml(_styled_mode_label_html(raw_text))
        doc.setTextWidth(max(0.0, float(text_rect.width())))

        painter.save()
        painter.setClipRect(text_rect)
        y = text_rect.top() + (text_rect.height() - doc.size().height()) * 0.5
        painter.translate(text_rect.left(), y)
        ctx = QAbstractTextDocumentLayout.PaintContext()
        doc.documentLayout().draw(painter, ctx)
        painter.restore()

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


def _has_dt_fields(comp: dict | None) -> bool:
    if not isinstance(comp, dict):
        return False
    return ("dt_status_baseline" in comp) and ("dt_status_test" in comp)


def _dt_summary_text(comp: dict | None) -> str:
    if not _has_dt_fields(comp):
        return "-"
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
        return dt_status_label
    return f"{dt_status_label} ({max_val})"


def _dt_detail_text(comp: dict | None) -> str:
    if not _has_dt_fields(comp):
        return "-"
    dt_b = _dt_status_display(comp.get("dt_status_baseline"))
    dt_t = _dt_status_display(comp.get("dt_status_test"))
    dt_max_b = comp.get("dt_max_baseline") or "-"
    dt_max_t = comp.get("dt_max_test") or "-"
    return f"B:{dt_b}({dt_max_b}) / T:{dt_t}({dt_max_t})"


def _summary_state(comp: dict | None) -> str | None:
    if not comp:
        return None
    path_ok = comp.get("path_ok") if "path_ok" in comp else comp.get("path_and_traffic_ok")
    traffic_ok = comp.get("traffic_ok") if "traffic_ok" in comp else comp.get("path_and_traffic_ok")
    if bool(comp.get("lane_ids_ok")) and bool(comp.get("vsl_ok")) and bool(comp.get("object_ids_ok")) and bool(path_ok) and bool(traffic_ok):
        return STATUS_UNCHANGED
    return STATUS_CHANGED


def _lop_marker_states(comp: dict | None) -> tuple[str | None, str | None, str | None]:
    if not isinstance(comp, dict):
        return None, None, None
    lane_ok = bool(comp.get("lane_ids_ok"))
    vsl_ok = bool(comp.get("vsl_ok"))
    object_ok = bool(comp.get("object_ids_ok"))
    path_ok = bool(comp.get("path_ok")) if "path_ok" in comp else bool(comp.get("path_and_traffic_ok"))
    lane_marker = STATUS_UNCHANGED if (lane_ok and vsl_ok) else STATUS_CHANGED
    object_marker = STATUS_UNCHANGED if object_ok else STATUS_CHANGED
    path_marker = STATUS_UNCHANGED if path_ok else STATUS_CHANGED
    return lane_marker, object_marker, path_marker


def _ok_total(node: dict | None) -> tuple[int, int]:
    if not isinstance(node, dict):
        return 0, 0
    try:
        ok = int(node.get("ok", 0) or 0)
    except Exception:
        ok = 0
    try:
        total = int(node.get("total", 0) or 0)
    except Exception:
        total = 0
    return max(0, ok), max(0, total)


def _validation_metric_node(side_validation: dict | None, horizon: str, threshold: str):
    if not isinstance(side_validation, dict):
        return None
    hnode = side_validation.get(horizon)
    if not isinstance(hnode, dict):
        alt_horizon = horizon.replace("-", "_")
        hnode = side_validation.get(alt_horizon)
    if not isinstance(hnode, dict):
        alt_horizon = horizon.replace("_", "-")
        hnode = side_validation.get(alt_horizon)
    if isinstance(hnode, dict):
        node = hnode.get(threshold)
        if isinstance(node, dict):
            return node
        if horizon == "time-relaxed":
            node = hnode.get("time-relaxed") or hnode.get("loose")
            if isinstance(node, dict):
                return node
    # Backward compatibility: old schema had threshold keys at side root.
    node = side_validation.get(threshold)
    if isinstance(node, dict):
        return node
    if horizon == "time-relaxed":
        node = side_validation.get("time-relaxed") or side_validation.get("loose")
        if isinstance(node, dict):
            return node
    return None


def _half_time_relaxed_rate(side_validation: dict | None) -> float | None:
    node = _validation_metric_node(side_validation, "half-time-relaxed", "approximate")
    ok, total = _ok_total(node)
    if total <= 0:
        return None
    return 100.0 * float(ok) / float(total)


def _half_delta_mark(comp: dict | None) -> str:
    if not isinstance(comp, dict):
        return "-"
    validation = comp.get("validation", {})
    if not isinstance(validation, dict):
        return "-"
    baseline_rate = _half_time_relaxed_rate(validation.get("baseline"))
    test_rate = _half_time_relaxed_rate(validation.get("test"))
    if baseline_rate is None or test_rate is None:
        return "-"
    if abs(test_rate - baseline_rate) <= 1e-9:
        return "-"
    return "↑" if test_rate > baseline_rate else "↓"


def _collision_mark(comp: dict | None) -> str:
    if not isinstance(comp, dict):
        return "-"
    collision = comp.get("collision", {})
    if not isinstance(collision, dict):
        return "-"
    found = False
    for side in VALIDATION_SIDES:
        side_collision = collision.get(side, {})
        if not isinstance(side_collision, dict):
            continue
        for kind in COLLISION_KINDS:
            node = _collision_kind_node(side_collision, kind)
            if not isinstance(node, dict):
                continue
            found = True
            if bool(node.get("has_collision", False)):
                return "×"
    if found:
        return "○"
    return "-"


def _row_from_comp(rel: str, comp: dict | None) -> list[str]:
    """Return [toggle, half_delta, collision, L, O, P, rel, valid_frames, lane_ids, vsl, object_ids, path, traffic, segfault, dt_status]."""
    if not comp:
        return ["", "-", "-", "-", "-", "-", rel, "-", "-", "-", "-", "-", "-", "-", "-"]
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
    dt_status = _dt_summary_text(comp)
    half_delta = _half_delta_mark(comp)
    collision_mark = _collision_mark(comp)
    return ["", half_delta, collision_mark, "■", "■", "■", rel, valid_str, a, b, c, d, e, seg, dt_status]


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


def _collision_kind_node(side_collision: dict | None, kind: str):
    if not isinstance(side_collision, dict):
        return None
    node = side_collision.get(kind)
    return node if isinstance(node, dict) else None


def _collision_state_stats(node: dict | None) -> tuple[str, QColor | None]:
    if not isinstance(node, dict):
        return "-", None
    has_collision = bool(node.get("has_collision", False))
    if has_collision:
        return "Collision", COLOR_RED
    return "Safe", COLOR_GREEN


def _validation_summary_rows(comp: dict | None) -> list[tuple[str, str, str, QColor | None, QColor | None]]:
    validation = comp.get("validation", {}) if isinstance(comp, dict) else {}
    baseline = validation.get("baseline", {}) if isinstance(validation, dict) else {}
    test = validation.get("test", {}) if isinstance(validation, dict) else {}
    collision = comp.get("collision", {}) if isinstance(comp, dict) else {}
    collision_baseline = collision.get("baseline", {}) if isinstance(collision, dict) else {}
    collision_test = collision.get("test", {}) if isinstance(collision, dict) else {}
    rows: list[tuple[str, str, str, QColor | None, QColor | None]] = []
    for kind in COLLISION_KINDS:
        b_text, b_color = _collision_state_stats(_collision_kind_node(collision_baseline, kind))
        t_text, t_color = _collision_state_stats(_collision_kind_node(collision_test, kind))
        rows.append((f"{kind}-collision judgement", b_text, t_text, b_color, t_color))
    for horizon, threshold in VALIDATION_SUMMARY_ROWS:
        b_node = _validation_metric_node(baseline, horizon, threshold)
        t_node = _validation_metric_node(test, horizon, threshold)
        b_text, b_rate = _validation_rate_stats(b_node)
        t_text, t_rate = _validation_rate_stats(t_node)
        b_color = None
        t_color = None
        if b_rate is not None and t_rate is not None and abs(b_rate - t_rate) > 1e-9:
            if b_rate > t_rate:
                b_color = COLOR_GREEN
                t_color = COLOR_RED
            else:
                b_color = COLOR_RED
                t_color = COLOR_GREEN
        if threshold == "approximate":
            th_label = "approximate"
        else:
            th_label = "strict"
        if horizon == "half-time-relaxed":
            label = f"half-time-relaxed {th_label} success rate"
        else:
            label = f"time-relaxed {th_label} success rate"
        rows.append((label, b_text, t_text, b_color, t_color))
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


def _row_key(row_data: dict) -> tuple[str, str | None, int | None]:
    if row_data.get("is_overall"):
        return ("overall", None, None)
    rel = str(row_data.get("rel") or "")
    if row_data.get("is_subscene"):
        return ("subscene", rel, _to_int_nonneg(row_data.get("subscene_index"), 0))
    return ("scene", rel, None)


def _comp_total_frames(comp: dict | None) -> int | None:
    if not isinstance(comp, dict):
        return None
    total = comp.get("total_frames")
    if total is not None:
        try:
            return max(0, int(total))
        except Exception:
            return None
    valid = comp.get("valid_frames")
    rb = comp.get("record_frames_baseline")
    rt = comp.get("record_frames_test")
    if valid is None or rb is None or rt is None:
        return None
    try:
        return max(0, int(rb) + int(rt) - int(valid))
    except Exception:
        return None


def _to_int_nonneg(value, default=0) -> int:
    try:
        return max(0, int(value))
    except Exception:
        return default


def _to_float_or_none(value) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


def _dt_status_rank(status: str | None) -> int:
    s = _dt_status_display(status)
    if s == "invalid":
        return 2
    if s == "warning":
        return 1
    return 0


def _dt_status_from_rank(rank: int) -> str:
    if rank >= 2:
        return "invalid"
    if rank == 1:
        return "warning"
    return "valid"


def _aggregate_comparison(rows: list[tuple[str, Path, dict | None]]) -> dict | None:
    comps = [comp for _rel, _cmp_path, comp in rows if isinstance(comp, dict)]
    if not comps:
        return None

    agg_valid = 0
    agg_total = 0
    has_total = False
    lane_ok = True
    vsl_ok = True
    object_ok = True
    path_ok = True
    traffic_ok = True
    segfault_baseline = False
    segfault_test = False
    dt_rank_baseline = 0
    dt_rank_test = 0
    dt_max_baseline = None
    dt_max_test = None
    diff_by_source = _empty_diff_map(False)
    diff_counts_by_source = _empty_diff_map(0)

    validation = {
        side: {
            horizon: {
                threshold: {"ok": 0, "total": 0, "rate": 0.0, "detail": "overall"}
                for threshold in VALIDATION_THRESHOLDS
            }
            for horizon in VALIDATION_SUMMARY_HORIZONS
        }
        for side in VALIDATION_SIDES
    }
    collision = {
        side: {
            kind: {
                "has_collision": False,
                "collision_paths": 0,
                "checked_paths": 0,
                "detail": "overall",
            }
            for kind in COLLISION_KINDS
        }
        for side in VALIDATION_SIDES
    }

    for comp in comps:
        agg_valid += _to_int_nonneg(comp.get("valid_frames"), 0)
        total = _comp_total_frames(comp)
        if total is not None:
            agg_total += total
            has_total = True
        lane_ok = lane_ok and bool(comp.get("lane_ids_ok"))
        vsl_ok = vsl_ok and bool(comp.get("vsl_ok"))
        object_ok = object_ok and bool(comp.get("object_ids_ok"))
        _path_ok = comp.get("path_ok") if "path_ok" in comp else comp.get("path_and_traffic_ok")
        _traffic_ok = comp.get("traffic_ok") if "traffic_ok" in comp else comp.get("path_and_traffic_ok")
        path_ok = path_ok and bool(_path_ok)
        traffic_ok = traffic_ok and bool(_traffic_ok)
        segfault_baseline = segfault_baseline or bool(comp.get("segfault_baseline"))
        segfault_test = segfault_test or bool(comp.get("segfault_test"))
        dt_rank_baseline = max(dt_rank_baseline, _dt_status_rank(comp.get("dt_status_baseline")))
        dt_rank_test = max(dt_rank_test, _dt_status_rank(comp.get("dt_status_test")))
        max_b = _to_float_or_none(comp.get("dt_max_baseline"))
        max_t = _to_float_or_none(comp.get("dt_max_test"))
        if max_b is not None and (dt_max_baseline is None or max_b > dt_max_baseline):
            dt_max_baseline = max_b
        if max_t is not None and (dt_max_test is None or max_t > dt_max_test):
            dt_max_test = max_t

        comp_diff_map, comp_diff_counts = _extract_diff_maps(comp)
        for cat, _label in DIFF_CATEGORY_ROWS:
            for src in DIFF_SUPPORTED_SOURCES.get(cat, set()):
                diff_by_source[cat][src] = bool(diff_by_source[cat][src]) or bool(comp_diff_map.get(cat, {}).get(src, False))
                diff_counts_by_source[cat][src] += _to_int_nonneg(comp_diff_counts.get(cat, {}).get(src, 0), 0)

        comp_validation = comp.get("validation", {})
        for side in VALIDATION_SIDES:
            side_validation = comp_validation.get(side, {}) if isinstance(comp_validation, dict) else {}
            for horizon in VALIDATION_SUMMARY_HORIZONS:
                for threshold in VALIDATION_THRESHOLDS:
                    node = _validation_metric_node(side_validation, horizon, threshold)
                    ok, total_cnt = _ok_total(node)
                    validation[side][horizon][threshold]["ok"] += ok
                    validation[side][horizon][threshold]["total"] += total_cnt
        comp_collision = comp.get("collision", {})
        for side in VALIDATION_SIDES:
            side_collision = comp_collision.get(side, {}) if isinstance(comp_collision, dict) else {}
            for kind in COLLISION_KINDS:
                node = _collision_kind_node(side_collision, kind)
                if not isinstance(node, dict):
                    continue
                collision[side][kind]["has_collision"] = (
                    bool(collision[side][kind]["has_collision"]) or bool(node.get("has_collision", False))
                )
                collision[side][kind]["collision_paths"] += _to_int_nonneg(node.get("collision_paths"), 0)
                collision[side][kind]["checked_paths"] += _to_int_nonneg(node.get("checked_paths"), 0)

    for side in VALIDATION_SIDES:
        for horizon in VALIDATION_SUMMARY_HORIZONS:
            for threshold in VALIDATION_THRESHOLDS:
                node = validation[side][horizon][threshold]
                total_cnt = _to_int_nonneg(node.get("total"), 0)
                ok = _to_int_nonneg(node.get("ok"), 0)
                node["rate"] = (float(ok) / float(total_cnt)) if total_cnt > 0 else 0.0
                node["detail"] = "overall_aggregated"

    out = {
        "valid_frames": agg_valid,
        "lane_ids_ok": lane_ok,
        "vsl_ok": vsl_ok,
        "object_ids_ok": object_ok,
        "path_ok": path_ok,
        "traffic_ok": traffic_ok,
        "path_and_traffic_ok": bool(path_ok and traffic_ok),
        "segfault_baseline": segfault_baseline,
        "segfault_test": segfault_test,
        "dt_status_baseline": _dt_status_from_rank(dt_rank_baseline),
        "dt_status_test": _dt_status_from_rank(dt_rank_test),
        "dt_max_baseline": "-" if dt_max_baseline is None else dt_max_baseline,
        "dt_max_test": "-" if dt_max_test is None else dt_max_test,
        "diff_by_source": diff_by_source,
        "diff_counts_by_source": diff_counts_by_source,
        "validation": validation,
        "collision": collision,
        "_overall_total_dirs": len(rows),
        "_overall_loaded_dirs": len(comps),
    }
    if has_total:
        out["total_frames"] = agg_total
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
        self.current_subscene_index = None
        self.play_proc = None
        self.viewer_proc = None
        self.viewer_embed = None
        self._play_finished_timer = None
        self._play_paused = False
        self._playing_rel = None
        self._playing_subscene_index = None
        self._play_start_time = None
        self._viewer_launch_timer = QTimer(self)
        self._viewer_launch_timer.timeout.connect(self._check_viewer_launched)
        self._viewer_launch_start = None
        self._table_rows: list[dict] = []
        self._expanded_scene_rels: set[str] = set()
        self._selected_row_data: dict | None = None
        self._selected_subscene_table = None
        self._handling_selection_change = False
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
        self.table = StableHScrollTableWidget()
        self.table.setColumnCount(len(TABLE_HEADERS))
        self.table.setHorizontalHeaderLabels(TABLE_HEADERS)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSelectionMode(QTableWidget.SingleSelection)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
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
        self.validation_table.setRowCount(len(VALIDATION_SUMMARY_ROWS) + len(COLLISION_KINDS))
        self.validation_table.setColumnCount(3)
        self.validation_table.setHorizontalHeaderLabels(["Metric", "Baseline", "Test"])
        self.validation_table.setVerticalHeaderLabels(["", ""])
        self.validation_table.setSelectionMode(QTableWidget.NoSelection)
        self.validation_table.setEditTriggers(QTableWidget.NoEditTriggers)
        # Metric は固定幅、Baseline/Test は残り幅を固定比率で配分する。
        self.validation_table.horizontalHeader().setSectionResizeMode(QHeaderView.Fixed)
        self.validation_table.verticalHeader().setVisible(False)
        self.validation_table.setMaximumHeight(240)
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
        self.mode_combo = RichTextComboBox()
        self.mode_combo.setItemDelegate(RichTextComboItemDelegate(self.mode_combo))
        self.mode_combo.addItems(DISPLAY_MODE_LABELS)
        self.mode_combo.currentIndexChanged.connect(self._publish_display_mode)
        right_layout.addWidget(self.mode_combo)

        btn_layout = QHBoxLayout()
        self.btn_play = QPushButton("Play")
        self.btn_play.clicked.connect(self._play)
        self.btn_play.setEnabled(False)
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

    def _restore_indicator_columns(self):
        self.table.setColumnHidden(COL_TOGGLE, False)
        self.table.setColumnHidden(COL_HALF_DELTA, False)
        self.table.setColumnHidden(COL_COLLISION, False)
        self.table.setColumnHidden(COL_MARK_L, False)
        self.table.setColumnHidden(COL_MARK_O, False)
        self.table.setColumnHidden(COL_MARK_P, False)
        self.table.setColumnWidth(COL_TOGGLE, 28)
        self.table.setColumnWidth(COL_HALF_DELTA, 28)
        self.table.setColumnWidth(COL_COLLISION, 24)
        self.table.setColumnWidth(COL_MARK_L, 24)
        self.table.setColumnWidth(COL_MARK_O, 24)
        self.table.setColumnWidth(COL_MARK_P, 24)

    def _restore_list_viewport(self):
        self._restore_indicator_columns()

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

    def _make_scene_toggle_button(self, rel: str, expanded: bool) -> QToolButton:
        button = QToolButton(self.table)
        button.setText("▾" if expanded else "▸")
        button.setAutoRaise(True)
        button.clicked.connect(lambda _checked=False, scene_rel=rel: self._toggle_scene_expansion(scene_rel))
        return button

    def _populate_result_row(self, table: QTableWidget, row_index: int, row_data: dict):
        rel = str(row_data.get("display_rel") or row_data.get("rel") or "")
        comp = row_data.get("comp")
        cells = _row_from_comp(rel, comp)
        lane_marker, object_marker, path_marker = _lop_marker_states(comp)
        for j, text in enumerate(cells):
            item = QTableWidgetItem(text)
            if j == COL_HALF_DELTA:
                item.setTextAlignment(Qt.AlignCenter)
                if text == "↑":
                    item.setForeground(COLOR_GREEN)
                elif text == "↓":
                    item.setForeground(COLOR_RED)
            elif j == COL_COLLISION:
                item.setTextAlignment(Qt.AlignCenter)
                if text == "○":
                    item.setForeground(COLOR_GREEN)
                elif text == "×":
                    item.setForeground(COLOR_RED)
            elif j in (COL_MARK_L, COL_MARK_O, COL_MARK_P):
                item.setTextAlignment(Qt.AlignCenter)
                marker_state = None
                if j == COL_MARK_L:
                    marker_state = lane_marker
                elif j == COL_MARK_O:
                    marker_state = object_marker
                elif j == COL_MARK_P:
                    marker_state = path_marker
                if marker_state in STATUS_COLOR:
                    item.setForeground(STATUS_COLOR[marker_state])
            else:
                self._set_status_color(item)
            table.setItem(row_index, j, item)

        if table is self.table and row_data.get("is_scene") and row_data.get("has_children"):
            button = self._make_scene_toggle_button(
                str(row_data.get("rel") or ""),
                bool(row_data.get("expanded")),
            )
            table.setCellWidget(row_index, COL_TOGGLE, button)

    def _sync_child_table_layout(self, child_table: QTableWidget):
        child_table.setColumnHidden(COL_TOGGLE, True)
        for col in range(child_table.columnCount()):
            child_table.setColumnWidth(col, self.table.columnWidth(col))
        child_table.resizeRowsToContents()
        height = child_table.frameWidth() * 2
        if child_table.horizontalHeader().isVisible():
            height += child_table.horizontalHeader().height()
        for row in range(child_table.rowCount()):
            height += child_table.rowHeight(row)
        if child_table.horizontalScrollBar().isVisible():
            height += child_table.horizontalScrollBar().height()
        child_table.setMinimumHeight(height + 4)
        child_table.setMaximumHeight(height + 4)

    def _on_subscene_select(
        self,
        child_table: QTableWidget,
        child_rows: list[dict],
        parent_scene_row: int,
    ):
        if self._handling_selection_change:
            return
        sel = child_table.selectedIndexes()
        if not sel:
            return
        row = sel[0].row()
        if row < 0 or row >= len(child_rows):
            return
        self._handling_selection_change = True
        try:
            if self._selected_subscene_table is not None and self._selected_subscene_table is not child_table:
                self._selected_subscene_table.clearSelection()
            self._selected_subscene_table = child_table
            self.table.selectRow(parent_scene_row)
        finally:
            self._handling_selection_change = False
        self._apply_selected_row_data(child_rows[row])

    def _make_subscene_container(
        self,
        rel: str,
        cmp_path: Path | None,
        subscenes: list[dict],
        parent_scene_row: int,
    ) -> tuple[QWidget, QTableWidget, list[dict]]:
        child_rows: list[dict] = []
        for idx, sub in enumerate(subscenes):
            if not isinstance(sub, dict):
                continue
            label = str(sub.get("label") or f"subscene {idx}")
            subscene_index = _to_int_nonneg(sub.get("index"), idx)
            child_rows.append(
                {
                    "rel": rel,
                    "display_rel": label,
                    "cmp_path": cmp_path,
                    "comp": sub,
                    "is_overall": False,
                    "is_scene": False,
                    "is_subscene": True,
                    "subscene_index": subscene_index,
                }
            )

        child_table = StableHScrollTableWidget()
        child_table.setColumnCount(len(TABLE_HEADERS))
        child_headers = list(TABLE_HEADERS)
        child_headers[COL_DIRECTORY] = "Subsequence"
        child_table.setHorizontalHeaderLabels(child_headers)
        child_table.setRowCount(len(child_rows))
        child_table.setSelectionBehavior(QTableWidget.SelectRows)
        child_table.setSelectionMode(QTableWidget.SingleSelection)
        child_table.setEditTriggers(QTableWidget.NoEditTriggers)
        child_table.setItemDelegate(KeepSelectionColorDelegate(child_table))
        child_table.setStyleSheet(
            "QTableWidget::item:selected {"
            " background-color: rgba(90, 150, 255, 90);"
            "}"
        )
        child_table.verticalHeader().setVisible(False)
        for row_idx, child_row in enumerate(child_rows):
            self._populate_result_row(child_table, row_idx, child_row)
        child_table.itemSelectionChanged.connect(
            lambda table=child_table, rows=child_rows, parent_row=parent_scene_row: self._on_subscene_select(
                table, rows, parent_row
            )
        )

        container = QWidget()
        layout = QVBoxLayout(container)
        layout.setContentsMargins(24, 6, 0, 6)
        layout.addWidget(child_table)
        return container, child_table, child_rows

    def _refresh_list(self):
        prev_row = self._get_selected_row_data()
        prev_key = _row_key(prev_row) if prev_row else ("overall", None, None)
        rows = _collect_results(self.root, self.paths["test_results"])
        overall_comp = _aggregate_comparison(rows)
        table_rows = [{"rel": OVERALL_LABEL, "comp": overall_comp, "is_overall": True}]
        for rel, cmp_path, comp in rows:
            subscenes = comp.get("subscenes", []) if isinstance(comp, dict) else []
            if not isinstance(subscenes, list):
                subscenes = []
            has_children = bool(subscenes)
            expanded = has_children and (rel in self._expanded_scene_rels)
            table_rows.append(
                {
                    "rel": rel,
                    "display_rel": rel,
                    "cmp_path": cmp_path,
                    "comp": comp,
                    "is_overall": False,
                    "is_scene": True,
                    "has_children": has_children,
                    "expanded": expanded,
                    "subscenes": subscenes,
                }
            )
            if expanded:
                table_rows.append(
                    {
                        "rel": rel,
                        "is_container": True,
                        "is_overall": False,
                        "is_scene": False,
                        "cmp_path": cmp_path,
                        "subscenes": subscenes,
                    }
                )
        self._table_rows = table_rows

        self._handling_selection_change = True
        self._selected_subscene_table = None
        self.table.clearSpans()
        self.table.clearContents()
        self.table.setRowCount(len(table_rows))
        default_row_height = max(1, self.table.verticalHeader().defaultSectionSize())
        for row_index in range(len(table_rows)):
            self.table.setRowHeight(row_index, default_row_height)
        selected_main_row = 0
        selected_main_row_data = table_rows[0] if table_rows else None
        pending_child_selection = None
        child_tables: list[QTableWidget] = []
        child_containers: list[tuple[int, QWidget, QTableWidget]] = []

        for i, row_data in enumerate(table_rows):
            if row_data.get("is_container"):
                self.table.setSpan(i, 0, 1, self.table.columnCount())
                parent_scene_row = max(0, i - 1)
                container, child_table, child_rows = self._make_subscene_container(
                    str(row_data.get("rel") or ""),
                    row_data.get("cmp_path"),
                    row_data.get("subscenes") or [],
                    parent_scene_row,
                )
                self.table.setCellWidget(i, 0, container)
                child_tables.append(child_table)
                child_containers.append((i, container, child_table))
                self._sync_child_table_layout(child_table)
                self.table.setRowHeight(i, container.sizeHint().height())
                for child_row_idx, child_row in enumerate(child_rows):
                    if _row_key(child_row) == prev_key:
                        pending_child_selection = (child_table, child_row_idx, child_row, parent_scene_row)
                        selected_main_row = parent_scene_row
                        selected_main_row_data = self._table_rows[parent_scene_row]
                continue

            self._populate_result_row(self.table, i, row_data)
            if _row_key(row_data) == prev_key:
                selected_main_row = i
                selected_main_row_data = row_data

        self.table.resizeColumnsToContents()
        self.table.resizeRowsToContents()
        self._restore_list_viewport()
        for child_table in child_tables:
            self._sync_child_table_layout(child_table)
        for row_index, container, _child_table in child_containers:
            self.table.setRowHeight(row_index, container.sizeHint().height())
        self._handling_selection_change = False

        if self.table.rowCount() > 0:
            self.table.selectRow(selected_main_row)
        if pending_child_selection is not None:
            child_table, child_row_idx, child_row_data, parent_scene_row = pending_child_selection
            self._handling_selection_change = True
            try:
                self.table.selectRow(parent_scene_row)
            finally:
                self._handling_selection_change = False
            child_table.selectRow(child_row_idx)
            self._selected_subscene_table = child_table
            self._apply_selected_row_data(child_row_data)
        elif selected_main_row_data is not None:
            self._apply_selected_row_data(selected_main_row_data)
        else:
            self._selected_row_data = None
            self._refresh_play_button_enabled()

    def _apply_selected_row_data(self, row_data: dict | None):
        self._selected_row_data = row_data
        if not row_data:
            self.current_subscene_index = None
            self._refresh_play_button_enabled()
            return

        is_overall = bool(row_data.get("is_overall"))
        selected_rel = str(row_data.get("rel") or "")
        selected_subscene_index = row_data.get("subscene_index") if row_data.get("is_subscene") else None
        self.current_rel = None if is_overall else selected_rel
        self.current_subscene_index = None if is_overall else selected_subscene_index
        if not is_overall and not self.current_rel:
            self._refresh_play_button_enabled()
            return
        if (
            self.play_proc
            and self.play_proc.poll() is None
            and (
                self.current_rel != self._playing_rel
                or self.current_subscene_index != self._playing_subscene_index
            )
        ):
            self._end_playback()
        comp = row_data.get("comp")

        if comp is None:
            self._set_validation_table_rows(_validation_summary_rows(None))
            self._set_diff_table_no_data()
            if is_overall:
                total_dirs = sum(1 for item in self._table_rows if item.get("is_scene"))
                self._set_detail_table_rows([
                    ("Directory", OVERALL_LABEL),
                    ("comparison.json", f"loaded 0/{total_dirs}"),
                ])
            else:
                baseline_bag = self.baseline_root / self.current_rel / "result_baseline.bag"
                test_bag = self.test_results_root / self.current_rel / "result_test.bag"
                self._set_detail_table_rows([
                    ("Directory", self.current_rel),
                    ("Baseline bag", str(baseline_bag)),
                    ("Test bag", str(test_bag)),
                    ("comparison.json", "not found or parse error"),
                ])
            self._refresh_play_button_enabled()
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
            dt_text = _dt_detail_text(comp)

        if is_overall:
            loaded_dirs = _to_int_nonneg(comp.get("_overall_loaded_dirs"), 0)
            total_dirs = _to_int_nonneg(
                comp.get("_overall_total_dirs"),
                sum(1 for item in self._table_rows if item.get("is_scene")),
            )
            detail_rows = [
                ("Directory", OVERALL_LABEL),
                ("comparison.json", f"loaded {loaded_dirs}/{total_dirs}"),
                ("Valid/Total", valid_text),
                ("Lane IDs", lane_text),
                ("VSL", vsl_text),
                ("Object IDs", object_text),
                ("Path", path_text),
                ("Traffic", traffic_text),
                ("Seg fault", seg_text),
                ("DT status", dt_text),
            ]
        else:
            baseline_bag = self.baseline_root / self.current_rel / "result_baseline.bag"
            test_bag = self.test_results_root / self.current_rel / "result_test.bag"
            detail_rows = [("Directory", self.current_rel)]
            if row_data.get("is_subscene"):
                detail_rows.append(("Subscene", str(comp.get("label") or f"#{selected_subscene_index}")))
            detail_rows = [
                *detail_rows,
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
        self._restore_list_viewport()
        self._refresh_play_button_enabled()

    def _on_select(self):
        if self._handling_selection_change:
            return
        self._restore_list_viewport()
        sel = self.table.selectedIndexes()
        if not sel:
            self._selected_row_data = None
            self.current_subscene_index = None
            self._refresh_play_button_enabled()
            return
        row = sel[0].row()
        if row < 0 or row >= len(self._table_rows):
            self._selected_row_data = None
            self.current_subscene_index = None
            self._refresh_play_button_enabled()
            return
        row_data = self._table_rows[row]
        if row_data.get("is_container"):
            return
        self._handling_selection_change = True
        try:
            if self._selected_subscene_table is not None:
                self._selected_subscene_table.clearSelection()
                self._selected_subscene_table = None
        finally:
            self._handling_selection_change = False
        self._apply_selected_row_data(row_data)

    def _toggle_scene_expansion(self, rel: str):
        if not rel:
            return
        if rel in self._expanded_scene_rels:
            self._expanded_scene_rels.discard(rel)
        else:
            self._expanded_scene_rels.add(rel)
        self._selected_row_data = {"rel": rel, "is_overall": False, "is_scene": True}
        self._refresh_list()

    def _publish_display_mode(self, mode=None):
        try:
            if mode is None:
                mode = self.mode_combo.currentIndex()
            mode_index = int(mode)
            mode_value = DISPLAY_MODE_VALUES[mode_index] if 0 <= mode_index < len(DISPLAY_MODE_VALUES) else 0
            subprocess.run(
                ["rostopic", "pub", "-1", "/validation/display_mode", "std_msgs/Int32", str(mode_value)],
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

    def _get_selected_row_data(self):
        return self._selected_row_data

    def _get_selected_rel(self):
        """Currently selected table row's directory (rel), or None."""
        row_data = self._get_selected_row_data()
        if not row_data or row_data.get("is_overall"):
            return None
        rel = row_data.get("rel")
        return str(rel) if rel else None

    def _refresh_play_button_enabled(self):
        if self.play_proc and self.play_proc.poll() is None:
            self.btn_play.setEnabled(False)
            return
        self.btn_play.setEnabled(self._get_selected_rel() is not None)

    def _play(self):
        row_data = self._get_selected_row_data()
        rel = self._get_selected_rel()
        if not rel or not row_data:
            QMessageBox.warning(self, "Play", "Select a non-overall data row first.")
            return
        self.btn_play.setText("Preparing...")
        self.btn_play.setEnabled(False)
        QApplication.processEvents()  # 即座に描画し、準備中は押せないように見せる（連打防止）
        self._stop()
        self.current_rel = rel
        self.current_subscene_index = row_data.get("subscene_index") if row_data.get("is_subscene") else None
        set_viewer_rosparams(self.settings)
        baseline_bag = self.baseline_root / rel / "result_baseline.bag"
        observed_baseline_bag = self.baseline_root / rel / "observed_baseline.bag"
        collision_baseline_bag = self.baseline_root / rel / "collision_judgement_baseline.bag"
        collision_baseline_bag_legacy = self.baseline_root / rel / "collision_judgement.bag"
        validation_baseline_bag = self.baseline_root / rel / "validation_baseline.bag"
        test_bag = self.test_results_root / rel / "result_test.bag"
        observed_test_bag = self.test_results_root / rel / "observed_test.bag"
        collision_test_bag = self.test_results_root / rel / "collision_judgement_test.bag"
        collision_test_bag_legacy = self.test_results_root / rel / "collision_judgement.bag"
        validation_test_bag = self.test_results_root / rel / "validation_test.bag"
        out_dir = self.test_results_root / rel
        common_bag = out_dir / "common.bag"
        diff_baseline_bag = out_dir / "diff_baseline.bag"
        diff_test_bag = out_dir / "diff_test.bag"
        if not baseline_bag.exists():
            self.btn_play.setText("Play")
            self._refresh_play_button_enabled()
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
        if collision_baseline_bag.exists():
            play_bags.append(str(collision_baseline_bag))
        elif collision_baseline_bag_legacy.exists():
            play_bags.append(str(collision_baseline_bag_legacy))
        if validation_baseline_bag.exists():
            play_bags.append(str(validation_baseline_bag))
        if test_bag.exists():
            play_bags.append(str(test_bag))
        if observed_test_bag.exists():
            play_bags.append(str(observed_test_bag))
        if collision_test_bag.exists():
            play_bags.append(str(collision_test_bag))
        elif collision_test_bag_legacy.exists():
            play_bags.append(str(collision_test_bag_legacy))
        if validation_test_bag.exists():
            play_bags.append(str(validation_test_bag))
        if common_bag.exists():
            play_bags.append(str(common_bag))
        if diff_baseline_bag.exists():
            play_bags.append(str(diff_baseline_bag))
        if diff_test_bag.exists():
            play_bags.append(str(diff_test_bag))
        # backward compatibility: when unified validation bag is absent, load legacy split bags.
        if (not validation_baseline_bag.exists()) or (not validation_test_bag.exists()):
            for horizon in VALIDATION_MODE_LAYOUT_HORIZONS:
                for threshold in VALIDATION_THRESHOLDS:
                    for group in VALIDATION_GROUPS:
                        for side in VALIDATION_SIDES:
                            for status in VALIDATION_STATUSES:
                                validation_root = self.baseline_root if side == "baseline" else self.test_results_root
                                validation_bag = validation_root / rel / f"{horizon}_{threshold}_{group}_{side}_{status}.bag"
                                if validation_bag.exists():
                                    play_bags.append(str(validation_bag))
        self._playing_rel = rel
        self._playing_subscene_index = self.current_subscene_index
        self._publish_display_mode(self.mode_combo.currentIndex())
        subscene = row_data.get("comp") if row_data.get("is_subscene") else None
        start_sec = 0.0
        duration_sec = None
        if isinstance(subscene, dict):
            start_sec = max(0.0, _to_float_or_none(subscene.get("start_offset_sec")) or 0.0)
            duration_sec = max(0.0, _to_float_or_none(subscene.get("duration_sec")) or 0.0)
            if duration_sec <= 1e-6:
                duration_sec = None
        self._start_rosbag_play(play_bags, start_sec=start_sec, duration_sec=duration_sec)

    def _start_rosbag_play(self, play_bags: list[str], start_sec: float = 0.0, duration_sec: float | None = None):
        self._publish_clear_buffers()
        cmd = ["rosbag", "play"] + play_bags + ["--clock", "--loop"]
        if start_sec > 1e-6:
            cmd.extend(["-s", f"{start_sec:.3f}"])
        if duration_sec is not None and duration_sec > 1e-6:
            cmd.extend(["-u", f"{duration_sec:.3f}"])
        try:
            self.play_proc = subprocess.Popen(
                cmd,
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
        self._playing_subscene_index = None
        self.btn_play.setText("Play")
        self._refresh_play_button_enabled()
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
