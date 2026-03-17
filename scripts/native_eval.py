from __future__ import annotations

import ctypes
import os
from bisect import bisect_right
from pathlib import Path


MAX_DT = 0.26
HORIZON_KIND = {
    "half-time-relaxed": 0,
    "time-relaxed": 1,
}
THRESHOLD_KIND = {
    "approximate": 0,
    "strict": 1,
}


def _default_lib_path() -> Path:
    return Path(__file__).resolve().parent.parent / "native" / "libtp_tester_native_eval.so"


def _load_library():
    so_path = Path(os.environ.get("TP_TESTER_NATIVE_EVAL_SO", "")).expanduser()
    if not so_path:
        so_path = _default_lib_path()
    if not so_path.exists():
        return None
    try:
        lib = ctypes.CDLL(str(so_path))
    except Exception:
        return None

    double_p = ctypes.POINTER(ctypes.c_double)
    lib.tp_eval_native_version.argtypes = []
    lib.tp_eval_native_version.restype = ctypes.c_int
    lib.tp_eval_validation_path.argtypes = [
        double_p, double_p, double_p, double_p, ctypes.c_int,
        double_p, double_p, double_p, double_p, ctypes.c_int,
        ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_double, ctypes.c_double,
        ctypes.POINTER(ctypes.c_double),
    ]
    lib.tp_eval_validation_path.restype = ctypes.c_int
    lib.tp_eval_collision_results.argtypes = [
        double_p, double_p, double_p, double_p, double_p, ctypes.c_int,
        ctypes.c_double, ctypes.c_double,
        double_p, double_p, double_p, double_p, double_p, ctypes.c_int,
        ctypes.c_double, ctypes.c_double, ctypes.c_double,
        ctypes.POINTER(ctypes.c_int), ctypes.POINTER(ctypes.c_int),
    ]
    lib.tp_eval_collision_results.restype = None
    return lib


_LIB = _load_library()
NATIVE_EVAL_AVAILABLE = _LIB is not None


def _c_double_array(values):
    n = len(values)
    arr_t = ctypes.c_double * n
    return arr_t(*values)


def _path_cache_native_arrays(path_cache: dict | None):
    if not path_cache:
        return None
    cached = path_cache.get("native_eval_arrays")
    if cached is not None:
        return cached
    steps = path_cache.get("steps", [])
    if not steps:
        return None
    t_vals = [float(step.get("t", 0.0) or 0.0) for step in steps]
    x_vals = [float(step.get("x", 0.0) or 0.0) for step in steps]
    y_vals = [float(step.get("y", 0.0) or 0.0) for step in steps]
    yaw_vals = [float(step.get("yaw", 0.0) or 0.0) for step in steps]
    speed_vals = [abs(float(step.get("speed_hint", 0.0) or 0.0)) for step in steps]
    cached = {
        "count": len(steps),
        "times": t_vals,
        "t": _c_double_array(t_vals),
        "x": _c_double_array(x_vals),
        "y": _c_double_array(y_vals),
        "yaw": _c_double_array(yaw_vals),
        "speed": _c_double_array(speed_vals),
    }
    path_cache["native_eval_arrays"] = cached
    return cached


def _count_for_max_t(path_cache: dict | None, max_t: float | None) -> int:
    if not path_cache:
        return 0
    times = path_cache.get("times", [])
    if max_t is None:
        return len(times)
    cache_map = path_cache.setdefault("native_eval_index_cache", {})
    cache_key = round(float(max_t), 6)
    cached = cache_map.get(cache_key)
    if cached is not None:
        return int(cached)
    idx = bisect_right(times, float(max_t) + 1e-6)
    cache_map[cache_key] = int(idx)
    return int(idx)


def native_validation_match_from_cache(
    observed_path_cache: dict | None,
    predicted_path_cache: dict | None,
    horizon: str,
    threshold: str,
    horizon_max_t: float,
    classification: int,
    max_t: float | None = None,
):
    if not NATIVE_EVAL_AVAILABLE:
        return None
    horizon_kind = HORIZON_KIND.get(horizon)
    threshold_kind = THRESHOLD_KIND.get(threshold)
    if horizon_kind is None or threshold_kind is None:
        return None
    obs_arrays = _path_cache_native_arrays(observed_path_cache)
    pred_arrays = _path_cache_native_arrays(predicted_path_cache)
    if obs_arrays is None or pred_arrays is None:
        return None
    obs_count = _count_for_max_t(observed_path_cache, max_t)
    pred_count = _count_for_max_t(predicted_path_cache, max_t)
    if obs_count <= 0 or pred_count <= 0:
        return (False, float("inf"))
    out_cost = ctypes.c_double(float("inf"))
    rc = _LIB.tp_eval_validation_path(
        obs_arrays["t"],
        obs_arrays["x"],
        obs_arrays["y"],
        obs_arrays["yaw"],
        int(obs_count),
        pred_arrays["t"],
        pred_arrays["x"],
        pred_arrays["y"],
        pred_arrays["yaw"],
        int(pred_count),
        int(classification),
        int(horizon_kind),
        int(threshold_kind),
        float(horizon_max_t),
        float(MAX_DT),
        ctypes.byref(out_cost),
    )
    return (bool(rc), float(out_cost.value) if rc else float("inf"))


def native_collision_results_from_cache(
    pred_path_cache: dict | None,
    pred_dims: tuple[float, float],
    ego_path_cache: dict | None,
    ego_dims: tuple[float, float],
):
    if not NATIVE_EVAL_AVAILABLE:
        return None
    pred_arrays = _path_cache_native_arrays(pred_path_cache)
    ego_arrays = _path_cache_native_arrays(ego_path_cache)
    if pred_arrays is None or ego_arrays is None:
        return None
    out_hard = ctypes.c_int(0)
    out_soft = ctypes.c_int(0)
    _LIB.tp_eval_collision_results(
        pred_arrays["t"],
        pred_arrays["x"],
        pred_arrays["y"],
        pred_arrays["yaw"],
        pred_arrays["speed"],
        int(pred_arrays["count"]),
        float(pred_dims[0]),
        float(pred_dims[1]),
        ego_arrays["t"],
        ego_arrays["x"],
        ego_arrays["y"],
        ego_arrays["yaw"],
        ego_arrays["speed"],
        int(ego_arrays["count"]),
        float(ego_dims[0]),
        float(ego_dims[1]),
        float(MAX_DT),
        ctypes.byref(out_hard),
        ctypes.byref(out_soft),
    )
    return {
        "hard": bool(out_hard.value),
        "soft": bool(out_soft.value),
    }
