"""
Common utilities for TP_tester: load settings, discover bag directories, git commit info.
"""
from __future__ import annotations

# 記録時のトピック名前空間（baseline/test 同時再生で競合しないようにする）
VALIDATION_BASELINE_NS = "/validation/baseline"
VALIDATION_TEST_NS = "/validation/test"

import json
import os
import selectors
import queue
import re
import shlex
import subprocess
import sys
import threading
import time
from pathlib import Path

try:
    import rosbag
    HAS_ROSBAG = True
except Exception:
    HAS_ROSBAG = False

# trajectory_predictor ログ行から dt を拾う正規表現（旧経路/PTY補助用）
DT_VALUE_RE = re.compile(r"dt\s*=\s*(\d+(?:\.\d+)?)", re.IGNORECASE)

# trajectory_predictor_io.cpp の ROS_INFO に合わせる（182行目: "Ready. Waiting for data..."）
NODE_READY_MARKERS = ("Ready. Waiting for data",)


def run_trajectory_predictor_offline(
    pkg: str,
    node_name: str,
    input_bags: list[Path],
    output_bag: Path,
    output_ns: str,
    map_name: str,
    timeout_sec: float | None = None,
) -> tuple[int, list[float], str]:
    """
    Run trajectory_predictor_sim_offline once for one scene.
    Returns (returncode, dt_values, combined_log_text).
    dt_values are read from --runtime-file output (not parsed from stdout/stderr).
    """
    cmd = ["rosrun", pkg, node_name]
    for bag in input_bags:
        cmd.extend(["--input-bag", str(bag)])
    runtime_file = output_bag.parent / f"{output_bag.stem}.runtime_ms.txt"
    try:
        if runtime_file.exists():
            runtime_file.unlink()
    except Exception:
        pass
    cmd.extend(
        [
            "--output-bag",
            str(output_bag),
            "--output-ns",
            output_ns,
            "--map-name",
            map_name,
            "--runtime-file",
            str(runtime_file),
        ]
    )

    log_parts: list[str] = []
    returncode = 124
    timed_out = False

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=os.environ,
    )
    assert proc.stdout is not None
    selector = selectors.DefaultSelector()
    selector.register(proc.stdout, selectors.EVENT_READ)

    start_time = time.monotonic()
    while True:
        if timeout_sec is not None and (time.monotonic() - start_time) > timeout_sec:
            timed_out = True
            try:
                proc.kill()
            except Exception:
                pass
            break

        events = selector.select(timeout=0.2)
        for key, _ in events:
            try:
                data = os.read(key.fileobj.fileno(), 4096)
            except OSError:
                data = b""
            if not data:
                try:
                    selector.unregister(key.fileobj)
                except Exception:
                    pass
                continue
            text = data.decode("utf-8", errors="replace")
            log_parts.append(text)
            if text:
                sys.stdout.write(text)
                sys.stdout.flush()

        if proc.poll() is not None:
            # Drain remaining bytes after process exit.
            while True:
                try:
                    data = os.read(proc.stdout.fileno(), 4096)
                except OSError:
                    data = b""
                if not data:
                    break
                text = data.decode("utf-8", errors="replace")
                log_parts.append(text)
                if text:
                    sys.stdout.write(text)
                    sys.stdout.flush()
            break

    try:
        selector.close()
    except Exception:
        pass

    if timed_out:
        try:
            proc.wait(timeout=2)
        except Exception:
            pass
        returncode = 124
    else:
        try:
            proc.wait(timeout=2)
        except Exception:
            pass
        returncode = int(proc.returncode if proc.returncode is not None else 1)

    log_text = "".join(log_parts)
    dt_values: list[float] = []
    if runtime_file.exists():
        try:
            for line in runtime_file.read_text(encoding="utf-8", errors="replace").splitlines():
                s = line.strip()
                if not s:
                    continue
                dt_values.append(float(s))
        except Exception:
            dt_values = []
    return (returncode, dt_values, log_text)


def check_offline_binary_supports_map_name(
    pkg: str,
    node_name: str,
    timeout_sec: float = 8.0,
) -> tuple[bool, str]:
    """
    Validate that trajectory_predictor_sim_offline supports required offline args.
    Returns (ok, detail_message).
    """
    cmd = ["rosrun", pkg, node_name, "--help"]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            env=os.environ,
        )
    except subprocess.TimeoutExpired:
        return (False, f"{' '.join(cmd)} timed out ({timeout_sec}s)")
    except FileNotFoundError:
        return (False, f"command not found: {cmd[0]}")
    except Exception as e:
        return (False, str(e))

    out = (proc.stdout or "") + ("\n" if proc.stdout and proc.stderr else "") + (proc.stderr or "")
    required_args = ("--map-name", "--runtime-file")
    missing = [opt for opt in required_args if opt not in out]
    if not missing:
        return (True, "")

    if proc.returncode != 0:
        return (
            False,
            f"{' '.join(cmd)} returned {proc.returncode} and does not advertise required args: {missing}",
        )
    return (False, f"offline binary help does not contain required args: {missing}")


class PTYCaptureState:
    """PTY 読み取りスレッドと共有。forward_to_terminal=False でターミナル表示を止め、dt 値を dt_values に蓄積する。"""
    __slots__ = ("forward_to_terminal", "dt_values", "_lock")

    def __init__(self) -> None:
        self.forward_to_terminal = True
        self.dt_values: list[float] = []
        self._lock = threading.Lock()

    def clear_dt(self) -> None:
        with self._lock:
            self.dt_values.clear()

    def take_dt_values(self) -> list[float]:
        with self._lock:
            out = list(self.dt_values)
            self.dt_values.clear()
            return out

    def append_dt(self, value: float) -> None:
        with self._lock:
            self.dt_values.append(value)


def _run_with_pty_and_wait_ready(
    cmd: list[str],
    markers: tuple[str, ...],
    timeout: float,
    capture_state: "PTYCaptureState | None" = None,
) -> tuple["subprocess.Popen | None", bool]:
    """
    PTY で cmd を起動。マーカーまで読み取りつつターミナルに表示し、(proc, ready_ok) を返す。
    capture_state を渡すと: forward_to_terminal が False の間は表示せず、dt=N をパースして append_dt。
    読み取りスレッドは proc 終了で EOF まで動く。Unix 専用。
    """
    try:
        import pty
    except ImportError:
        return (None, False)
    master_fd, slave_fd = pty.openpty()
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=slave_fd,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            env=os.environ,
            preexec_fn=os.setsid if os.name != "nt" else None,
        )
    except Exception:
        os.close(master_fd)
        os.close(slave_fd)
        return (None, False)
    os.close(slave_fd)
    ready = threading.Event()
    got_ready = [False]
    buf: list[str] = []

    def read_pty() -> None:
        nonlocal buf
        try:
            while True:
                chunk = os.read(master_fd, 4096).decode("utf-8", errors="replace")
                if not chunk:
                    break
                buf.append(chunk)
                line = "".join(buf)
                if "\n" not in line and "\r" not in line:
                    continue
                lines = line.replace("\r\n", "\n").replace("\r", "\n").split("\n")
                buf = [lines.pop(-1)] if lines else []
                for ln in lines:
                    if ln:
                        do_print = capture_state is None or capture_state.forward_to_terminal
                        if do_print:
                            print(ln, file=sys.stdout)
                            sys.stdout.flush()
                    if capture_state is not None:
                        for m in DT_VALUE_RE.finditer(ln or ""):
                            try:
                                capture_state.append_dt(float(m.group(1)))
                            except ValueError:
                                pass
                    if ready.is_set():
                        continue
                    line_lower = (ln or "").lower()
                    for m in markers:
                        if m.lower() in line_lower:
                            got_ready[0] = True
                            ready.set()
                            break
        except (ValueError, OSError):
            pass
        finally:
            try:
                os.close(master_fd)
            except OSError:
                pass

    t = threading.Thread(target=read_pty, daemon=True)
    t.start()
    ready.wait(timeout=timeout)
    return (proc, got_ready[0])


def wait_for_node_ready(
    proc: "subprocess.Popen",
    markers: tuple[str, ...] = NODE_READY_MARKERS,
    timeout: float = 30.0,
) -> bool:
    """
    proc の stdout/stderr を読み、マーカーが出るまでターミナルにそのまま表示しつつ待つ。
    PIPE のときは子プロセス側がフルバッファでブロックすることがあるため、
    run_baseline/run_test では start_trajectory_predictor_and_wait_ready()（PTY 使用）を推奨。
    戻り値: マーカーを検出したら True、タイムアウトなら False。
    """
    ready = threading.Event()
    line_queue: "queue.Queue[tuple[object, str] | None]" = queue.Queue()

    def read_stream(stream, out_file) -> None:
        try:
            if stream is None:
                return
            for line in iter(stream.readline, ""):
                if ready.is_set():
                    return
                if line:
                    line_queue.put((out_file, line))
                line_lower = (line or "").lower()
                for m in markers:
                    if m.lower() in line_lower:
                        ready.set()
                        return
        except (ValueError, OSError):
            pass

    def printer() -> None:
        while True:
            try:
                item = line_queue.get(timeout=0.1)
            except queue.Empty:
                if ready.is_set():
                    break
                continue
            if item is None:
                break
            out_file, line = item
            print(line, end="", file=out_file)
            out_file.flush()

    t_stdout = threading.Thread(
        target=read_stream, args=(proc.stdout, sys.stdout), daemon=True
    )
    t_stderr = threading.Thread(
        target=read_stream, args=(proc.stderr, sys.stderr), daemon=True
    )
    t_printer = threading.Thread(target=printer, daemon=False)
    t_printer.start()
    t_stdout.start()
    t_stderr.start()
    got = ready.wait(timeout=timeout)
    line_queue.put(None)
    t_printer.join(timeout=2.0)
    return got


def start_trajectory_predictor_and_wait_ready(
    cmd: list[str],
    timeout: float = 30.0,
) -> tuple["subprocess.Popen | None", bool, "PTYCaptureState | None"]:
    """
    trajectory_predictor を PTY で起動し、Ready まで待つ。PTY により子プロセスは TTY 扱いで
    行バッファになり「Succeed to load file」直後の ROS_INFO がブロックせず出る。
    戻り値: (proc, ready_ok, capture_state)。PTY 成功時は capture_state で再生中の dt 値を取得・表示制御可能。失敗時は (None, False, None)。
    """
    capture = PTYCaptureState()
    proc, ready = _run_with_pty_and_wait_ready(
        cmd, NODE_READY_MARKERS, timeout, capture_state=capture
    )
    return (proc, ready, capture if proc is not None else None)


def get_tester_root() -> Path:
    """TP_tester root: env TP_TESTER_ROOT or current working directory."""
    root = os.environ.get("TP_TESTER_ROOT")
    if root:
        return Path(root)
    return Path.cwd()


def load_settings(root: Path | None = None) -> dict:
    """Load settings.json from TP_tester root."""
    root = root or get_tester_root()
    path = root / "settings.json"
    if not path.exists():
        raise FileNotFoundError(f"settings.json not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def discover_bag_directories(test_bags_root: Path) -> list[tuple[str, Path]]:
    """
    Find all directories that contain at least one .bag file.
    Supports two structures:
    - Two-level: {日付}/{時刻}/*.bag  → rel e.g. "2026-03-09/11-12-34"
    - One-level: {名前}/*.bag        → rel e.g. "C28-06_OR_20250101-120000" (C28-06 部分は可変)
    Returns list of (relative_path, absolute_path).
    """
    if not test_bags_root.is_dir():
        return []
    out = []
    for top in sorted(test_bags_root.iterdir()):
        if not top.is_dir():
            continue
        # One-level: top に .bag が直接ある場合（例: C28-06_OR_日付-時間/*.bag）
        bags_direct = list(top.glob("*.bag"))
        if bags_direct:
            out.append((top.name, top))
            continue
        # Two-level: top/子ディレクトリ に .bag がある場合（例: 日付/時刻/*.bag）
        for sub in sorted(top.iterdir()):
            if not sub.is_dir():
                continue
            bags = list(sub.glob("*.bag"))
            if bags:
                rel = f"{top.name}/{sub.name}"
                out.append((rel, sub))
    return out


def get_bag_files_in_dir(dir_path: Path) -> list[Path]:
    """Return sorted list of .bag files in the directory."""
    return sorted(dir_path.glob("*.bag"))


def select_bag_files_by_topics(
    bag_files: list[Path],
    required_topics: list[str],
) -> tuple[list[Path], set[str]]:
    """
    Keep only bag files that contain at least one required topic.
    Returns (selected_bags, matched_topics).
    If python rosbag is unavailable, returns original bag_files.
    """
    if not bag_files or not required_topics:
        return (bag_files, set())
    if not HAS_ROSBAG:
        return (bag_files, set())

    required = set(required_topics)
    selected: list[Path] = []
    matched_topics: set[str] = set()

    for bag_path in bag_files:
        if not bag_path.exists():
            continue
        try:
            with rosbag.Bag(str(bag_path), "r") as bag:
                info = bag.get_type_and_topic_info()
                topics = set((info.topics or {}).keys())
        except Exception:
            continue

        hit = topics & required
        if hit:
            selected.append(bag_path)
            matched_topics |= hit

    if not selected:
        return (bag_files, set())
    return (selected, matched_topics)


def get_git_commit_info(repo_path: Path) -> dict:
    """
    Return dict with commit, describe, branch, dirty from the given path (git repo root).
    If not a git repo or git fails, returns commit="unknown", describe="", branch="", dirty=False.
    """
    out = {"commit": "unknown", "describe": "", "branch": "", "dirty": False, "source": "git"}
    if not repo_path.is_dir():
        return out
    try:
        r = subprocess.run(
            ["git", "-C", str(repo_path), "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=2
        )
        if r.returncode == 0 and r.stdout:
            out["commit"] = r.stdout.strip()
        r2 = subprocess.run(
            ["git", "-C", str(repo_path), "describe", "--always", "--dirty", "--tags"],
            capture_output=True, text=True, timeout=2
        )
        if r2.returncode == 0 and r2.stdout:
            out["describe"] = r2.stdout.strip()
            out["dirty"] = "--dirty" in (r2.stdout or "")
        r3 = subprocess.run(
            ["git", "-C", str(repo_path), "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True, text=True, timeout=2
        )
        if r3.returncode == 0 and r3.stdout:
            out["branch"] = r3.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired, Exception):
        pass
    return out


def _abs_path_no_resolve(p: Path, base: Path | None = None) -> Path:
    """絶対パスにするがシンボリックリンクは解決しない（nrc_ws のまま扱う）。"""
    if not p.is_absolute() and base is not None:
        p = base / p
    return Path(os.path.abspath(str(p)))


def get_nrc_ws_root(settings: dict, tester_root: Path | None = None) -> Path | None:
    """Return nrc_ws workspace root (parent of devel/) from settings or NRC_WS_DEVEL env, or None.
    nrc_ws_devel は devel/setup.bash のパス。シンボリックリンクは解決せず nrc_ws として扱う。
    """
    devel = settings.get("paths", {}).get("nrc_ws_devel", "") or os.environ.get("NRC_WS_DEVEL", "")
    if not devel:
        return None
    p = _abs_path_no_resolve(Path(devel), tester_root or get_tester_root() if not Path(devel).is_absolute() else None)
    # .../nrc_ws/devel/setup.bash → parent=devel, parent.parent=ws root
    return p.parent.parent


def get_bag_start_time(bag_path: "Path") -> float | None:
    """
    rosbag info で先頭時刻（秒）を取得。再生前の clock pre-roll 用。
    """
    try:
        r = subprocess.run(
            ["rosbag", "info", str(bag_path)],
            capture_output=True,
            text=True,
            timeout=10,
            env=os.environ,
        )
        if r.returncode != 0 or not r.stdout:
            return None
        for line in r.stdout.splitlines():
            line = line.strip()
            if line.startswith("start:"):
                return float(line.split(":", 1)[1].strip())
        return None
    except (FileNotFoundError, subprocess.TimeoutExpired, ValueError):
        return None


def run_clock_preroll(start_time_sec: float, duration_sec: float = 2.0) -> None:
    """
    再生開始前に /clock を bag の先頭時刻で duration_sec だけ 10Hz で publish し、
    trajectory_predictor のタイマー位相を揃える。
    根拠: TP は ros::Timer(0.1s) を使用しており、use_sim_time 時は ros::Timer が /clock 駆動になるため、
    先頭時刻で clock を流してから play すれば両 run で発火位相が揃う。2s で位相を安定させる。
    """
    secs = int(start_time_sec)
    nsecs = int(round((start_time_sec - secs) * 1e9))
    if nsecs >= 1e9:
        nsecs = 0
        secs += 1
    try:
        proc = subprocess.Popen(
            [
                "rostopic", "pub", "/clock", "rosgraph_msgs/Clock",
                f"clock: {{secs: {secs}, nsecs: {nsecs}}}",
                "-r", "10",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=os.environ,
        )
        time.sleep(duration_sec)
        proc.terminate()
        proc.wait(timeout=2)
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired):
        pass


def start_clock_publisher() -> "subprocess.Popen | None":
    """
    use_sim_time 時に trajectory_predictor が InitializeROSIO 内の ros::Time::now() 等で
    /clock 待ちしてブロックしないよう、/clock を 10Hz でパブリッシュするプロセスを起動する。
    戻り値の Popen は Ready 検出後に terminate すること。
    """
    try:
        proc = subprocess.Popen(
            [
                "rostopic", "pub", "/clock", "rosgraph_msgs/Clock",
                "clock: {secs: 0, nsecs: 0}",
                "-r", "10",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=os.environ,
        )
        return proc
    except (FileNotFoundError, OSError):
        return None


def kill_rosnodes_matching(name_substr: str) -> None:
    """
    既存の ROS ノードのうち名前が name_substr を含むものを kill する。
    バックグラウンドで残った trajectory_predictor 等が入力の取り合いで処理を飛ばすのを防ぐ。
    """
    try:
        r = subprocess.run(
            ["rosnode", "list"],
            capture_output=True,
            text=True,
            timeout=5,
            env=os.environ,
        )
        if r.returncode != 0 or not r.stdout:
            return
        for line in r.stdout.splitlines():
            node = line.strip()
            if name_substr in node:
                subprocess.run(
                    ["rosnode", "kill", node],
                    capture_output=True,
                    timeout=3,
                    env=os.environ,
                )
    except (FileNotFoundError, subprocess.TimeoutExpired, Exception):
        pass


def run_catkin_build(settings: dict) -> bool:
    """
    Run catkin_make --pkg nrc_wm_svcs in nrc_ws. Returns True on success, False on failure.
    Uses paths.nrc_ws_devel (or NRC_WS_DEVEL env) to derive workspace root (parent of devel).
    """
    root = get_tester_root()
    ws_root = get_nrc_ws_root(settings, tester_root=root)
    # catkin ワークスペースには src がある。無ければ NRC_WS_DEVEL から再取得（Docker 等で settings が相対パスだと TP_tester になることがある）
    if ws_root and (ws_root / "src").is_dir():
        pass
    elif os.environ.get("NRC_WS_DEVEL"):
        p = _abs_path_no_resolve(Path(os.environ["NRC_WS_DEVEL"])).parent.parent  # .../nrc_ws/devel/setup.bash → ws root
        if p.is_dir() and (p / "src").is_dir():
            ws_root = p
    if not ws_root or not ws_root.is_dir() or not (ws_root / "src").is_dir():
        nrc_devel = settings.get("paths", {}).get("nrc_ws_devel") or os.environ.get("NRC_WS_DEVEL", "")
        print("[catkin_make] nrc_ws が見つかりません。", file=sys.stderr)
        print(f"  nrc_ws_devel / NRC_WS_DEVEL: {nrc_devel}", file=sys.stderr)
        print(f"  ws_root 候補: {ws_root}", file=sys.stderr)
        if ws_root:
            print(f"  ws_root 存在: {ws_root.is_dir()}, src 存在: {(ws_root / 'src').is_dir()}", file=sys.stderr)
        return False
    ws_root_str = str(_abs_path_no_resolve(ws_root))  # シンボリックリンクは解決しない（nrc_ws のまま）
    # catkin_make は projects/nrc_ws（nrc_ws のルート）で実行する
    print(f"[catkin_make] 実行ディレクトリ: {ws_root_str}", file=sys.stderr)
    try:
        r = subprocess.run(
            ["bash", "-c", f"cd {shlex.quote(ws_root_str)} && catkin_make --pkg nrc_wm_svcs"],
            env=os.environ.copy(),
            timeout=600,
            capture_output=True,
            text=True,
        )
        if r.returncode != 0:
            print(f"[catkin_make] 実行ディレクトリ: {ws_root_str}", file=sys.stderr)
            if r.stdout:
                print(r.stdout, file=sys.stderr)
            if r.stderr:
                print(r.stderr, file=sys.stderr)
        return r.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired, Exception) as e:
        print(f"[catkin_make] error: {e}", file=sys.stderr)
        return False


def get_commit_info_for_run(settings: dict, env_key: str) -> dict:
    """
    Get commit info for a run (baseline or test).
    If environment variable env_key (e.g. TP_BASELINE_COMMIT) is set, use it as commit and describe.
    Otherwise derive repo path from settings and call get_git_commit_info().
    """
    explicit = os.environ.get(env_key)
    if explicit:
        return {"commit": explicit, "describe": explicit, "branch": "", "dirty": False, "source": "env"}
    root = get_tester_root()
    ws_root = get_nrc_ws_root(settings, tester_root=root)
    if not ws_root or not ws_root.is_dir():
        # run_catkin_build と同様に NRC_WS_DEVEL から再取得
        if os.environ.get("NRC_WS_DEVEL"):
            p2 = Path(os.environ["NRC_WS_DEVEL"])
            if p2.is_file():
                ws_root = _abs_path_no_resolve(p2).parent.parent
            elif p2.is_dir():
                ws_root = _abs_path_no_resolve(p2)
        if not ws_root or not ws_root.is_dir():
            return _commit_info_with_hint(get_git_commit_info(Path.cwd()), env_key, [])
    # コミット取得は git リポジトリ (nrc_wm) のみ。パッケージ (nrc_wm_svcs) とは別。
    repo = settings.get("node", {}).get("trajectory_predictor_repo", "nrc_wm")
    candidates = [
        ws_root / "src" / repo,
        ws_root / "src" / "nrc" / repo,
        ws_root,
    ]
    for candidate in candidates:
        if candidate.is_dir():
            info = get_git_commit_info(candidate)
            if info.get("commit") != "unknown":
                return info
    return _commit_info_with_hint(get_git_commit_info(ws_root), env_key, candidates)


def _commit_info_with_hint(info: dict, env_key: str, tried: list) -> dict:
    """commit が unknown のとき stderr にヒントと診断を出す。"""
    if info.get("commit") == "unknown":
        print(f"[commit] unknown (set {env_key}=<hash> to override; TP_DEBUG_COMMIT=1 for diagnostics)", file=sys.stderr)
        if os.environ.get("TP_DEBUG_COMMIT") and tried:
            for c in tried:
                exists = "exists" if c.is_dir() else "missing"
                try:
                    r = subprocess.run(
                        ["git", "-C", str(c), "rev-parse", "HEAD"],
                        capture_output=True, text=True, timeout=2,
                    )
                    why = f"git exit {r.returncode}" if not r.stdout else "ok"
                    print(f"  [commit] tried {c}: {exists}, {why}", file=sys.stderr)
                    if r.stderr and r.returncode != 0:
                        print(f"    stderr: {r.stderr.strip()[:200]}", file=sys.stderr)
                except FileNotFoundError:
                    print(f"  [commit] tried {c}: {exists}, git not found", file=sys.stderr)
                except Exception as e:
                    print(f"  [commit] tried {c}: {exists}, error: {e}", file=sys.stderr)
    return info
