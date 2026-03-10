#!/usr/bin/env python3
"""
指定トピックを監視し、連続して silence_sec 秒間メッセージが来なかったら exit 0 する。
trajectory_predictor_sim の逐次処理が終わったあと記録を止めるために使う。
"""
from __future__ import annotations

import argparse
import sys
import threading
import time

try:
    import rospy
    from nrc_msgs.msg import TrackedObjectSet2WithTrajectory
except ImportError as e:
    print(f"wait_publish_silence: import error: {e}", file=sys.stderr)
    sys.exit(2)

_last_msg_time: float | None = None


def _on_msg(_msg) -> None:
    global _last_msg_time
    _last_msg_time = time.time()


def main() -> int:
    parser = argparse.ArgumentParser(description="Exit after topic has been silent for N seconds.")
    parser.add_argument("topic", help="Topic to monitor (e.g. /validation/baseline/WM/tracked_object_set_with_prediction)")
    parser.add_argument("--silence-sec", type=float, default=1.0, help="Seconds of no messages to consider done (default: 1)")
    args = parser.parse_args()

    rospy.init_node("wait_publish_silence", anonymous=True)
    rospy.Subscriber(args.topic, TrackedObjectSet2WithTrajectory, _on_msg, queue_size=10)
    start = time.time()

    # use_sim_time 時は rospy.Rate が sim time でブロックするため、spin は別スレッドで実行しメインは wall clock のみで判定
    def spin_thread():
        rospy.spin()

    t = threading.Thread(target=spin_thread, daemon=True)
    t.start()

    while True:
        time.sleep(0.05)
        now = time.time()
        # 1秒間何も publish がなければ終了（1件も来てなくても「開始から1秒経過」= 1秒間なし とみなす）
        last_activity = _last_msg_time if _last_msg_time is not None else start
        if (now - last_activity) >= args.silence_sec:
            if _last_msg_time is None:
                print(f"wait_publish_silence: 開始から {args.silence_sec}s 間 publish なし → 記録終了", file=sys.stderr)
            else:
                print(f"wait_publish_silence: {args.silence_sec}s 間 publish なし → 記録終了", file=sys.stderr)
            break
    rospy.signal_shutdown("done")
    t.join(timeout=1.0)

    return 0


if __name__ == "__main__":
    sys.exit(main())
