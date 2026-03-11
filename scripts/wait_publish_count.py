#!/usr/bin/env python3
"""
指定トピックの受信件数が expected_count に到達するまで待機する。
trajectory_predictor_sim の遅延吐き出しにより末尾フレームが記録漏れしないように使う。
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
    print(f"wait_publish_count: import error: {e}", file=sys.stderr)
    sys.exit(2)

_count = 0


def _on_msg(_msg) -> None:
    global _count
    _count += 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Wait until topic message count reaches expected.")
    parser.add_argument("topic", help="Topic to monitor")
    parser.add_argument("expected_count", type=int, help="Expected message count")
    parser.add_argument("--timeout-sec", type=float, default=60.0, help="Timeout seconds (default: 60)")
    parser.add_argument("--settle-sec", type=float, default=0.2, help="Extra wall-clock wait after reached (default: 0.2)")
    args = parser.parse_args()

    if args.expected_count <= 0:
        print("wait_publish_count: expected_count must be > 0", file=sys.stderr)
        return 2

    rospy.init_node("wait_publish_count", anonymous=True)
    rospy.Subscriber(args.topic, TrackedObjectSet2WithTrajectory, _on_msg, queue_size=100)
    start = time.time()

    def spin_thread():
        rospy.spin()

    t = threading.Thread(target=spin_thread, daemon=True)
    t.start()

    rc = 1
    while True:
        time.sleep(0.05)
        now = time.time()
        if _count >= args.expected_count:
            print(
                f"wait_publish_count: reached {args.expected_count} on {args.topic}"
                f" (actual={_count}), settle {args.settle_sec}s",
                file=sys.stderr,
            )
            time.sleep(max(args.settle_sec, 0.0))
            rc = 0
            break
        if (now - start) >= args.timeout_sec:
            print(
                f"wait_publish_count: timeout {args.timeout_sec}s on {args.topic}"
                f" (actual={_count}, expected={args.expected_count})",
                file=sys.stderr,
            )
            rc = 1
            break

    rospy.signal_shutdown("done")
    t.join(timeout=1.0)
    return rc


if __name__ == "__main__":
    sys.exit(main())

