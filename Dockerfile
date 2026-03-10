# TP_tester: run baseline/test/viewer with nrc_ws mounted at /home/nissan/projects
FROM ros:noetic-robot

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    python3-pip \
    python3-rosbag \
    ros-noetic-rosbag \
    ros-noetic-rosbash \
    x11-apps \
    xdotool \
    && rm -rf /var/lib/apt/lists/*

# Runtime libs for TrajectoryPredictorViewer_validation and similar nodes (host-built binary in mounted nrc_ws)
RUN apt-get update && apt-get install -y --no-install-recommends \
    freeglut3 \
    libglew2.1 \
    libgl1-mesa-glx \
    libglu1-mesa \
    libvtk7.1p \
    libopencv-core4.2 \
    libopencv-imgproc4.2 \
    libopencv-imgcodecs4.2 \
    libpcl-common1.10 \
    libpcl-io1.10 \
    libpcl-visualization1.10 \
    libxcb-xinerama0 \
    libxkbcommon0 \
    libace-6.4.5 \
    libace-dev \
    && rm -rf /var/lib/apt/lists/*

# Optional: PyQt5 for viewer GUI (fallback: tkinter)
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-pyqt5 \
    && rm -rf /var/lib/apt/lists/* || true

WORKDIR /home/nissan/TP_tester

# Default: run viewer (override with run_baseline.sh / run_test.sh)
CMD ["./run_viewer.sh"]
