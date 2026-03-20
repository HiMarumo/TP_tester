# TP_tester SPEC

## 1. 目的と適用範囲
このドキュメントは、`TP_tester` の現行実装（`scripts/*.py` と `TrajectoryPredictorViewer_validation`）に基づく仕様を定義する。

対象範囲:
- baseline 記録
- observed_baseline.bag / observed_test.bag 生成
- test 記録
- baseline/test 比較評価（Changed/Unchanged 判定）
- baseline/test collision 判定（hard/soft）
- `compare.bag` 生成（common / diff_baseline / diff_test をトピック名で分離した 1 ファイル）
- GUI ビューアと表示モード（validation は `half-time-relaxed-{approximate|strict}` / `time-relaxed-{approximate|strict}`、collision は `hard/soft` を表示）

注意:
- 本仕様は「実装準拠」。コード変更時は本ファイルを更新すること。

---

## 2. 用語
- `raw input bags`: `test_bags/<scene>/` 配下の入力 bag 群。
- `selected input bags`: scene の raw input bags から、必要topicを含む bag のみ抽出した入力bag集合。
- `scene`: 1回の比較単位（`discover_bag_directories` が返す `rel`）。
- `subscene`: 長尺sceneを 30 秒窓で区切った評価単位。scene 長が 40 秒以上のときのみ生成し、末尾 20 秒未満の窓は捨てる。
- `record_topics`: `settings.json` の9トピック。
- `common stamp`: baseline/test の参照 WM トピック（`/WM/tracked_object_set_with_prediction`）で共通に存在する stamp。
- `valid_frames`: common stamp の数。
- `total_frames`: 原則 raw input bags の `/clock` 総数（取れない場合はフォールバック）。

---

## 3. 設定・パス・トピック

### 3.1 settings.json
- `offline.map_name` (`trajectory_predictor_sim_offline` へ `--map-name` で渡す。default `new_MM_map`)
  - 実行時の解決順:
    1. `offline.map_name`
    2. `rosparam.map_name`（後方互換）
    3. `new_MM_map`
- `record_topics`:
  - `/WM/tracked_object_set_with_prediction`
  - `/WM/along_object_set_with_prediction`
  - `/WM/crossing_object_set_with_prediction`
  - `/WM/oncoming_object_set_with_prediction`
  - `/WM/other_object_set_with_prediction`
  - `/viz/su/multi_lane_ids_set`
  - `/viz/su/crossing_lane_ids_set`
  - `/viz/su/opposite_lane_ids_set`
  - `/viz/su/ym0_converted_traffic_light_state`
- `paths`:
  - `test_bags`
  - `baseline_results`
  - `test_results`
  - `nrc_ws_devel`
- `node`:
  - trajectory_predictor 実行用 pkg/node
  - viewer 実行用 pkg/node

### 3.2 実行時の ROS master 要否
- baseline/test の offline 実行（`run_baseline.sh`, `run_test.sh`）は ROS master 不要。
- viewer 実行（`run_viewer.sh`）は ROS master が必要。
- `run_viewer.sh` はホスト側で `rqt_image_view` を起動し、viewer（Docker）とは同一 ROS graph を使う。
  - 既定値: `RQT_IMAGE_TOPIC=/odet_cam_front_tele/image_raw`, `RQT_IMAGE_TRANSPORT=compressed`
  - `rqt_image_view --clear-config _image_transport:=compressed image:=<topic>` で起動する。
  - `rqt_image_view` 未インストール時は warning を出してスキップする。
  - `XDG_RUNTIME_DIR` 未設定時は `/tmp/runtime-<user>` を作成して利用する。

### 3.3 名前空間
- baseline 記録: `/validation/baseline`
- test 記録: `/validation/test`
- observed baseline: `/validation/observed_baseline`
- observed test: `/validation/observed_test`
- compare生成 common: `/validation/common`
- compare生成 diff baseline: `/validation/diff_baseline`
- compare生成 diff test: `/validation/diff_test`
- validation評価生成: `/validation/eval/<side>/<horizon>/<threshold>/<group>/<status>`
- collision評価生成: `/validation/collision/<side>/<hard|soft>/<status>`

---

## 4. 生成物（scene単位）

### 4.1 baseline_results
- `baseline_results/commit_info.json`
- `baseline_results/<rel>/result_baseline.bag`
- `baseline_results/<rel>/observed_baseline.bag`
- `baseline_results/<rel>/validation_baseline_summary.json`
- `baseline_results/<rel>/scene_timing.json`
- `baseline_results/<rel>/validation_baseline.bag`
- `baseline_results/<rel>/collision_judgement_baseline.bag`
- `baseline_results/<rel>/dt_status` (`valid|warning|invalid`)
- `baseline_results/<rel>/dt_max` (最大 dt、取れなければ `-`)
- `baseline_results/<rel>/dt_mean` (平均 dt、取れなければ `-`)
- `baseline_results/<rel>/dt_summary.json`（scene+subscene の `dt_status/dt_max/dt_mean/sample_count` と mapping 情報）
- `baseline_results/<rel>/segfault` (trajectory_predictor 異常終了時のみ touch)

### 4.2 test_results
- `test_results/commit_info.json`
- `test_results/<rel>/result_test.bag`
- `test_results/<rel>/observed_test.bag`
- `test_results/<rel>/validation_test_summary.json`
- `test_results/<rel>/scene_timing.json`
- `test_results/<rel>/validation_test.bag`
- `test_results/<rel>/collision_judgement_test.bag`
- `test_results/<rel>/comparison_direct.json`
- `test_results/<rel>/comparison.json`
- `test_results/<rel>/compare.bag`（common / diff_baseline / diff_test の全トピックを格納。旧形式の common.bag, diff_baseline.bag, diff_test.bag は新規生成しないが後方互換で読み込み可能）
- `test_results/<rel>/dt_status`, `dt_max`, `dt_mean`, `segfault`
- `test_results/<rel>/dt_summary.json`

---

## 5. 実行フロー

## 5.0 `trajectory_predictor_sim_offline` 入力仕様（timestamp基準）
対象:
- `projects/nrc_ws/src/nrc_wm/nrc_wm_svcs/src/trajectory_predictor/trajectory_predictor_io_sim.cpp`

### 5.0.1 全体構成
- `trajectory_predictor_sim_offline` は入力 bag を直接読み出して予測実行する。
- 入力同期は `TrackedObjectSet2` 基準で実装する（外部 `create_tp_sim_input` は使わない）。
- `rosbag play` / `rosbag record` は使わない。
- offline 実行では ROS master を必須としない。
- metric map は `--map-name` 引数で指定する。
- offline executable の `main` で `ros::Time::init()` を呼び、`ros::Time::now()` を NodeHandle 非依存で使用可能にする。

### 5.0.2 入力収集
- 駆動トリガは `TrackedObjectSet2`（既定 `/target_tracker/tracked_object_set2`）。
- 入力 bag 群を時刻順に走査し、トピックごとに内部バッファへ反映する。
- `TrackedObjectSet2` を読んだタイミングで 1 frame を組み立てて TP を1回実行する。
- 「最新値スナップショット」を使う topic:
  - `road_level_route` (`/route_plan/road_level_route`)
  - `traffic_light_state2` (`/traffic_light_state2`)
  - `destination_ad_state` (`/ad_state`)
- それ以外の stamped topic は `header.stamp.toSec()` をキーに map バッファ保持:
  - `global_pose` (`/dynamic_global_pose`)
  - `ego_motion` (`/RX/EgoMotion_info`)
  - `parked_object_set` (`/parked/tracked_object_set`)
  - `lane_level_route` (`/route_plan/lane_level_route`)
  - `lane_level_route_plan` (`/route_plan/lane_level_route_plan`)
  - `logical_pose` (`/LogicalPose`)
  - `controller_state` (`/controller_state`)
  - `odometry_input` (`/CAN/OdometryInput`)
  - `stopline_position` (`/DM/stopline_position`)
  - `stopline_decisions` (`/DM/stopline_decisions`)

### 5.0.3 同期規則（待機なし）
- queue 先頭 `TrackedObjectSet2` の時刻を `t_obj` とする。
- 各 stamped topic は `<= t_obj` の最新1件を採用する。
- 該当データが無い topic は `has_* = false`（空扱い）で処理継続する。
- 旧実装にあった待機・timeout fallback は行わない。

### 5.0.4 `TrajectoryPredictorSimInputFrame` の内部生成
- 各 `TrackedObjectSet2` メッセージに対して内部で `TrajectoryPredictorSimInputFrame` を組み立てる。
- `frame.tracked_object_set2` はその `TrackedObjectSet2` をそのまま使用。
- `frame.header` は `tracked_object_set2.header`。
- `has_*` が立っている入力のみ TP に適用。

### 5.0.5 出力stamp
- 出力 `header.stamp` は `tracked_object_set2.header.stamp` を使用。
- その cycle で生成する 9トピックは同一 `header` を共有し、output bag に直接 write する。
- bag write 時刻 `t` は入力メッセージ時刻（`msg_instance.getTime()`）を使い、0 の場合は `tracked_object_set2.header.stamp` を使う。

### 5.0.6 offline 実行時の進捗表示
- `TrackedObjectSet2` の総メッセージ数を先に数え、`processed/total` で進捗バーを更新する。
- 表示形式は 1 行更新（`\r`）で、`percent` が変化した時のみ更新する。
- `TrackedObjectSet2` が 0 件の場合はその旨をログ出力する。

## 5.1 baseline 作成（記録アルゴリズム）
対象: `run_baseline.sh` -> `run_baseline_docker.sh` -> `scripts/run_baseline.py`

### 5.1.1 baseline の入力の集め方（どのトピックをどう集めるか）
- `test_bags/<rel>/*.bag` から scene の入力 bag 群を取得する。
- `trajectory_predictor_sim_offline` 利用時は、必要topicを1つ以上含む bag のみを入力対象にする。
- 実行前に `rosparam set/get` は行わない。
- 実行前に `rosrun <pkg> <node> --help` を確認し、必須オプション（`--map-name`, `--runtime-file`）非対応バイナリならエラー終了する（旧バイナリでの固着防止）。
  - 必須判定対象 topic:
    - `/dynamic_global_pose`
    - `/target_tracker/tracked_object_set2`
    - `/RX/EgoMotion_info`
    - `/parked/tracked_object_set`
    - `/route_plan/road_level_route`
    - `/route_plan/lane_level_route`
    - `/route_plan/lane_level_route_plan`
    - `/LogicalPose`
    - `/controller_state`
    - `/CAN/OdometryInput`
    - `/DM/stopline_position`
    - `/traffic_light_state2`
    - `/DM/stopline_decisions`
    - `/ad_state`
  - `/target_tracker/tracked_object_set2` を含む bag が無い scene はエラー終了。
- scene ごとに以下を実行する:
  1. `rosrun nrc_wm_svcs trajectory_predictor_sim_offline --input-bag ... --output-bag ... --output-ns /validation/baseline --map-name <settings.offline.map_name> --runtime-file <result_baseline.runtime_ms.txt>`
  2. `--runtime-file` に出力された dt(ms) 一覧を読み取り `dt_status` / `dt_max` / `dt_mean` を生成
  3. 実行終了コードが 0 以外なら `segfault` を作成
- 生成対象は baseline 名前空間の 9 トピック:
  - `/validation/baseline/WM/tracked_object_set_with_prediction`
  - `/validation/baseline/WM/along_object_set_with_prediction`
  - `/validation/baseline/WM/crossing_object_set_with_prediction`
  - `/validation/baseline/WM/oncoming_object_set_with_prediction`
  - `/validation/baseline/WM/other_object_set_with_prediction`
  - `/validation/baseline/viz/su/multi_lane_ids_set`
  - `/validation/baseline/viz/su/crossing_lane_ids_set`
  - `/validation/baseline/viz/su/opposite_lane_ids_set`
  - `/validation/baseline/viz/su/ym0_converted_traffic_light_state`

### 5.1.2 baseline のタイムスタンプ基準
- baseline作成時、`TP_tester` はメッセージの `header.stamp` を書き換えない。
- `result_baseline.bag` の各メッセージの `header.stamp` は trajectory_predictor_sim_offline が生成した値そのまま。
- bag 時刻 (`t`) も trajectory_predictor_sim_offline の output bag write 時刻（入力側 message time）を使う。
- `trajectory_predictor_sim_offline` 利用時の stamp 決定規則は 5.0 を参照。

### 5.1.3 baseline の出力
- `result_baseline.bag` を sceneごとに生成。
- `dt_status` (`valid|warning|invalid`) / `dt_max` / `dt_mean` / `dt_summary.json` / `segfault` を付帯生成。
- その後に `observed_baseline.bag` を同sceneで生成（5.2）。
- `validation_baseline_summary.json`・`validation_baseline.bag`・`collision_judgement_baseline.bag` は、baseline 作成直後ではなく **test 実行時の評価** でまとめて生成する（5.3.3）。`run_evaluation.py --side all` で baseline/test 両方を評価するため、baseline だけを先に評価する必要はなく、二重に評価しない。

## 5.2 observed_baseline.bag / observed_test.bag 作成（baseline/testと別アルゴリズム）
対象:
- `scripts/run_baseline.py::_build_observed_bag_from_input`
- baseline作成時: `scripts/run_baseline.py` から `result_baseline.bag` を参照して `observed_baseline.bag` を生成
- test作成時: `scripts/run_test.py` から `result_test.bag` を参照して `observed_test.bag` を生成

### 5.2.1 observed の入力の集め方（トピック選択）
- observed 生成で読む入力bagは、その scene の topic選別後 bag 群を使う。
- 入力bag群から以下型トピックを1つ選んで使う:
  - `nrc_msgs/TrackedObjectSet2`
  - `nrc_msgs/TrajectoryPredictorSimInputFrame`
- 優先順:
  1. 呼び出し側が指定した `preferred_source_topic`
     - `settings.observed_source_topic`（設定されていれば）
  2. 固定候補順:
     - `/target_tracker/tracked_object_set2`
     - `/sensor_fusion/tracked_object_set2`
     - `/local_model/tracked_object_set2`
  3. それでも決まらなければ、対象型トピックのうちメッセージ数最多トピック
- 選択した source topic は baseline/test それぞれの observed 生成でログ出力される。

### 5.2.2 observed のタイムスタンプ基準とサンプル収集
- source topic の各メッセージを `record` として収集する。
- source メッセージが `TrajectoryPredictorSimInputFrame` の場合は、内部の `tracked_object_set2` を取り出して以降の処理に使う。
- 収集recordは以下で管理:
  - `stamp_ns`: `header.stamp` 優先（無ければ bag 時刻）
  - `bag_t_ns`: bag 時刻
  - `obj_by_id`: object_id -> object
- 全recordを `(stamp_ns, bag_t_ns)` でソート。
- 各 anchor record `i` について:
  - anchor時刻 `anchor_stamp_ns` を基準に、`0.0, 0.5, ..., 10.0` 秒先の `target_ns` を作る。
  - 各stepで `target_ns ± 0.26s` 範囲から「同一 object_id」を持つ record を探索する。
  - 対象stepで見つからない場合は、起点から horizon 終端（`+10.0s`）まで再接続を探索する。
    - 実際には収集対象 records の末尾時刻で探索は頭打ちになる。
  - 見つかった場合、直前の追加済みstepから再接続stepまでの未追加stepを線形補間で埋める。
  - horizon 終端（または records 末尾）まで再接続できない場合は、その object の軌跡を打ち切る。
- ID一致を前提に再接続するため、ID不一致の補間は行わない。

### 5.2.3 observed の生成内容
- baseline/test の「記録そのまま保存」とは異なり、observed は**再構成生成**する。
- anchor の各 object から `TrackedObject2WithTrajectory` を作り:
  - `out_obj.object` は anchor 時刻の観測値を全stepで固定利用。
  - `trajectory_set` は1本 (`prediction_mode=0`)。
  - trajectory 点は各stepで:
    - `t = step_i * 0.5`
    - `x,y,yaw,velocity` は、観測があるstepは観測値、欠損stepは前後観測間の線形補間値を使用
- `covariance` は各点で 16 要素ゼロを設定。

### 5.2.4 observed の group 分配（baseline/test結果との整合）
- observed 作成時に、対応する result bag (`result_baseline.bag` または `result_test.bag`) から
  `along/crossing/oncoming/other` 各トピックを読み、`stamp -> object_id集合` を作成する。
  - この `object_id集合` は「予測pathを持つ object（`trajectory_set` 内に点を持つ）」に限定する。
  - 特に `other` は「予測対象としての `other_object_set_with_prediction`」として解釈する。
- トピック解決は以下順:
  1. `result_ns + suffix`（例: `/validation/baseline/WM/along_object_set_with_prediction`）
  2. `suffix`（旧形式互換）
- observed base メッセージの各 stamp について:
  - along/crossing/oncoming/other の各メッセージは、上記 `stamp` で一致し、かつその group の ID 集合に含まれる object だけを出力。
  - 同一 object_id が複数groupに存在する場合は、各groupへ独立に出力する。

### 5.2.5 observed の出力タイムスタンプとトピック
- 出力メッセージの `header` は anchor メッセージの `header` をコピー。
- `rosbag.write(..., t)` の `t` は anchor record の bag 時刻を使用。
- 書き込みトピック:
  - baseline側 observed: `/validation/observed_baseline/WM/*`
  - test側 observed: `/validation/observed_test/WM/*`
  - いずれも `tracked/along/crossing/oncoming/other` の5トピックを書き出す。
- lane/traffic は observed 系 bag に書かない。

### 5.2.6 observed 生成時の進捗表示
- observed 再構成（anchor ごとの trajectory 組み立て）で進捗バーを表示する。
- observed bag 書き込み（base + group 4本）でも進捗バーを表示する。
- 表示は 1 行更新（`\r`）で、`percent` が変化した時のみ更新する。

### 5.2.7 observed と baseline/test の timestamp 軸の対応
- 5.0 の通り、baseline/test の9トピック `header.stamp` は `TrackedObjectSet2.header.stamp` が基準。
- observed も同じ `TrackedObjectSet2` 系列を anchor にして `header.stamp` をそのまま採用するため、比較時の時刻軸は対応づけ可能。
- `trajectory_predictor_sim_offline` 利用時は baseline/test とも同一 scene の topic選別後 bag 群から observed を作る。
- 入力 topic の選択は `settings.observed_source_topic` を優先するため、必要時は trajectory_predictor 入力 topic に合わせることを推奨。
- 差異は生成アルゴリズムのみ:
  - baseline/test: trajectory_predictor_sim_offline が算出した予測結果を記録。
  - observed: 入力観測から再構成した疑似 trajectory（prediction_mode=0）を生成。

## 5.3 test 作成（記録アルゴリズム）
対象: `run_test.sh` -> `run_test_docker.sh` -> `scripts/run_test.py`

### 5.3.1 test の入力の集め方
- `trajectory_predictor_sim_offline` 利用時は 5.1.1 と同じ topic選別を行い、入力 bag 群として渡す。
- 実行前に `rosparam set/get` は行わない。
- 実行前に必須オプション（`--map-name`, `--runtime-file`）対応チェックを行い、非対応ならエラー終了する。
- scene ごとに以下を実行する:
  1. `rosrun nrc_wm_svcs trajectory_predictor_sim_offline --input-bag ... --output-bag ... --output-ns /validation/test --map-name <settings.offline.map_name> --runtime-file <result_test.runtime_ms.txt>`
  2. `--runtime-file` に出力された dt(ms) 一覧を読み取り `dt_status` / `dt_max` / `dt_mean` を生成
  3. 実行終了コードが 0 以外なら `segfault` を作成
- 生成対象は test 名前空間の 9 トピック:
  - `/validation/test/WM/tracked_object_set_with_prediction`
  - `/validation/test/WM/along_object_set_with_prediction`
  - `/validation/test/WM/crossing_object_set_with_prediction`
  - `/validation/test/WM/oncoming_object_set_with_prediction`
  - `/validation/test/WM/other_object_set_with_prediction`
  - `/validation/test/viz/su/multi_lane_ids_set`
  - `/validation/test/viz/su/crossing_lane_ids_set`
  - `/validation/test/viz/su/opposite_lane_ids_set`
  - `/validation/test/viz/su/ym0_converted_traffic_light_state`

### 5.3.2 test のタイムスタンプ基準
- `result_test.bag` も `header.stamp` は trajectory_predictor_sim_offline 生成値そのまま（TP_testerで加工しない）。
- bag 時刻 (`t`) も trajectory_predictor_sim_offline の output bag write 時刻（入力側 message time）。
- `trajectory_predictor_sim_offline` 利用時の stamp 決定規則は 5.0 を参照。

### 5.3.3 test の出力と比較
- sceneごとに `result_test.bag`, `dt_status`, `dt_max`, `dt_mean`, `dt_summary.json`, `segfault` を生成。
- 各sceneで `result_test.bag` 作成成功後に `observed_test.bag` を生成（5.2）。
- **評価**は `run_evaluation.py` で行う。`--side all` で baseline と test の両方を評価し、各 scene で `validation_baseline_summary.json`・`validation_test_summary.json` および対応する validation/collision bag を生成する（baseline 側は baseline_results 配下、test 側は test_results 配下）。同一の評価処理を二重に回さない。
- 全scene後に `compare_baseline_test.py` を実行し、`comparison_direct.json` と `compare.bag` を生成（旧 `common.bag` / `diff_baseline.bag` / `diff_test.bag` は後方互換入力のみ）。
- その後 `aggregate_comparison_results.py` を実行し、`comparison_direct.json` と baseline/test の validation/collision summary を集約して `comparison.json` を生成。
- 補足:
  - `compare_baseline_test.py` 単体終了コード: 全scene Unchanged=0、1つでも Changed=1。
  - `run_test.py` は compare を `check=False` で呼ぶため、Changedでも `run_test.py` 自体は失敗扱いにしない。

---

## 6. 比較評価仕様 (`scripts/compare_baseline_test.py`)

## 6.1 スタンプ整列
- 全比較は stamp 整列で実施。
- baseline/test bag は名前空間付き（`/validation/baseline`/`/validation/test`）と旧形式（`/WM/...`, `/viz/su/...`）の混在を許容し、自動判定で読み分ける。
- stamp 取得は `header.stamp` 優先、無ければ bag time。
- 内部 `dict` 化時、同一stamp重複は「後勝ち」（最後のメッセージが残る）。
- 比較基準の common stamp は以下:
  - baseline 参照WMトピック stamp 集合 ∩ test 参照WMトピック stamp 集合

## 6.2 評価項目

### 6.2.1 Lane IDs (`lane_ids_ok`)
- 対象: lane 3トピック。
- source別に `along / opposite / crossing` を独立比較する。
- 1曲線 = lane id 配列を tuple 化（曲線内順序は保持）。
- 比較: 曲線集合の一致（配列の格納順は不問）。
- 不一致があれば `lane_ids_ok=False`。

### 6.2.2 Object IDs (`object_ids_ok`)
- 対象: WM 5トピック。
- source別に `along / opposite / crossing / other / base` を独立比較する。
- 各stampで `trajectory_set` を持つ object_id 集合を比較。
- さらに source別（along/opposite/crossing/other/base）に全stampの union 集合同士も比較。
- どこか不一致で `object_ids_ok=False`。

### 6.2.3 VSL (`vsl_ok`)
- 対象 object は `VSL-like` 判定で抽出する（object_id 固定比較に限定しない）。
- `VSL-like` 判定:
  - `object_id in {1, 500001}` または `object_id >= 500100`
  - または寸法が stopline 形状（`length <= 0.2` かつ `width >= 2.0`）
  - または `debug_info` に `vsl` / `stopline` を含む
- 対象 source: `along / opposite / crossing` の WM 3系統。
  - `opposite` はトピック上は `oncoming`（`/WM/oncoming_object_set_with_prediction`）として入力される。
- 比較キー: `(x,y,yaw)` を小数5桁丸めした multiset。
- object_id は比較に使わない（位置・向きのみ）。
- 不一致で `vsl_ok=False`。

### 6.2.4 Path (`path_ok`)
- 対象 source: `along / opposite / crossing / other` の WM 4系統（`base` は `path_ok` 判定対象外）。
- sourceごと・stampごとに object 単位で比較する。
- objectごとに `trajectory_set` を「path集合（multiset）」として比較する（path順は不問）。
- path署名は `((t,x,y), ...)`（各値は小数6桁丸め）で、path内の点順（時系列順）は一致必須。
- 不一致で `path_ok=False`。

### 6.2.5 Traffic (`traffic_ok`)
- 対象: traffic light トピック。
- 比較キー: `(stoplineid,state)` のソート済み配列。
- 不一致で `traffic_ok=False`。

### 6.2.6 総合 (`overall_ok`)
```
overall_ok = lane_ids_ok and vsl_ok and object_ids_ok and path_and_traffic_ok
path_and_traffic_ok = path_ok and traffic_ok
```

## 6.3 Changed / Unchanged 判定
- sceneの最終判定:
  - `overall_ok == True` -> `Unchanged`
  - `overall_ok == False` -> `Changed`
- 注意: `segfault` / `dt_status` は `overall_ok` の真偽には含まれない。

## 6.4 フレーム数指標
- `valid_frames`: common stamp 数。
- `record_frames_baseline/test`: 各参照WMトピックのユニークstamp数。
- `record_stamps_only_baseline/test(_count)`: 片側にしか無いstamp。
- `total_frames` 決定順:
  1. raw input bags の `/clock` 総数
  2. baseline/test 参照トピックのメッセージ数 max
  3. stamp 和集合サイズ

## 6.5 追加診断情報
- `segfault_baseline/test`: `segfault` ファイル有無。
- `dt_status_baseline/test`: `valid|warning|invalid`。
- `dt_max_baseline/test`: 最大 dt 値（文字列）。
- `dt_mean_baseline/test`: 平均 dt 値（文字列）。
- `dt_sample_count_baseline/test`: dt サンプル数。

## 6.6 compare 実行時の進捗表示
- scene ごとに比較進捗を表示する（lane + WM + traffic の比較ステップ合計を分母）。
- 表示は 1 行更新（`\r`）で、`percent` が変化した時のみ更新する。
- 欠損メッセージで比較をスキップする場合もステップを消費し、最終的に 100% へ到達する。

---

## 7. 比較/集約 JSON 構造
- `comparison_direct.json`:
  - 生成: `compare_baseline_test.py`
  - 内容: baseline/test の直接比較結果のみ（lane/vsl/object/path/traffic, diff集計, commit, dt, segfault など）
  - `scene_timing` と `subscenes[]` を含む。`subscenes[]` には direct compare の窓別集計を保持する。
- `comparison.json`:
  - 生成: `aggregate_comparison_results.py`
  - 内容: `comparison_direct.json` に observed validation summary（baseline/test）をマージした最終表示用JSON
  - scene root に加えて `subscenes[]` を持ち、各subsceneごとの direct compare / validation / collision を保持する。

### 7.1 `comparison.json` 主要キー
- 基本:
  - `directory`
  - `scene_timing.{start_ns,end_ns,duration_sec,subscenes[]}`
  - `lane_ids_ok`, `vsl_ok`, `object_ids_ok`, `path_ok`, `traffic_ok`, `path_and_traffic_ok`, `overall_ok`
  - `lane_ids_detail`, `vsl_detail`, `object_ids_detail`, `path_and_traffic_detail`
- フレーム統計:
  - `record_frames_baseline`, `record_frames_test`, `valid_frames`, `total_frames`
  - `record_stamps_only_baseline_count`, `record_stamps_only_test_count`
  - `record_stamps_only_baseline`, `record_stamps_only_test`（表示用サンプル。先頭100件のみ）
  - `record_baseline_topic_counts`, `record_test_topic_counts`
- source別差分:
  - `diff_by_source`
  - `diff_counts_by_source`
- commit/実行診断:
  - `baseline_commit`, `baseline_describe`, `test_commit`, `test_describe`
  - `segfault_baseline`, `segfault_test`
  - `dt_status_baseline`, `dt_status_test`, `dt_max_baseline`, `dt_max_test`
  - `dt_mean_baseline`, `dt_mean_test`, `dt_sample_count_baseline`, `dt_sample_count_test`
- observed妥当性評価:
  - `validation.baseline.half-time-relaxed.approximate.{ok,total,rate,detail}`
  - `validation.baseline.half-time-relaxed.strict.{ok,total,rate,detail}`
  - `validation.baseline.time-relaxed.approximate.{ok,total,rate,detail}`
  - `validation.baseline.time-relaxed.strict.{ok,total,rate,detail}`
  - `validation.test.half-time-relaxed.approximate.{ok,total,rate,detail}`
  - `validation.test.half-time-relaxed.strict.{ok,total,rate,detail}`
  - `validation.test.time-relaxed.approximate.{ok,total,rate,detail}`
  - `validation.test.time-relaxed.strict.{ok,total,rate,detail}`
  - `validation.baseline_by_class_group.<four_wheel|two_wheel|pedestrian>.<horizon>.<threshold>.{ok,total,rate,detail}`
  - `validation.test_by_class_group.<four_wheel|two_wheel|pedestrian>.<horizon>.<threshold>.{ok,total,rate,detail}`
  - `validation.baseline_common.<horizon>.<threshold>.{ok,total,rate,detail}`
  - `validation.test_common.<horizon>.<threshold>.{ok,total,rate,detail}`
  - `validation.baseline_common_by_class_group.<four_wheel|two_wheel|pedestrian>.<horizon>.<threshold>.{ok,total,rate,detail}`
  - `validation.test_common_by_class_group.<four_wheel|two_wheel|pedestrian>.<horizon>.<threshold>.{ok,total,rate,detail}`
  - `common_object_ids`（scene で共通評価対象となった object_id 一覧）
- collision評価:
  - `collision.baseline.hard.{has_collision,collision_paths,checked_paths,detail}`
  - `collision.baseline.soft.{has_collision,collision_paths,checked_paths,detail}`
  - `collision.test.hard.{has_collision,collision_paths,checked_paths,detail}`
  - `collision.test.soft.{has_collision,collision_paths,checked_paths,detail}`
  - `collision.<side>.<kind>.by_group.<along|opposite|crossing>.{has_collision,collision_paths,checked_paths,detail}`
  - `collision.baseline_common.<kind>.{has_collision,collision_paths,checked_paths,detail,by_group}`
  - `collision.test_common.<kind>.{has_collision,collision_paths,checked_paths,detail,by_group}`
- subscene配列:
  - `subscenes[]`
  - 各要素は `index`, `label`, `start_offset_sec`, `end_offset_sec`, `duration_sec` を持つ。
  - scene root と同じ direct compare key を持つ。
  - `validation.baseline|test|baseline_common|test_common.*` と `collision.baseline|test|baseline_common|test_common.*` も scene root と同型で保持する。
  - 各 subscene も `dt_status_*`, `dt_max_*`, `dt_mean_*`, `dt_sample_count_*`, `common_object_ids` を持つ。

### 7.2 common 再集約（aggregate）仕様
- `baseline_common/test_common` は、validation bag から抽出した `ID+stamp` の共通サンプル集合で再集計する。
- class group（`four_wheel/two_wheel/pedestrian`）も同じ `ID+stamp` 軸で再集計する。
- common再集約に失敗したsceneは処理を継続し、当該sceneの common 系フィールドを `*_common_samples_missing` で埋めてスキップする。
- `aggregate_comparison_results.py` は scene 全体進捗に加え、subscene merge の進捗バーを表示する。

---

## 8. common/diff bag 生成仕様
`compare_baseline_test.py::_write_diff_bags_impl`。
- 比較・差分判定はこの生成段階で完結させる。
- Viewer側（mode 2..9）は `compare.bag`（内部 namespace は `common/diff_*`）に保存済みの結果を選択描画するだけで、baseline/test の再差分計算は行わない。
- diff/common bag 生成中は進捗バーを表示する（計算 + 書き込みの合計ステップ基準）。

## 8.1 対象stamp
- `compare.bag`（旧: common.bag / diff_baseline.bag / diff_test.bag）は common stamp のみ対象。1 ファイルに全トピックを書き出す。
- 可能なら `/clock` も common stamp ごとに書く。
- topic単位でその stamp の実メッセージが無い場合は、`header.stamp` を合わせた空メッセージを書き込む
  （WM: `objects=[]`, lane: `arrays=[]`, traffic: `trafficlightlist=[]`）。

## 8.2 common トピック（`compare.bag` 内 `/validation/common/*`）
- 名前空間: `/validation/common/*`
- traffic: baseline 側 traffic を common stamp で保存。
- WM（source別 topic を分離保存）:
  - `/validation/common/WM/tracked_object_set_with_prediction`（base）
  - `/validation/common/WM/along_object_set_with_prediction`（along）
  - `/validation/common/WM/oncoming_object_set_with_prediction`（opposite 相当）
  - `/validation/common/WM/crossing_object_set_with_prediction`（crossing）
  - `/validation/common/WM/other_object_set_with_prediction`（other）
  - object_id差分なし・path完全一致 object -> object丸ごと保持。
  - 同一objectでpath差分あり -> 共通部分の path のみ保持（trajectory_set フィルタ）。
    - 共通pathの判定は「path集合（multiset）比較」。
    - `trajectory_set` 内の path 並び順は不問。
    - 各 path 内の点順（時系列順）は一致必須。
  - VSLは位置向き共通分を保持（ID無視 multiset）。
- lane（source別 topic を分離保存）:
  - `/validation/common/viz/su/multi_lane_ids_set`（along）
  - `/validation/common/viz/su/opposite_lane_ids_set`（opposite）
  - `/validation/common/viz/su/crossing_lane_ids_set`（crossing）
  - 内容は baseline/test 曲線集合の積集合のみ。
- 各 topic で stamp 欠落時は空メッセージを補完し、common stamp 軸を維持する。

## 8.3 diff_baseline トピック（`compare.bag` 内 `/validation/diff_baseline/*`）
- 名前空間: `/validation/diff_baseline/*`
- traffic: baseline 側 traffic を common stamp で保存。
- WM（source別 topic を分離保存。common stamp 全てでメッセージ生成、差分なしは空）:
  - `/validation/diff_baseline/WM/tracked_object_set_with_prediction`（base）
  - `/validation/diff_baseline/WM/along_object_set_with_prediction`（along）
  - `/validation/diff_baseline/WM/oncoming_object_set_with_prediction`（opposite 相当）
  - `/validation/diff_baseline/WM/crossing_object_set_with_prediction`（crossing）
  - `/validation/diff_baseline/WM/other_object_set_with_prediction`（other）
  - baseline にのみ存在する object_id -> object丸ごと保持。
  - 共通objectで baseline 側のみの path -> その path のみ保持。
  - VSL baseline-only（位置向き差分）を保持。
- lane（source別 topic を分離保存）:
  - `/validation/diff_baseline/viz/su/multi_lane_ids_set`（along）
  - `/validation/diff_baseline/viz/su/opposite_lane_ids_set`（opposite）
  - `/validation/diff_baseline/viz/su/crossing_lane_ids_set`（crossing）
  - 内容は baseline-only 曲線。
- 各 topic で stamp 欠落時は空メッセージを補完する。

## 8.4 diff_test トピック（`compare.bag` 内 `/validation/diff_test/*`）
- 名前空間: `/validation/diff_test/*`
- traffic: test 側 traffic を common stamp で保存。
- WM（source別 topic を分離保存）:
  - `/validation/diff_test/WM/tracked_object_set_with_prediction`（base）
  - `/validation/diff_test/WM/along_object_set_with_prediction`（along）
  - `/validation/diff_test/WM/oncoming_object_set_with_prediction`（opposite 相当）
  - `/validation/diff_test/WM/crossing_object_set_with_prediction`（crossing）
  - `/validation/diff_test/WM/other_object_set_with_prediction`（other）
  - test にのみ存在する object_id -> object丸ごと保持。
  - 共通objectで test 側のみの path -> その path のみ保持。
  - VSL test-only（位置向き差分）を保持。
- lane（source別 topic を分離保存）:
  - `/validation/diff_test/viz/su/multi_lane_ids_set`（along）
  - `/validation/diff_test/viz/su/opposite_lane_ids_set`（opposite）
  - `/validation/diff_test/viz/su/crossing_lane_ids_set`（crossing）
  - 内容は test-only 曲線。
- 各 topic で stamp 欠落時は空メッセージを補完する。

---

## 9. Viewer GUI 仕様 (`scripts/viewer_app.py`)

## 9.1 一覧表示（左表）
- 列: `toggle`, `↑/↓/-`, `C`, `L`, `O`, `P`, `Directory`, `Valid/Total`, `Seg fault`, `DT status`, `Lane IDs`, `VSL`, `Object IDs`, `Path`, `Traffic`。
- 先頭行に `overall` を追加し、全sceneの `comparison.json` を集約した総合評価を表示する。
- 起動時のデフォルト選択は `overall` 行とする。
- scene 行は最左 `toggle` 列のボタン（`▸/▾`）で展開/折りたたみする。
- 展開時はインデント付きの子テーブルを挿入し、子テーブルの `Directory` ヘッダは `Subsequence` とする。
- 親行は scene 全体の評価、子行は当該 subscene の評価を表示する。
- `Play` ボタンは「scene行またはsubscene行が選択されている場合のみ」有効化する。未選択または `overall` 選択時は無効（グレーアウト）とする。
- `toggle` 列と `↑/↓/-` 列のヘッダ文字は空（非表示）とし、`C/L/O/P` 列のヘッダにはそれぞれ `C`, `L`, `O`, `P` を表示する。
- `C` 列は collision 判定結果を表示する:
  - **test 側** hard/soft のいずれかで `has_collision=true` が1件でもあれば `×`（赤）
  - test 側 hard/soft がすべて `has_collision=false` なら `○`（緑）
  - collision summary が欠損している場合は `-`（黒）
- `DT status` 列は test 側の `dt_status_test` / `dt_max_test` を表示する。
- `L/O/P` 列は `■` マーカーで changed/unchanged を表示する:
  - `L`: `Lane IDs` と `VSL` の統合判定（どちらか一方でも changed なら changed）
  - `O`: `Object IDs`
  - `P`: `Path`
  - `Unchanged` -> 緑、`Changed` -> 赤
- ステータス文字色:
  - `Unchanged` 緑
  - `Changed` 赤
  - `No` 緑, `Yes` 赤
  - `Valid` 緑, `Warning` 橙, `Invalid` 赤
- 選択ハイライト中でも文字色は保持（delegateで HighlightedText を上書き）。
- 最左列 `↑/↓/-` は `half-time-relaxed approximate` success rate の Baseline/Test 比較で判定する。
  - test > baseline: `↑`（緑）
  - test < baseline: `↓`（赤）
  - 同値または欠損: `-`（黒）

## 9.2 Diff matrix（右上）
- 行: Lane IDs / Object IDs / Path / VSL / Traffic
- 列: along / opposite / crossing / other / base
- `base` は `tracked_object_set_with_prediction`（group分割前の全体集合）を意味する。
- ただし VSL 行は `along / opposite / crossing` のみ対象（`other` と `base` は `-`）。
- Traffic 行は `base` のみ対象（それ以外は `-`）。
- 値は `comparison.json.diff_by_source` と `diff_counts_by_source` 由来。
- `overall` 選択時は全sceneの差分を source別に合算した結果を表示する。
- Diff matrix の高さは固定（全行が収まる高さ）で、縦方向の余剰は validation summary 側へ配分する。

## 9.3 Detail（右中）
- 常時表示:
  - `Baseline commit: ...`
  - `Test commit: ...`
- scene選択時: 選択sceneの summary と bag パスを表形式で表示。
- subscene選択時: scene名に加えて subscene ラベルを表示し、そのsubscene集計値を表形式で表示する。
- `overall` 選択時: 全scene集約の summary（`comparison.json` 読み込み件数を含む）を表形式で表示する。
- Detail の主要項目順は左表サマリー列と揃える（`Valid/Total` → `Seg fault` → `DT status` → `Lane IDs` → `VSL` → `Object IDs` → `Path` → `Traffic`）。

## 9.4 Validation summary（右上）
- `Metric / Baseline / Test` の3列表示。
- 表示順:
  1. `DT status`
  2. `DT mean`
  3. `half-time-relaxed/time-relaxed` の common success rate（approximate/strict）
  4. `hard/soft-collision judgement`
  5. 通常 success rate（approximate/strict）
- `DT status` は `Invalid` 赤 / `Warning` 橙 / `Valid` 緑で表示。
- collision は `Collision (N)` / `Safe` 表示（`N = collision_paths`）。
- `DT` 行以外はトグル展開可能:
  - validation 行の展開: `four_wheel / two_wheel / pedestrian` の内訳を表示。
  - collision 行の展開: `along / crossing / opposite` の内訳を表示。
- collision の `common` は評価軸として採用していないため、validation summary には表示しない。

## 9.5 Viewer 起動
- `rosrun <viewer_pkg> <viewer_validation_node> -m` を実行。
- 引数 remap:
  - baseline topics: `_xxx_topic:=/validation/baseline/...`
  - test topics: `_test_xxx_topic:=/validation/test/...`
  - observed baseline WM topics: `_observed_xxx_topic:=/validation/observed_baseline/...`
  - observed test WM topics: `_observed_test_xxx_topic:=/validation/observed_test/...`
- `/validation/display_mode` に Int32 publish してモード切替。
- `/validation/clear_buffers` を再生前後に publish。
- display mode 選択肢の文字装飾:
  - `along`: 緑
  - `opposite`: 赤
  - `crossing`: 青
  - `other`: オレンジ
  - `Baseline` / `Test` / `Compare`: 太字
  - 上記は該当語のみを部分装飾し、ラベル全文の単色化は行わない。
- GUI の display mode 先頭に `Compare`（mode=59）を配置し、デフォルト選択にする。
- display mode 選択肢の並び（Diff path系）:
  - `Diff along path` → `Diff along path with observed` → `Diff opposite path` → `Diff opposite path with observed` → `Diff crossing path` → `Diff crossing path with observed` → `Diff other path` → `Diff other path with observed`

## 9.6 再生 bag セット
Play時に **常に全bagを同時再生** し、表示は mode で切替する。
再生順:
1. `test_bags/<rel>/*.bag`（入力bag群。`images*.bag` も含む。重複は除去）
2. `baseline_results/<rel>/result_baseline.bag`
3. `baseline_results/<rel>/observed_baseline.bag`（存在時）
4. `baseline_results/<rel>/collision_judgement_baseline.bag`（存在時、なければ legacy `collision_judgement.bag`）
5. `baseline_results/<rel>/validation_baseline.bag`（存在時）
6. `test_results/<rel>/result_test.bag`（存在時）
7. `test_results/<rel>/observed_test.bag`（存在時）
8. `test_results/<rel>/collision_judgement_test.bag`（存在時、なければ legacy `collision_judgement.bag`）
9. `test_results/<rel>/validation_test.bag`（存在時）
10. `test_results/<rel>/compare.bag`（存在時。なければ legacy `common.bag` / `diff_baseline.bag` / `diff_test.bag` を後方互換で使用）
- `Play` 押下直後はボタン文言を `Preparing...` に変更し、起動完了まで押下不可にする。

scene親行の再生は `rosbag play --clock --loop` を使用する。
subscene子行の再生は、上記bag集合に対して `-s <start_offset_sec>` と `-u <duration_sec>` を付けて、該当区間のみをループ再生する。

## 9.7 長尺sceneのsubscene分割
- scene 長は raw input bags の bag time 範囲（最小 start 〜 最大 end）から求める。
- `duration_sec >= 40.0` の scene を分割対象とする。
- 窓長は 30.0 秒固定。
- 末尾窓の長さが 20.0 秒未満なら、その末尾窓は生成しない。
- observed 生成 / offline 実行 / bag 出力は scene 全体を使う。
- direct compare / validation / collision の数値集計のみを subscene ごとに算出する。
- `compare.bag` / `validation_*.bag` / `collision_judgement_*.bag` は scene ごとに1ファイルのみ生成する。

---

## 10. 描画モード仕様（TrajectoryPredictorViewer_validation）

## 10.1 mode一覧
- 0: Baseline
- 1: Test
- 2: Diff overlay
- 3: Diff along lane
- 4: Diff opposite lane
- 5: Diff crossing lane
- 6: Diff along path
- 7: Diff opposite path
- 8: Diff crossing path
- 9: Diff other path
- 10: Diff along path with observed
- 11: Diff opposite path with observed
- 12: Diff crossing path with observed
- 13: Diff other path with observed
- 14: Observed
- 15..34: Baseline validation（`half-time-relaxed` / `time-relaxed` × `approximate` / `strict` を group別に表示。groupは `along/opposite/crossing/other/all`）
- 35..54: Test validation（`half-time-relaxed` / `time-relaxed` × `approximate` / `strict` を group別に表示。groupは `along/opposite/crossing/other/all`）
- 55: Baseline hard collision
- 56: Test hard collision
- 57: Baseline soft collision
- 58: Test soft collision
- 59: Compare
- GUI viewer の display mode 選択肢では、validation系は `half-time-relaxed` のみ表示する（`time-relaxed` は backend 実装として残し、選択肢からは除外）。

## 10.2 mode別の入力ソース
- 0: baseline stream (`/validation/baseline/*`)
- 1: test stream (`/validation/test/*`)
- 2: `common + diff_baseline + diff_test`
- 3..9: `common + diff_baseline + diff_test`（再差分計算なし、bag内容を直接描画）
- 10..13: `common + diff_baseline + diff_test + observed baseline`（`Diff * path` に observed path を重畳）
- 14: observed baseline stream (`/validation/observed_baseline/*`)
- 15..34: validation baseline stream（`/validation/eval/baseline/{half_time_relaxed|time_relaxed}/{approximate|strict}/{group}/{status}/WM/tracked_object_set_with_prediction`）
- 35..54: validation test stream（`/validation/eval/test/{half_time_relaxed|time_relaxed}/{approximate|strict}/{group}/{status}/WM/tracked_object_set_with_prediction`）
  - `all` mode は追加bagを持たず、`along/opposite/crossing/other` の4groupを viewer 内で合成して描画する。
- 55: collision baseline hard（`/validation/collision/baseline/hard/*`）
- 56: collision test hard（`/validation/collision/test/hard/*`）
- 57: collision baseline soft（`/validation/collision/baseline/soft/*`）
- 58: collision test soft（`/validation/collision/test/soft/*`）
- 59: compare 表示（baseline/test を同一画面で左右比較）
- modeごとの不足データは他modeへフォールバックしない。不足stampは破棄して次stampへ進む。

## 10.3 各modeの描画仕様

### 0 Baseline
- baseline フレームをそのまま描画（通常描画）。

### 1 Test
- test フレームをそのまま描画（通常描画）。

### 2 Diff overlay
- `common` を低不透明度描画（全体 alpha 0.35、lane alpha 0.14）。
- `diff_baseline` を白系で重畳。
- `diff_test` を高め alpha (0.78) で重畳。
- `base` にのみ存在する予測対象外IDも、他属性と同等の context cube として描画する。
- 信号円は左右分割表示（左=baseline側、右=test側、片側欠損時はcommonから補完）。

### 3/4/5 Diff * lane
- レーン差分:
  - baseline-only lane（diff_baseline）を赤
  - test-only lane（diff_test）を緑
- context 物体:
  - `common + diff_baseline + diff_test` を統合し、cube box を白
  - `base` にのみ存在する予測対象外IDも、他属性と同等の context cube として表示する
- mode4/5 追加:
  - VSL差分 cube を強調
    - baseline-only: deep crimson
    - test-only: deep green
- 信号円は左右分割（left=diff_baseline, right=diff_test）。

### 6/7/8/9 Diff * path
- 対象group（along/opposite/crossing/other）で:
  - baseline側差分 path を赤
  - test側差分 path を緑
- context:
  - 同groupで差分のない物体は白
  - 非対象groupは低alpha白（0.15）
  - `base` に存在し対象groupに含まれないID（予測対象外IDを含む）も同じ非対象contextとして扱う
  - test側差分物体は緑（cube/補助マーカ）
- レーンは描画しない（path系抽出表示）。
- 信号円は左右分割（left=diff_baseline, right=diff_test）。

### 10/11/12/13 Diff * path with observed
- 6/7/8/9 の描画仕様に加えて、observed path を重畳する。
- observed path 色・不透明度は validation の `observed_ok` path と同一（灰白、alpha 0.62）。
- observed path は対象groupの予測対象IDに対応する軌道を表示する。

### 14 Observed
- observed baseline の base を描画に使用（group分類は持たない）。
- 白系（alpha 0.78）で object cube + trajectory visual（tapered/volume/step系）を表示。
- lane描画なし。

### 15..34, 35..54 Validation observed vs prediction
- GUI の validation mode 並びは `all -> along -> opposite -> crossing -> other`（各項目で baseline/test ペア）。
- `half-time-relaxed` / `time-relaxed` を side(baseline/test)・threshold(approximate/strict)・group(all/along/opposite/crossing/other)ごとに表示する。
- path色:
  - observed_ok: 白（通常alpha）
  - observed_ng: 赤（通常alpha）
  - optimal: 緑（通常alpha）
  - ignore: 白（低alpha）
  - fail: 黄（通常alpha）
- cube色:
  - `observed_ok` は緑
  - `observed_ng` は赤
  - `optimal/fail/ignore` による cube 色上書きは行わない
- 対象外contextの cube は低alpha白で描画する。
  - 対象外contextには、`base` に存在し対象groupに含まれないID（予測対象外IDを含む）も含める。
  - `other` group では「対象group=予測対象other_object_set」のため、`base - other_target_ids` が予測対象外IDとなる。
  - `all` group では `along/opposite/crossing/other` の予測対象ID和集合を対象groupとし、`base` との差分を予測対象外contextとして描画する。
- lane描画:
  - `along / opposite / crossing` mode では、該当属性の lane polygon を通常色（along=白, opposite=赤, crossing=青）で低alpha表示する（diff overlay の base に準拠）。
  - `other` mode では lane polygon は描画しない。
  - `all` mode では `along/opposite/crossing` の lane polygon を低alphaで同時表示する。

### 55/56/57/58 Collision judgement
- 判定種別:
  - 55/56: hard collision judgement
  - 57/58: soft collision judgement
- 予測path描画:
  - `pred_collision` と `ego_pred_collision`: 通常配色（baseline/test 相当）
  - `pred_safe` と `ego_pred_safe`: validation の `ignore` 相当（白・低alpha）
  - `pred_safe` は path に紐づく cube box / key point も ignore 相当（白・低alpha）
- 自車 observed path:
  - `ego_observed_safe`: 緑
  - `ego_observed_collision`: 赤
- lane polygon は baseline/test mode と同じ仕様（通常色）で描画する。

### 59 Compare
- 4分割表示は維持し、左半分を baseline 系、右半分を test 系として描画する。
- 上段は従来4分割の左下相当、下段は従来4分割の右下相当を左右比較で表示する。
- 視点操作は compare モード時のみ左右連動:
  - 上段は左右同期（どちらをドラッグしても反対側へ反映）
  - 下段は左右同期（どちらをドラッグしても反対側へ反映）
  - 上段と下段の相互同期はしない

---

## 11. observed予測妥当性評価（validation）
- 実装: `scripts/evaluate_observed_validation.py`
- baseline側は `run_baseline.py` 実行中に生成し、test側は `run_test.py` 実行中に生成する。
- sideごとの集計JSON:
  - baseline: `baseline_results/<rel>/validation_baseline_summary.json`
  - test: `test_results/<rel>/validation_test_summary.json`

## 11.1 評価対象
- baseline側: `baseline_results/<rel>/result_baseline.bag` と `baseline_results/<rel>/observed_baseline.bag`
- test側: `test_results/<rel>/result_test.bag` と `test_results/<rel>/observed_test.bag`
- 評価は side ごとに独立に実行する。
- `other` group の評価対象object_idは、`result`側 `other_object_set_with_prediction` で予測path（`trajectory_set`）を持つものに限定する。
  - `observed`側に存在しても、上記を満たさないIDは `other`評価から除外する。
- `VSL-like` 物体は validation 評価対象から除外する。
  - 判定は compare と同一軸（`object_id in {1,500001} or >=500100`、または stopline 形状、または `debug_info` の `vsl/stopline`）を使う。
- observed軌道の有効長（先頭からの最終 `t`）が 3.0 秒未満の物体は、`half-time-relaxed` / `time-relaxed` の `approximate` / `strict` すべて評価対象外とする。
  - ただし bag 出力では、該当 observed 軌道は `observed_ok`、その物体IDに紐づく予測 path は `ignore` として保存する（viewer 描画も同フラグ扱い）。

## 11.2 閾値
- 前後左右の基準軸は observed path の曲線座標系（Frenet の `s/d`）を使う。
  - observed path（評価区間内）の `x,y` から中心線を作り、各 observed step の `s_obs`（累積距離）を定義する。
  - 各時刻で対応づけた予測点 `pred(x,y)` は、observed 中心線へ射影して `(s_pred, d_pred)` を求める。
  - 判定誤差は `longitudinal = s_pred - s_obs`、`lateral = d_pred` とする。
  - すなわち「許容範囲を世界座標へ変換する」のではなく、「予測点を observed の曲線座標へ変換して評価する」方式を採用する。
  - observed path が退化（有効線分がない）している場合のみ、従来どおり速度ベクトル方向（低速時は yaw）をフォールバック軸として使う。
- 許容領域は、各stepの基準軸（前後: longitudinal / 左右: lateral）に対して、中心を observed 位置に置いた長方形とする。
  - `|longitudinal| <= longitudinal_max`
  - `|lateral| <= lateral_max`
  - ここで `longitudinal_max` / `lateral_max` は「前後左右それぞれ片側」の許容値。
- クラス別パラメータ（`TrackedObject2.classification`）:
  - `car / truck / bus`
    - time-relaxed:
      - `t=0`: `longitudinal_max = 1.0m`, `lateral_max = 0.5m`
      - `t=T_end`: `longitudinal_max = 7.5m`, `lateral_max = 2.0m`
  - `cyclist / bike`（msg上は `cyclist / motorcyclist`）
    - time-relaxed:
      - `t=0`: `longitudinal_max = 0.5m`, `lateral_max = 0.35m`
      - `t=T_end`: `longitudinal_max = 4.0m`, `lateral_max = 1.5m`
  - `pedestrian`
    - time-relaxed:
      - `t=0`: `longitudinal_max = 0.5m`, `lateral_max = 0.3m`
      - `t=T_end`: `longitudinal_max = 0.8m`, `lateral_max = 0.5m`
- `T_end` は通常groupで 10.0 秒、other で 3.0 秒。
- `0 <= t <= T_end` の各stepで `longitudinal_max / lateral_max` を線形補間する（time-relaxed系共通）。
- 評価区間:
  - half-time-relaxed: 通常groupは先頭 5.0 秒、other は先頭 1.5 秒
  - time-relaxed: 通常groupは先頭 10.0 秒、other は先頭 3.0 秒
- 判定基準:
  - `strict`: 評価対象の全stepで time-relaxed 許容範囲内であること。
  - `approximate`: 評価対象stepのうち 70%以上が time-relaxed 許容範囲内であり、かつ残りstepも `1.5x` の拡張許容範囲内であること。

## 11.3 判定手順（object_id単位）
- groupごと（along/opposite/crossing/other）に observed 軌跡と予測 path を比較し、全評価stepが閾値内の path を候補とする。
- 候補 path には二次差分コストを与える。`object_id` ごとの `optimal` 選択は group 優先順位 `along > opposite > crossing > other` を先に適用し、同優先順位内でのみ最小コストを採用する。
- `optimal` が存在する object_id:
  - 対応する observed 軌跡（複数groupに存在する場合は全group）を `observed_ok`
  - 非選択の予測 path（同idの全group）を `ignore`
- `optimal` が存在しない object_id:
  - 対応する observed 軌跡（複数groupに存在する場合は全group）を `observed_ng`
  - 同idの予測 path（全group）を `fail`

## 11.4 出力bag
- sideごとに 1 bag へ集約して出力する。
- ファイル名:
  - baseline: `validation_baseline.bag`
  - test: `validation_test.bag`
- bag 内 topic:
  - `/validation/eval/<side>/<horizon>/<threshold>/<group>/<status>/WM/tracked_object_set_with_prediction`
  - topic の `<horizon>` は `half_time_relaxed` / `time_relaxed`。`<threshold>` は `approximate` または `strict`。
  - `<status>` は `optimal / ignore / fail / observed_ok / observed_ng`。

## 11.5 success rate
- object_id を frame単位で集計し、`OK率 = ok_count / total_count` を算出する。
- `half-time-relaxed` / `time-relaxed` × `approximate` / `strict`（baseline/test）の8指標を `comparison.json` の `validation` キーに保存する:
  - `validation.baseline.half-time-relaxed.approximate.{ok,total,rate,detail}`
  - `validation.baseline.half-time-relaxed.strict.{ok,total,rate,detail}`
  - `validation.baseline.time-relaxed.approximate.{ok,total,rate,detail}`
  - `validation.baseline.time-relaxed.strict.{ok,total,rate,detail}`
  - `validation.test.half-time-relaxed.approximate.{ok,total,rate,detail}`
  - `validation.test.half-time-relaxed.strict.{ok,total,rate,detail}`
  - `validation.test.time-relaxed.approximate.{ok,total,rate,detail}`
  - `validation.test.time-relaxed.strict.{ok,total,rate,detail}`
- `comparison.json` への取り込みは `aggregate_comparison_results.py` が行う。
- viewer GUI では Diff matrix の上に validation summary 表を表示する。
- 行は `DT status`, `DT mean` を先頭に置き、その後に common success rate / collision / success rate を表示する。
- `validation common` は `ID+stamp` 共通サンプルで再集約した値を使う。
- 各セルの表示形式:
  - success rate: `70.0% (200/300)`
  - collision: `Collision (N)` または `Safe`
- collision 行は `Collision (N)` / `Safe` を表示する。
  - 1件以上 collision があれば `Collision (N)`（赤）
  - collision が0件なら `Safe`（緑）
- success rate 行（validation 4行 + common 4行）は Baseline/Test を比較し:
  - 高い方を緑文字
  - 低い方を赤文字
  - 同値なら両方黒文字
- `DT mean` は Baseline/Test が両方ある場合のみ大小比較色を付ける。
- `DT` 以外の行はトグル展開可能:
  - validation 行の展開: `four_wheel / two_wheel / pedestrian` の内訳
  - collision 行の展開: `along / crossing / opposite` の内訳
- collision common は評価軸として表示しない（JSON には保持するが summary 行には出さない）。

## 11.6 collision評価（hard / soft）
- 実装: `scripts/evaluate_observed_validation.py`（validation評価と同時に生成）
- 出力:
  - `baseline_results/<rel>/collision_judgement_baseline.bag`
  - `test_results/<rel>/collision_judgement_test.bag`
- 評価対象:
  - 予測pathのうち、自車予測path（ego）と VSL を除外した path を衝突判定対象とする。
  - 自車 observed path は `observed_*` bag から取得する。
  - VSL は collision 判定は行わないが、`pred_safe`（no-collision）側トピックとして出力する。
- コライダー定義（path曲線座標 `s/d` 基準）:
  - `hard`:
    - 長方形: `length = object.length * 1.0`, `width = object.width * 1.0`
    - 進行方向前方に半楕円: 長軸長 `= speed`, 短軸長 `= object.width * 1.0`
  - `soft`:
    - 長方形: `length = object.length * 1.1`, `width = object.width * 1.1`
    - 進行方向前方に半楕円: 長軸長 `= speed * 2.0`, 短軸長 `= object.width * 1.1`
  - `middle`（ego 用）:
    - 長方形: `length = object.length * 1.05`, `width = object.width * 1.05`
    - 進行方向前方に半楕円: 長軸長 `= speed * 1.5`, 短軸長 `= object.width * 1.05`
- 各 step の基準点を path の累積距離 `s_i` とし、輪郭をローカル座標 `(ds, d)` で定義する。
  - `ds`: path 接線方向（s方向）
  - `d`: path 法線方向（d方向）
  - 世界座標への変換は `s_i + ds` の中心線点を補間し、その法線方向へ `d` オフセットする。
- `speed` は path の各 step が持つ速度値（`trajectory[].velocity`）を使用する。
  - 速度値が無い場合のみ、隣接 step 間の `x,y,t` 差分から推定した速度を使う。
- 速度が `3.6/3.6` m/s 以下の step では、半楕円成分は追加せず長方形成分のみをコライダーとする。
- 接線/法線は path 幾何から求める。path が退化（有効な線分がない）している場合のみ、stepの向きは従来どおり `yaw`/速度差分フォールバックを使う。
- 判定ルール:
  - `hard-collision judgement`: `ego middle` と `other hard` の重なり判定
  - `soft-collision judgement`: `ego middle` と `other soft` の重なり判定
  - 同一 timestep 同士（近傍時刻マッチ）で重なりが1回でもあればその path を collision とする。
- `collision_judgement_<side>.bag` の topic:
  - grouped（along/oncoming/crossing/other）:
    - `/validation/collision/<side>/<kind>/pred_collision/WM/<group>_object_set_with_prediction`
    - `/validation/collision/<side>/<kind>/pred_safe/WM/<group>_object_set_with_prediction`
    - `/validation/collision/<side>/<kind>/ego_pred_collision/WM/<group>_object_set_with_prediction`
    - `/validation/collision/<side>/<kind>/ego_pred_safe/WM/<group>_object_set_with_prediction`
  - base:
    - `/validation/collision/<side>/<kind>/ego_observed_collision/WM/tracked_object_set_with_prediction`
    - `/validation/collision/<side>/<kind>/ego_observed_safe/WM/tracked_object_set_with_prediction`
- summary JSON:
  - `validation_*_summary.json` に `collision.{hard,soft}` を保存する。
  - `collision.<kind>.by_group.<along|opposite|crossing>.{has_collision,collision_paths,checked_paths,detail}` を保存する。
  - common再集約用に `per_object_collision` も保存する。
  - `aggregate_comparison_results.py` が `comparison.json` の `collision.baseline.*` / `collision.test.*` へ集約する。

---

## 12. dt / segfault の仕様

## 12.1 dt収集
- `trajectory_predictor_sim_offline --runtime-file <path>` で scene ごとの dt(ms) を直接ファイル出力する。
- `TP_tester` はこの runtime ファイルを読み取り、stdout/stderr の `dt=...` 文字列抽出は行わない。
- sceneごとに:
  - `invalid`: `dt >= 100` が1回でもある
  - `warning`: 上記がなく `dt >= 70` が1回でもある
  - `valid`: それ以外
- `dt_max`: scene内最大値（未取得時 `-`）。
- `dt_mean`: scene内平均値（未取得時 `-`）。
- `sample_count`: scene内サンプル数。
- `dt_summary.json` を scene ごとに保存し、subscene 単位の `dt_status/dt_max/dt_mean/sample_count` も格納する。
  - runtime と `/clock` の件数不一致時は `mapping.status=clock_count_mismatch` として subscene 割り当てを行わない。
  - 一致時は `mapping.status=clock_aligned` で subscene に分配する。

## 12.2 segfault
- scene実行の終了コードが 0 以外なら `segfault` ファイルを作成。
- compare で `segfault_baseline/test` として取り込む。

---

## 13. 既知の注意点
1. 比較ロジックは重複stampを「後勝ち」で潰すため、重複stamp挙動は結果へ影響する。
2. `overall_ok` は lane/vsl/object/path/traffic のみ。segfault/dt は別軸診断。

---

## 14. 変更時のルール（推奨）
1. 比較仕様（Changed/Unchanged判定）を変える場合は、`compare_baseline_test.py` と本 `SPEC.md` を同時更新する。
2. observed validation仕様を変える場合は、`evaluate_observed_validation.py` と本 `SPEC.md` を同時更新する。
3. `comparison.json` の集約仕様を変える場合は、`aggregate_comparison_results.py` と `viewer_app.py` と本 `SPEC.md` を同時更新する。
4. 描画モード仕様を変える場合は、`TrajectoryPredictorViewer_validation.cpp` と `viewer_app.py` の mode ラベルを同期する。
5. bagフォーマット（topic名・namespace・stamp基準）を変える場合は、baseline/test/compare/viewer を一式で整合させる。
6. buildは行わない。
