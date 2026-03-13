# TP_tester SPEC

## 1. 目的と適用範囲
このドキュメントは、`TP_tester` の現行実装（`scripts/*.py` と `TrajectoryPredictorViewer_validation`）に基づく仕様を定義する。

対象範囲:
- baseline 記録
- observed_baseline.bag / observed_test.bag 生成
- test 記録
- baseline/test 比較評価（Changed/Unchanged 判定）
- `common.bag` / `diff_baseline.bag` / `diff_test.bag` 生成
- GUI ビューアと表示モード（validation は `half-time-relaxed-{approximate|strict}` / `time-relaxed-{approximate|strict}` を表示）

注意:
- 本仕様は「実装準拠」。コード変更時は本ファイルを更新すること。

---

## 2. 用語
- `raw input bags`: `test_bags/<scene>/` 配下の入力 bag 群。
- `selected input bags`: scene の raw input bags から、必要topicを含む bag のみ抽出した入力bag集合。
- `scene`: 1回の比較単位（`discover_bag_directories` が返す `rel`）。
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

### 3.3 名前空間
- baseline 記録: `/validation/baseline`
- test 記録: `/validation/test`
- observed baseline: `/validation/observed_baseline`
- observed test: `/validation/observed_test`
- compare生成 common: `/validation/common`
- compare生成 diff baseline: `/validation/diff_baseline`
- compare生成 diff test: `/validation/diff_test`
- validation評価生成: `/validation/eval/<side>/<horizon>/<threshold>/<group>/<status>`

---

## 4. 生成物（scene単位）

### 4.1 baseline_results
- `baseline_results/commit_info.json`
- `baseline_results/<rel>/result_baseline.bag`
- `baseline_results/<rel>/observed_baseline.bag`
- `baseline_results/<rel>/validation_baseline_summary.json`
- `baseline_results/<rel>/<horizon>_<threshold>_<group>_baseline_<status>.bag`
- `baseline_results/<rel>/dt_status` (`valid|warning|invalid`)
- `baseline_results/<rel>/dt_max` (最大 dt、取れなければ `-`)
- `baseline_results/<rel>/segfault` (trajectory_predictor 異常終了時のみ touch)

### 4.2 test_results
- `test_results/commit_info.json`
- `test_results/<rel>/result_test.bag`
- `test_results/<rel>/observed_test.bag`
- `test_results/<rel>/validation_test_summary.json`
- `test_results/<rel>/<horizon>_<threshold>_<group>_test_<status>.bag`
- `test_results/<rel>/comparison_direct.json`
- `test_results/<rel>/comparison.json`
- `test_results/<rel>/common.bag`
- `test_results/<rel>/diff_baseline.bag`
- `test_results/<rel>/diff_test.bag`
- `test_results/<rel>/dt_status`, `dt_max`, `segfault`

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
  2. `--runtime-file` に出力された dt(ms) 一覧を読み取り `dt_status` / `dt_max` を生成
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
- `dt_status` (`valid|warning|invalid`) / `dt_max` / `segfault` を付帯生成。
- その後に `observed_baseline.bag` を同sceneで生成（5.2）。
- さらに `evaluate_observed_validation.py`（baseline side）を実行し、`validation_baseline_summary.json` と `*_baseline_*.bag` を生成（11）。

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
  2. `--runtime-file` に出力された dt(ms) 一覧を読み取り `dt_status` / `dt_max` を生成
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
- sceneごとに `result_test.bag`, `dt_status`, `dt_max`, `segfault` を生成。
- 各sceneで `result_test.bag` 作成成功後に `observed_test.bag` を生成（5.2）。
- 各sceneで `evaluate_observed_validation.py`（test side）を実行し、`validation_test_summary.json` と `*_test_*.bag` を生成。
- 全scene後に `compare_baseline_test.py` を実行し、`comparison_direct.json` と `common/diff_*.bag` を生成。
- その後 `aggregate_comparison_results.py` を実行し、`comparison_direct.json` と baseline/test の validation summary を集約して `comparison.json` を生成。
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
- 対象 object: `object_id == 500001` または `>=500100`。
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

## 6.6 compare 実行時の進捗表示
- scene ごとに比較進捗を表示する（lane + WM + traffic の比較ステップ合計を分母）。
- 表示は 1 行更新（`\r`）で、`percent` が変化した時のみ更新する。
- 欠損メッセージで比較をスキップする場合もステップを消費し、最終的に 100% へ到達する。

---

## 7. 比較/集約 JSON 構造
- `comparison_direct.json`:
  - 生成: `compare_baseline_test.py`
  - 内容: baseline/test の直接比較結果のみ（lane/vsl/object/path/traffic, diff集計, commit, dt, segfault など）
- `comparison.json`:
  - 生成: `aggregate_comparison_results.py`
  - 内容: `comparison_direct.json` に observed validation summary（baseline/test）をマージした最終表示用JSON

### 7.1 `comparison.json` 主要キー
- 基本:
  - `directory`
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
- observed妥当性評価:
  - `validation.baseline.half-time-relaxed.approximate.{ok,total,rate,detail}`
  - `validation.baseline.half-time-relaxed.strict.{ok,total,rate,detail}`
  - `validation.baseline.time-relaxed.approximate.{ok,total,rate,detail}`
  - `validation.baseline.time-relaxed.strict.{ok,total,rate,detail}`
  - `validation.test.half-time-relaxed.approximate.{ok,total,rate,detail}`
  - `validation.test.half-time-relaxed.strict.{ok,total,rate,detail}`
  - `validation.test.time-relaxed.approximate.{ok,total,rate,detail}`
  - `validation.test.time-relaxed.strict.{ok,total,rate,detail}`

---

## 8. common/diff bag 生成仕様
`compare_baseline_test.py::_write_diff_bags_impl`。
- 比較・差分判定はこの生成段階で完結させる。
- Viewer側（mode 2..9）は `common/diff_*.bag` に保存済みの結果を選択描画するだけで、baseline/test の再差分計算は行わない。
- diff/common bag 生成中は進捗バーを表示する（計算 + 書き込みの合計ステップ基準）。

## 8.1 対象stamp
- `common.bag` / `diff_baseline.bag` / `diff_test.bag` は common stamp のみ対象。
- 可能なら `/clock` も common stamp ごとに書く。
- topic単位でその stamp の実メッセージが無い場合は、`header.stamp` を合わせた空メッセージを書き込む
  （WM: `objects=[]`, lane: `arrays=[]`, traffic: `trafficlightlist=[]`）。

## 8.2 `common.bag`
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

## 8.3 `diff_baseline.bag`
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

## 8.4 `diff_test.bag`
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
- 列: `↑/↓/-`, `L`, `O`, `P`, `Directory`, `Valid/Total`, `Lane IDs`, `VSL`, `Object IDs`, `Path`, `Traffic`, `Seg fault`, `DT status`。
- 先頭行に `overall` を追加し、全sceneの `comparison.json` を集約した総合評価を表示する。
- 起動時のデフォルト選択は `overall` 行とする。
- `Play` ボタンは「scene行が選択されている場合のみ」有効化する。未選択または `overall` 選択時は無効（グレーアウト）とする。
- `↑/↓/-` 列のヘッダ文字は空（非表示）とし、`L/O/P` 列のヘッダにはそれぞれ `L`, `O`, `P` を表示する。
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
- 行: Lane IDs / Object IDs / Path / VSL
- 列: along / opposite / crossing / other / base
- `base` は `tracked_object_set_with_prediction`（group分割前の全体集合）を意味する。
- ただし VSL 行は `along / opposite / crossing` のみ対象（`other` と `base` は `-`）。
- 値は `comparison.json.diff_by_source` と `diff_counts_by_source` 由来。
- `overall` 選択時は全sceneの差分を source別に合算した結果を表示する。

## 9.3 Detail（右中）
- 常時表示:
  - `Baseline commit: ...`
  - `Test commit: ...`
- scene選択時: 選択sceneの summary と bag パスを表形式で表示。
- `overall` 選択時: 全scene集約の summary（`comparison.json` 読み込み件数を含む）を表形式で表示する。

## 9.4 Viewer 起動
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
  - `Baseline` / `Test`: 太字
  - 上記は該当語のみを部分装飾し、ラベル全文の単色化は行わない。
- display mode 選択肢の並び（Diff path系）:
  - `Diff along path` → `Diff along path with observed` → `Diff opposite path` → `Diff opposite path with observed` → `Diff crossing path` → `Diff crossing path with observed` → `Diff other path` → `Diff other path with observed`

## 9.5 再生 bag セット
Play時に **常に全bagを同時再生** し、表示は mode で切替する。
再生順:
1. `test_bags/<rel>/*.bag`（入力bag群）
2. `baseline_results/<rel>/result_baseline.bag`
3. `baseline_results/<rel>/observed_baseline.bag`（存在時）
4. `test_results/<rel>/result_test.bag`（存在時）
5. `test_results/<rel>/observed_test.bag`（存在時）
6. `test_results/<rel>/common.bag`（存在時）
7. `test_results/<rel>/diff_baseline.bag`（存在時）
8. `test_results/<rel>/diff_test.bag`（存在時）
9. `baseline_results/<rel>/<horizon>_<threshold>_<group>_baseline_<status>.bag`（存在時）
10. `test_results/<rel>/<horizon>_<threshold>_<group>_test_<status>.bag`（存在時）

`rosbag play --clock --loop` を使用。

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
- VSL関連ID（`500001` および `>=500100`）は validation 評価対象から除外する。
- observed軌道の有効長（先頭からの最終 `t`）が 3.0 秒未満の物体は、`half-time-relaxed` / `time-relaxed` の `approximate` / `strict` すべて評価対象外とする。
  - ただし bag 出力では、該当 observed 軌道は `observed_ok`、その物体IDに紐づく予測 path は `ignore` として保存する（viewer 描画も同フラグ扱い）。

## 11.2 閾値
- 前後左右の基準軸は observed の「速度ベクトル方向」を使う。
  - 速度ベクトルは隣接ステップの位置差分から算出する（中間点は前後差分、端点は片側差分）。
  - 速度が `3.6/3.6` m/s 以下なら observed yaw を基準軸として使う。
- 許容領域は、前方/後方それぞれで「遠い側が長辺」の台形を対向に接続した形状とする。
  - `|longitudinal| <= longitudinal_max` を満たす範囲で、
  - `lateral_max = lateral_near + (lateral_far - lateral_near) * (|longitudinal| / longitudinal_max)` を適用する。
- クラス別パラメータ（`TrackedObject2.classification`）:
  - `car / truck / bus`
    - time-relaxed:
      - `t=0`: `longitudinal_max = 1.0m`, `lateral_near = 0.5m`, `lateral_far = 1.2m`
      - `t=T_end`: `longitudinal_max = 7.5m`, `lateral_near = 2.0m`, `lateral_far = 4.8m`
  - `cyclist / bike`（msg上は `cyclist / motorcyclist`）
    - time-relaxed:
      - `t=0`: `longitudinal_max = 0.5m`, `lateral_near = 0.35m`, `lateral_far = 0.84m`
      - `t=T_end`: `longitudinal_max = 4.0m`, `lateral_near = 1.5m`, `lateral_far = 3.6m`
  - `pedestrian`
    - time-relaxed:
      - `t=0`: `longitudinal_max = 0.5m`, `lateral_near = 0.3m`, `lateral_far = 0.6m`
      - `t=T_end`: `longitudinal_max = 0.8m`, `lateral_near = 0.5m`, `lateral_far = 1.0m`
- `T_end` は通常groupで 10.0 秒、other で 3.0 秒。
- `0 <= t <= T_end` の各stepで `longitudinal_max / lateral_near / lateral_far` を線形補間する（time-relaxed系共通）。
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
- side, horizon, threshold, group, flag ごとに bag を出力する。
- ファイル名:
  - `<horizon>_<threshold>_<group>_<side>_optimal.bag`
  - `<horizon>_<threshold>_<group>_<side>_ignore.bag`
  - `<horizon>_<threshold>_<group>_<side>_fail.bag`
  - `<horizon>_<threshold>_<group>_<side>_observed_ok.bag`
  - `<horizon>_<threshold>_<group>_<side>_observed_ng.bag`
- 各 bag の記録 topic:
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
- 行は `half-time-relaxed approximate`, `half-time-relaxed strict`, `time-relaxed approximate`, `time-relaxed strict` の順、列は `Baseline`, `Test`。
- 各セルの表示形式は `70.0% (200/300)`。
- 各行で Baseline/Test を比較し:
  - 高い方を緑文字
  - 低い方を赤文字
  - 同値なら両方黒文字

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
4. buildは行わない
