# TP_tester SPEC

## 1. 目的と適用範囲
このドキュメントは、`TP_tester` の現行実装（`scripts/*.py` と `TrajectoryPredictorViewer_validation`）に基づく仕様を定義する。

対象範囲:
- baseline 記録
- observed_baseline.bag / observed_test.bag 生成
- test 記録
- baseline/test 比較評価（Changed/Unchanged 判定）
- `common.bag` / `diff_baseline.bag` / `diff_test.bag` 生成
- GUI ビューアと表示モード（mode 0..16）

注意:
- 本仕様は「実装準拠」。コード変更時は本ファイルを更新すること。

---

## 2. 用語
- `input bag`: `test_bags/<scene>/` 配下の入力 bag 群。
- `scene`: 1回の比較単位（`discover_bag_directories` が返す `rel`）。
- `record_topics`: `settings.json` の9トピック。
- `common stamp`: baseline/test の参照 WM トピック（`/WM/tracked_object_set_with_prediction`）で共通に存在する stamp。
- `valid_frames`: common stamp の数。
- `total_frames`: 原則 input bag の `/clock` 総数（取れない場合はフォールバック）。

---

## 3. 設定・パス・トピック

### 3.1 settings.json
- `rosparam.use_sim_time` (default true)
- `rosparam.map_name`
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

### 3.2 名前空間
- baseline 記録: `/validation/baseline`
- test 記録: `/validation/test`
- observed baseline: `/validation/observed_baseline`
- observed test: `/validation/observed_test`
- compare生成 common: `/validation/common`
- compare生成 diff baseline: `/validation/diff_baseline`
- compare生成 diff test: `/validation/diff_test`

---

## 4. 生成物（scene単位）

### 4.1 baseline_results
- `baseline_results/commit_info.json`
- `baseline_results/<rel>/result_baseline.bag`
- `baseline_results/<rel>/observed_baseline.bag`
- `baseline_results/<rel>/dt_status` (`valid|warning|invalid`)
- `baseline_results/<rel>/dt_max` (最大 dt、取れなければ `-`)
- `baseline_results/<rel>/segfault` (trajectory_predictor 異常終了時のみ touch)

### 4.2 test_results
- `test_results/commit_info.json`
- `test_results/<rel>/result_test.bag`
- `test_results/<rel>/observed_test.bag`
- `test_results/<rel>/comparison.json`
- `test_results/<rel>/common.bag`
- `test_results/<rel>/diff_baseline.bag`
- `test_results/<rel>/diff_test.bag`
- `test_results/<rel>/dt_status`, `dt_max`, `segfault`

---

## 5. 実行フロー

## 5.0 trajectory_predictor_sim_node の入力同期仕様（timestamp基準）
対象:
- `projects/nrc_ws/src/nrc_wm/nrc_wm_svcs/src/trajectory_predictor/trajectory_predictor_sim_node.cpp`
- `projects/nrc_ws/src/nrc_wm/nrc_wm_svcs/src/trajectory_predictor/trajectory_predictor_io_sim.cpp`

### 5.0.1 駆動トリガ
- `trajectory_predictor_sim_node` は `/use_sim_time=true` で起動。
- 予測実行（`TP.Exe()`）は `TrackedObjectSet2` 受信を起点にキュー処理される。
- 入力 `TrackedObjectSet2` topic 既定値は `/target_tracker/tracked_object_set2`（paramで変更可）。

### 5.0.2 入力topicの収集方法
- `TrackedObjectSet2` は到着ごとに `sim_pending_tracked_objects2` へ enqueue。
- enqueue 時に「最新値スナップショット」を同梱する topic:
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

### 5.0.3 1フレーム処理時の時刻合わせ
- 1ms timer (`CallBackSimProcessQueue`) が queue 先頭を処理。
- 先頭 `TrackedObjectSet2` の時刻を `t_obj` とし、まず同期待機を行う。
- 同期待機条件（stamped topic 全て）:
  - 各バッファの最新stampが `t_obj + 1e-4` より新しいこと（`HasNewerDataForAllStampedTopics`）。
- 待機上限:
  - 0.5秒を超えたら timeout fallback で処理続行（待機を打ち切る）。
- 実際に適用する入力は `ApplyFromBuffersAtTime(T=t_obj)` で決まる:
  - stamped topic: 各バッファで「`<= t_obj` の最新1件」を採用（`upper_bound(t_obj)` の直前）。
  - 採用後、そのtopicバッファの `<= t_obj` データは破棄。
  - `<= t_obj` データが無いtopicは、その cycle では更新されず前回状態を維持。
  - snapshot topic（road_level_route / traffic_light / ad_state）は enqueue 時点の最新値をそのまま採用（stamp整列しない）。

### 5.0.4 出力timestamp（baseline/testの基準になる値）
- `ProcessTrackedObjectSet2Sim` で `header.stamp = TrackedObjectSet2.header.stamp` を設定。
- その cycle で publish する以下の出力はすべてこの同一 `header` を使う:
  - `/WM/tracked_object_set_with_prediction`
  - `/WM/along_object_set_with_prediction`
  - `/WM/crossing_object_set_with_prediction`
  - `/WM/oncoming_object_set_with_prediction`
  - `/WM/other_object_set_with_prediction`
  - `/viz/su/multi_lane_ids_set`
  - `/viz/su/crossing_lane_ids_set`
  - `/viz/su/opposite_lane_ids_set`
  - `/viz/su/ym0_converted_traffic_light_state`
- したがって baseline/test の 9トピック `header.stamp` は「各 cycle の `TrackedObjectSet2` stamp」に一致する。

## 5.1 baseline 作成（記録アルゴリズム）
対象: `run_baseline.sh` -> `run_baseline_docker.sh` -> `scripts/run_baseline.py`

### 5.1.1 baseline の入力の集め方（どのトピックをどう集めるか）
- sceneごとに入力は `test_bags/<rel>/*.bag` の**全bag同時再生**。
- `TP_tester` 自体は「入力をトピック別に再サンプリング/整列して集める」処理はしない。
- 実際の流れ:
  1. `rosbag record` 開始（出力記録）
  2. `/clock` pre-roll 2秒
  3. `rosbag play --clock` で入力bag群再生
  4. silence検知で記録停止
- 記録対象は `record_topics` の9トピックを baseline 名前空間に remap したもの:
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
- `result_baseline.bag` の各メッセージの `header.stamp` は trajectory_predictor が publish した値そのまま。
- `rosbag record` が付ける bag 時刻 (`t`) は記録時刻（`/use_sim_time` 下では `/clock` 進行に依存）。
- 詳細な入力同期と stamp 決定規則は 5.0 を参照。

### 5.1.3 baseline の出力
- `result_baseline.bag` を sceneごとに生成。
- `dt_status` (`valid|warning|invalid`) / `dt_max` / `segfault` を付帯生成。
- その後に `observed_baseline.bag` を同sceneで生成（5.2）。

## 5.2 observed_baseline.bag / observed_test.bag 作成（baseline/testと別アルゴリズム）
対象:
- `scripts/run_baseline.py::_build_observed_bag_from_input`
- baseline作成時: `scripts/run_baseline.py` から `result_baseline.bag` を参照して `observed_baseline.bag` を生成
- test作成時: `scripts/run_test.py` から `result_test.bag` を参照して `observed_test.bag` を生成

### 5.2.1 observed の入力の集め方（トピック選択）
- 入力bag群から `nrc_msgs/TrackedObjectSet2` 型トピックを1つ選んで使う。
- 優先順:
  1. `settings.observed_source_topic`（存在時）
  2. 固定候補順:
     - `/target_tracker/tracked_object_set2`
     - `/sensor_fusion/tracked_object_set2`
     - `/local_model/tracked_object_set2`
  3. それでも決まらなければ、`TrackedObjectSet2` 型のうちメッセージ数最多トピック
- 選択した source topic は baseline/test それぞれの observed 生成でログ出力される。

### 5.2.2 observed のタイムスタンプ基準とサンプル収集
- source topic の各メッセージを `record` として収集し、以下で管理:
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

### 5.2.6 observed と baseline/test の timestamp 軸の対応
- 5.0 の通り、baseline/test の9トピック `header.stamp` は `TrackedObjectSet2.header.stamp` が基準。
- observed も同じ `TrackedObjectSet2` 系列を anchor にして `header.stamp` をそのまま採用するため、比較時の時刻軸は対応づけ可能。
- 厳密整合のため、`settings.observed_source_topic` は trajectory_predictor 入力 topic（通常 `/target_tracker/tracked_object_set2`）に合わせること。
- 差異は生成アルゴリズムのみ:
  - baseline/test: trajectory_predictor が算出した予測結果を記録。
  - observed: 入力観測から再構成した疑似 trajectory（prediction_mode=0）を生成。

## 5.3 test 作成（記録アルゴリズム）
対象: `run_test.sh` -> `run_test_docker.sh` -> `scripts/run_test.py`

### 5.3.1 test の入力の集め方
- baseline と同様に `test_bags/<rel>/*.bag` を scene単位で全bag同時再生。
- `TP_tester` 側で topic ごとの再整列はしない。
- 記録対象は `record_topics` 9トピックを test 名前空間に remap:
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
- `result_test.bag` も `header.stamp` は trajectory_predictor publish値そのまま（TP_testerで加工しない）。
- bag 時刻 (`t`) は `rosbag record` の記録時刻。
- 詳細な入力同期と stamp 決定規則は 5.0 を参照。

### 5.3.3 test の出力と比較
- sceneごとに `result_test.bag`, `dt_status`, `dt_max`, `segfault` を生成。
- 各sceneで `result_test.bag` 作成成功後に `observed_test.bag` を生成（5.2）。
- 全scene後に `compare_baseline_test.py` を実行し、`comparison.json` と `common/diff_*.bag` を生成。
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
  1. input bag群の `/clock` 総数
  2. baseline/test 参照トピックのメッセージ数 max
  3. stamp 和集合サイズ

## 6.5 追加診断情報
- `segfault_baseline/test`: `segfault` ファイル有無。
- `dt_status_baseline/test`: `valid|warning|invalid`。
- `dt_max_baseline/test`: 最大 dt 値（文字列）。

---

## 7. `comparison.json` 構造（主要キー）
- 基本:
  - `directory`
  - `lane_ids_ok`, `vsl_ok`, `object_ids_ok`, `path_ok`, `traffic_ok`, `path_and_traffic_ok`, `overall_ok`
  - `lane_ids_detail`, `vsl_detail`, `object_ids_detail`, `path_and_traffic_detail`
- フレーム統計:
  - `record_frames_baseline`, `record_frames_test`, `valid_frames`, `total_frames`
  - `record_stamps_only_baseline_count`, `record_stamps_only_test_count`
  - `record_stamps_only_baseline`, `record_stamps_only_test`（先頭100件）
  - `record_baseline_topic_counts`, `record_test_topic_counts`
- source別差分:
  - `diff_by_source`
  - `diff_counts_by_source`
- commit/実行診断:
  - `baseline_commit`, `baseline_describe`, `test_commit`, `test_describe`
  - `segfault_baseline`, `segfault_test`
  - `dt_status_baseline`, `dt_status_test`, `dt_max_baseline`, `dt_max_test`

---

## 8. common/diff bag 生成仕様
`compare_baseline_test.py::_write_diff_bags_impl`。
- 比較・差分判定はこの生成段階で完結させる。
- Viewer側（mode 2..9）は `common/diff_*.bag` に保存済みの結果を選択描画するだけで、baseline/test の再差分計算は行わない。

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
- 列: `Directory`, `Valid/Total`, `Lane IDs`, `VSL`, `Object IDs`, `Path`, `Traffic`, `Seg fault`, `DT status`。
- 先頭列に要約マーカー `■`:
  - 全カテゴリOK -> 緑
  - どれかNG -> 赤
- ステータス文字色:
  - `Unchanged` 緑
  - `Changed` 赤
  - `No` 緑, `Yes` 赤
  - `Valid` 緑, `Warning` 橙, `Invalid` 赤
- 選択ハイライト中でも文字色は保持（delegateで HighlightedText を上書き）。

## 9.2 Diff matrix（右上）
- 行: Lane IDs / Object IDs / Path / VSL
- 列: along / opposite / crossing / other / base
- `base` は `tracked_object_set_with_prediction`（group分割前の全体集合）を意味する。
- ただし VSL 行は `along / opposite / crossing` のみ対象（`other` と `base` は `-`）。
- 値は `comparison.json.diff_by_source` と `diff_counts_by_source` 由来。

## 9.3 Detail（右中）
- 常時表示:
  - `Baseline commit: ...`
  - `Test commit: ...`
- 選択sceneの summary と bag パスを表形式で表示。

## 9.4 Viewer 起動
- `rosrun <viewer_pkg> <viewer_validation_node> -m` を実行。
- 引数 remap:
  - baseline topics: `_xxx_topic:=/validation/baseline/...`
  - test topics: `_test_xxx_topic:=/validation/test/...`
  - observed baseline WM topics: `_observed_xxx_topic:=/validation/observed_baseline/...`
  - observed test WM topics: `_observed_test_xxx_topic:=/validation/observed_test/...`
- `/validation/display_mode` に Int32 publish してモード切替。
- `/validation/clear_buffers` を再生前後に publish。

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
- 10: Observed
- 11: Compare obs-baseline along
- 12: Compare obs-test along
- 13: Compare obs-baseline opposite
- 14: Compare obs-test opposite
- 15: Compare obs-baseline crossing
- 16: Compare obs-test crossing

## 10.2 mode別の入力ソース
- 0: baseline stream (`/validation/baseline/*`)
- 1: test stream (`/validation/test/*`)
- 2: `common + diff_baseline + diff_test`
- 3..9: `common + diff_baseline + diff_test`（再差分計算なし、bag内容を直接描画）
- 10: observed baseline stream (`/validation/observed_baseline/*`)
- 11/13/15: observed baseline + baseline（対象group）
- 12/14/16: observed test + test（対象group）
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
- 信号円は左右分割表示（左=baseline側、右=test側、片側欠損時はcommonから補完）。

### 3/4/5 Diff * lane
- レーン差分:
  - baseline-only lane（diff_baseline）を赤
  - test-only lane（diff_test）を緑
- context 物体:
  - `common + diff_baseline + diff_test` を統合し、cube box を白
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
  - test側差分物体は緑（cube/補助マーカ）
- レーンは描画しない（path系抽出表示）。
- 信号円は左右分割（left=diff_baseline, right=diff_test）。

### 10 Observed
- observed baseline の base を描画に使用（group分類は持たない）。
- 白系（alpha 0.78）で object cube + trajectory visual（tapered/volume/step系）を表示。
- lane描画なし。

### 11..16 Compare observed vs baseline/test
- groupは mode に応じて along/opposite/crossing。
- observed側:
  - 対象group object の軌跡 visual を白（alpha 0.78）
  - 非対象は cube のみ白（alpha 0.30）
- 比較側:
  - mode 11/13/15 は baseline の対象group軌跡 visual を白（alpha 0.30）
  - mode 12/14/16 は test の対象group軌跡 visual を白（alpha 0.30）
- lane描画なし。

---

## 11. dt / segfault の仕様

## 11.1 dt収集
- trajectory_predictor の stdout から `dt=...` を正規表現で抽出。
- sceneごとに:
  - `invalid`: `dt >= 100` が1回でもある
  - `warning`: 上記がなく `dt >= 70` が1回でもある
  - `valid`: それ以外
- `dt_max`: scene内最大値（未取得時 `-`）。

## 11.2 segfault
- scene終了時に trajectory_predictor プロセスが死んでいたら `segfault` ファイルを作成。
- compare で `segfault_baseline/test` として取り込む。

---

## 12. 既知の注意点
1. 同一コミットでも runごとに stamp 集合差が出ることがある。
   - 背景: `trajectory_predictor_sim_node` は ObjectSet2 基準同期だが、stamped topic の到着遅延時は 0.5秒で timeout fallback して処理継続する。
   - そのため境界時刻付近では「どの入力が `<= t_obj` 最新として採用されるか」が再生実行ごとに揺れる場合がある。
2. 比較ロジックは重複stampを「後勝ち」で潰すため、重複stamp挙動は結果へ影響する。
3. `overall_ok` は lane/vsl/object/path/traffic のみ。segfault/dt は別軸診断。

---

## 13. 変更時のルール（推奨）
1. 比較仕様（Changed/Unchanged判定）を変える場合は、`compare_baseline_test.py` と本 `SPEC.md` を同時更新する。
2. 描画モード仕様を変える場合は、`TrajectoryPredictorViewer_validation.cpp` と `viewer_app.py` の mode ラベルを同期する。
3. bagフォーマット（topic名・namespace・stamp基準）を変える場合は、baseline/test/compare/viewer を一式で整合させる。
4. buildは行わない
