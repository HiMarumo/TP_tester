# TP_tester – trajectory_predictor 検証ツール

trajectory_predictor のベースラインとテスト結果を記録・比較するためのツールです。**Docker でビルドし、以下の手順で実行する**ことを想定しています。

---

## 事前準備（Docker を使う場合）

Docker と docker-compose を apt でインストールしてください。

```bash
sudo apt update
sudo apt install docker.io docker-compose
```

**apt の docker-compose (1.25) では、環境によって Python/urllib3 の不整合でエラーになることがあります。** その場合は `docker-compose` を直接使わず、リポジトリのラッパースクリプトを使うと、`PYTHONNOUSERSITE=1` を毎回付けずに済みます（bashrc に書く必要はありません）。

- **build.sh** … イメージのビルド
- **compose.sh** … `docker-compose run` などにそのまま渡す（例: `./compose.sh run --rm tp_tester ./run_baseline.sh`）

※ `docker compose`（ハイフンなしのプラグイン）が使える環境では、以下で `docker-compose` の代わりに `docker compose` を実行すればラッパーは不要です。

---

## 実行手順（Docker）

次の順番で実行します。`docker-compose` でエラーになる場合は **./build.sh** と **./compose.sh** を使ってください。

### 1. Docker イメージをビルドする

```bash
cd ~/TP_tester
./build.sh
```

または（`docker compose` が使える場合）:

```bash
docker compose build
```

- 初回や Dockerfile を変更したときに実行します。
- `~/TP_tester` と `~/projects` はコンテナ内にマウントされます（apt 版 Docker を推奨）。

**ホストで `./run_baseline.sh` / `./run_test.sh` を実行すると**、まずホストの nrc_ws 上で `catkin_make --pkg nrc_wm_svcs` を実行してから Docker で baseline/test を流します（参照 commit と実行バイナリの一致を図る）。Docker 内では `TP_SKIP_BUILD=1` のためビルドはスキップされ、直前にホストでビルドした結果が使われます。

### 2. ベースラインを作成する

```bash
cd ~/TP_tester
./run_baseline.sh
```

（中でホストの nrc_ws をビルドしてから `./compose.sh run --rm tp_tester ./run_baseline_docker.sh` を実行します。）

**ホストで trajectory_predictor まで実行して検証したい場合**（推奨）:  
`TP_USE_DOCKER=0 ./run_baseline.sh` および `TP_USE_DOCKER=0 ./run_test.sh` で、**trajectory_predictor を含む処理をすべてホストで実行**できます。実際の機能検証にはホスト環境の方が向いています。表示用ビューアだけ `./compose.sh run ... ./run_viewer.sh` で Docker を使う運用も可能です。

- **中で行うこと**: ホストで nrc_ws の `catkin_make --pkg nrc_wm_svcs` → Docker 内（または TP_USE_DOCKER=0 のときはホストで）`run_baseline_docker.sh`（test_bags 再生 ＋ trajectory_predictor ＋ 記録）→ 出力は **baseline_results/<日付>/<時刻>/result_baseline.bag**。
- `baseline_results/` は毎回削除してから作り直します。
- 使用した commit は **baseline_results/commit_info.json** に保存されます。

### 3. テストを実行して比較する

```bash
cd ~/TP_tester
./run_test.sh
```

- **中で行うこと**: ホストでビルド → Docker 内（または TP_USE_DOCKER=0 のときはホストで）`run_test_docker.sh`（trajectory_predictor ＋ 記録）→ **test_results/<日付>/<時刻>/result_test.bag** に記録 → 全ディレクトリ分の記録後に比較し **comparison.json** を出力。
- 使用した commit は **test_results/commit_info.json** に保存されます。

### 4. 検証結果を確認する（ビューア）

```bash
cd ~/TP_tester
./compose.sh run --rm -e DISPLAY=$DISPLAY -v /tmp/.X11-unix:/tmp/.X11-unix tp_tester ./run_viewer.sh
```

（`docker compose` が使える場合は `docker compose run --rm -e DISPLAY=$DISPLAY -v /tmp/.X11-unix:/tmp/.X11-unix tp_tester ./run_viewer.sh`）

- GUI が開きます。**左**にデータ一覧（Directory / Lane IDs / VSL / Object IDs / Path / Traffic の Unchanged/Changed）、**右**に可視化エリア（Viewer）＋詳細＋再生用ボタン。
- 「Launch で可視化を開始」で TrajectoryPredictorViewer_validation を起動し、**右側のパネルに埋め込んで表示**します。「Play」で選択した bag を再生すると、同じ右側の Viewer で可視化されます。
- 右側への埋め込みには **xdotool** が必要です（`sudo apt install xdotool`）。無い場合は Viewer は別ウィンドウで開きます。
- モードで Baseline / Test / 差分強調 / 差分表示 を切り替えられます。

---

## ディレクトリと設定

| 役割 | 説明 |
|------|------|
| **test_bags/** | 入力 bag。次のいずれかの構造で配置。同一ディレクトリ内の複数 bag は同時再生される。<br>・**2階層**: `<日付>/<時刻>/*.bag`（例: `2026-03-09/11-12-34/*.bag`）<br>・**1階層**: `<名前>/*.bag`（例: `C28-06_OR_20250101-120000/*.bag`。`C28-06` 部分は可変） |
| **settings.json** | `paths.nrc_ws_devel`（nrc_ws の devel/setup.bash）、`offline.map_name`（`trajectory_predictor_sim_offline` に `--map-name` で渡す）、`record_topics`、ノード名などを指定。 |
| **baseline_results/** | ベースライン記録。`<日付>/<時刻>/result_baseline.bag` とルートに `commit_info.json`。 |
| **test_results/** | テスト記録と比較結果。`<日付>/<時刻>/result_test.bag` と `comparison.json`、ルートに `commit_info.json`。 |

---

## 比較項目（comparison.json）

1. **Lane IDs** – multi/crossing/opposite_lane_ids_set の一致  
2. **VSL** – ID 500001, 500100+, 500200+ の位置・向きの一致  
3. **Object IDs** – path が存在する object_id 集合の一致  
4. **Path** – 軌跡（object trajectory）の一致  
5. **Traffic** – traffic_light_state の一致  

各 `<日付>/<時刻>/comparison.json` には上記の Unchanged/Changed と、baseline/test の commit 情報（記録されている場合）が含まれます。**commit が unknown になる場合**は、nrc_ws または `src/nrc_wm_svcs` が git リポジトリでないか、Docker 内で git が動いていない可能性がある。手動で記録するには実行前に `TP_BASELINE_COMMIT=<hash>` または `TP_TEST_COMMIT=<hash>` を設定する。

---

## Docker を使わない場合（ホストで実行）

`~/projects/nrc_ws` をビルドし、`devel/setup.bash` を source したうえで:

```bash
cd ~/TP_tester
source /home/nissan/projects/nrc_ws/devel/setup.bash   # または NRC_WS_DEVEL を設定

./run_baseline.sh   # ベースライン作成
./run_test.sh       # テスト実行 + 比較
./run_viewer.sh     # 検証結果ビューア
```

`run_baseline.sh` / `run_test.sh`（offline実行）は ROS master 不要です。`run_viewer.sh` は ROS master が必要です。

---

## ビューアの補足

- **TrajectoryPredictorViewer_validation**（nrc_svcs）を使用。モード 0=Baseline, 1=Test, 2=差分強調, 3=差分表示。
- **記録トピックの名前空間**: baseline は `/validation/baseline/...`、test は `/validation/test/...` で記録するため、差分モードで同時再生しても競合しません。
- Play 時は test_bags から record_topics を除外した一時 bag を作成して再生するため、test_bags に含まれる取得結果と result bag の競合もありません。

### TrajectoryPredictorViewer_validation の購読トピック（nrc_svcs 側の対応）

**受け取り側でトピック名を名前空間付きに変更する必要があります。** TP_tester の result bag にはもはや `/WM/...` や `/viz/su/...` は出ておらず、次のトピックだけが記録されています。

| モード | 購読するトピック |
|--------|------------------|
| 0 (Baseline) | `/validation/baseline/WM/along_object_set_with_prediction` 他、`/validation/baseline/viz/su/multi_lane_ids_set` 他（`settings.json` の `record_topics` の先頭に `/validation/baseline` を付けたもの） |
| 1 (Test) | 上と同様に `/validation/test` を付けたトピック |
| 2, 3 (差分) | baseline 用と test 用の**両方**（`/validation/baseline/*` と `/validation/test/*`） |

- 表示モードは既存どおり `/validation/display_mode`（`std_msgs/Int32`）で 0/1/2/3 を受け取ればよい。
- 元のトピック一覧は `settings.json` の `record_topics` と同一。名前空間は `common.py` の `VALIDATION_BASELINE_NS` / `VALIDATION_TEST_NS` で定義。
