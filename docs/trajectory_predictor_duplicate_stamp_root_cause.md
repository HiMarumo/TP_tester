# trajectory_predictor: 重複スタンプの根本原因（切り分け結果・変更なし）

## 現象

- baseline / test ともに **メッセージ数は同じ**（例: 301 メッセージ/トピック）
- **ユニークなスタンプ数**が run ごとに異なる（例: baseline=288, test=291）
- その結果、比較で「only_baseline」「only_test」のスタンプが生じ、diff が飛び飛びになる

## 根本原因（コード上で特定した事実）

### 1. 公開は「タイマー駆動」で固定 10 Hz

- **trajectory_predictor_io.cpp** 142 行目:
  ```cpp
  ros_timer = nh.createTimer(ros::Duration(0.1), &ROSTrajectoryPredictorApp::CallBackTimer, this);
  ```
- **CallBackTimer** が **0.1 秒ごと**に呼ばれ、その中で:
  - `TP.Exe()`
  - `PublishPrediction()` … `tracked_object_set_with_prediction` を `header` で publish
  - `PublishVizInfo2()` … along/crossing/oncoming/other, lane_ids, traffic_light をすべて **同じ `header`** で publish

→ **毎タイマー刻みに、全 WM トピックが同じ `header.stamp` で 1 回ずつ publish される。**

### 2. `header.stamp` の更新は「入力コールバック」のみ

- **header.stamp を書き換えているのは次の 1 箇所だけ:**
  - **trajectory_predictor_io.cpp** 221–227 行目: `CallbackTrackedObjectSet2` 内
    ```cpp
    void ROSTrajectoryPredictorApp::CallbackTrackedObjectSet2(const nrc_msgs::TrackedObjectSet2::ConstPtr &msg)
    {
        if ( b_use_object2 == true )
        {
            objects2_msg = *msg;
            header.stamp    = msg->header.stamp;   // ← ここだけ
            header.frame_id = msg->header.frame_id;
    ```
- 他に `header.stamp` を代入しているのは初期化時の `ros::Time::now()` のみ（86 行目）。WM 出力の stamp として使われるのは上記コールバックで代入された値。

→ **「いま出力に使う stamp」は「最後に届いた tracked_object_set2 の header.stamp」のまま。**

### 3. 結果として起きていること

- タイマーは **wall-time で 10 Hz**（0.1 秒間隔）。
- 入力 `tracked_object_set2` は **bag 再生のシミュ時間**で届く（再生レートやバッファで到着タイミングは 10 Hz と一致しない）。
- したがって:
  - **新しい入力が来るまで**、何度タイマーが fire しても `header.stamp` は変わらない。
  - 同じ stamp で **複数回** publish される（タイマーが 2 回 fire すれば同じ stamp が 2 回出る）。
- メッセージ総数は「タイマーが fire した回数」に一致（例: 301 回）。
- ユニークな stamp 数は「入力が更新された回数」に一致（例: 288 や 291）。
- baseline と test は **別プロセス・別実行**のため、タイマーと入力の**位相**が少しずれる → どちらの stamp が「重複側」「欠ける側」になるかが run ごとに変わり、**only_baseline / only_test の差**が出る。

## 結論（原因の切り分け）

| 項目 | 内容 |
|------|------|
| **原因** | **タイマーは 10 Hz で publish するが、header.stamp は tracked_object_set2 のコールバックでしか更新されない**ため、同じ stamp が複数回 publish される。 |
| **場所** | `nrc_wm_svcs/src/trajectory_predictor/trajectory_predictor_io.cpp`: タイマー周期(142)、CallBackTimer(612)、header.stamp 更新(226)、PublishPrediction/PublishVizInfo2(765, 2224) |
| **記録側** | 記録は 301 メッセージを欠けず取得できている。重複は TP の publish 仕様による。 |
| **非決定的に見える理由** | タイマー（wall-time 10 Hz）と bag 入力の到着タイミングの位相が run ごとに変わるため、どの stamp が何回重複するかが run ごとに変わり、baseline と test で「ユニークな stamp の集合」が一致しない。 |

修正する場合は、「1 論理フレームあたり 1 回だけ同じ stamp で publish する」ようにする（例: 入力が更新されたときだけ publish する、またはタイマーで publish するならそのときの stamp を入力に紐づけて一意にする）必要がある。本ドキュメントは原因の切り分けのみで、コード変更は行っていない。
