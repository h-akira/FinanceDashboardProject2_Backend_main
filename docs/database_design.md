# データベース設計

## 概要

メイン API のデータストアとして DynamoDB を採用する。シングルテーブル設計により、将来的にデータ種別が増加しても 1 つのテーブルで管理する。

### テーブル基本情報

| 項目 | 値 |
|------|-----|
| テーブル名 | `table-fdp-${env}-backend-main` |
| 課金モード | オンデマンド（PAY_PER_REQUEST） |
| 認証方式 | IAM 認証（Lambda 実行ロール） |
| アクセス方式 | boto3 DynamoDB リソース |

---

## キー設計

### キースキーマ

| キー | 属性名 | 型 | 説明 |
|------|--------|-----|------|
| パーティションキー (PK) | `PK` | String | データ系列の識別子 |
| ソートキー (SK) | `SK` | String | 時系列の識別子 |

### キーパターン

| PK パターン | SK パターン | 属性 | 用途 |
|------------|------------|------|------|
| `KIND#<データ系列名>` | `TIME#<YYYY-MM-DD>` | `value` (Number) | 時系列データ（月次） |

- **PK**: データ系列単位で分割する。同じデータを複数のグラフから参照可能にするため、グラフ単位ではなくデータ系列単位とする
- **SK**: 月末日（`YYYY-MM-DD` 形式）。Query の `ScanIndexForward=True` で昇順取得する
- **value**: 数値データ。DynamoDB の Number 型（内部的には Decimal）で格納する

---

## 現在のデータ系列

### 金利データ（US-6.1: 政策金利・長期金利チャート）

| PK | SK 例 | value 例 | データソース |
|----|--------|---------|------------|
| `KIND#target_rate` | `TIME#2024-12-31` | `4.33` | FRED DFEDTAR / DFEDTARU |
| `KIND#dgs10` | `TIME#2024-12-31` | `4.577` | FRED DGS10 |

- `target_rate`: FF 金利誘導目標（上限）。DFEDTAR（2008年以前）と DFEDTARU（2008年以降）を結合したデータ
- `dgs10`: 米国10年国債利回り

---

## アクセスパターン一覧

| # | パターン | 対応 API | クエリ | 方向 |
|---|---------|---------|--------|------|
| 1 | データ系列の全件取得 | `GET /finance/interest-rate` | `PK = KIND#target_rate` で Query | 昇順 |
| 2 | データ系列の全件取得 | `GET /finance/interest-rate` | `PK = KIND#dgs10` で Query | 昇順 |

現時点ではすべて全件取得（`ScanIndexForward=True`）で対応する。将来的にデータ量が増加した場合は、SK の範囲指定による期間絞り込みを検討する。

---

## データ登録

### 方式

データの登録・更新は手動スクリプト（`bin/load_interest_rate.py`）で行う。Lambda からの書き込みは行わない。

```bash
# dev 環境に登録
python bin/load_interest_rate.py --env dev

# pro 環境に登録
python bin/load_interest_rate.py --env pro
```

### スクリプトの動作

1. FRED API（pandas_datareader）から 1982年〜2025年末のデータを取得
2. 日次データを月次（月末値）にリサンプリング
3. `batch_writer` で DynamoDB に書き込み（PutItem による冪等な上書き）

### データの境界

| 期間 | データソース | 保持先 |
|------|------------|--------|
| 〜2025-12-31 | FRED（手動取得） | DynamoDB |
| 2026-01-01〜 | FRED（Lambda がリクエスト時に取得） | レスポンスに動的結合（DynamoDB には書き込まない） |

---

## 将来のデータ系列追加

新しいデータ系列を追加する手順:

1. PK の新しいパターンを本ドキュメントに追記する（例: `KIND#new_series`）
2. `bin/` に登録スクリプトを作成する
3. `repositories/finance_repository.py` の `query_by_kind` はそのまま利用可能
4. サービス層に新しいデータ系列の取得・結合ロジックを追加する

シングルテーブル設計により、テーブルや SAM テンプレートの変更は不要。

---

## DynamoDB 固有の考慮事項

### パーティションサイズ

同一 PK のアイテム数は数千程度が推奨される。月次データの場合、30年分でも約 360 件であり、十分に余裕がある。

### 数値の精度

DynamoDB の Number 型は内部的に Decimal で管理される。Python の `float` との変換時に丸め誤差が発生しないよう、登録時は `Decimal(str(round(value, 4)))` として 4 桁に丸めて格納する。

### 読み込み整合性

Query はデフォルトで結果整合性読み込み（Eventually Consistent Read）。データの更新は手動スクリプトのみであり、リアルタイム性が求められないため、結果整合性で十分である。
