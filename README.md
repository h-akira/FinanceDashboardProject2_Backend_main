# Backend Main

金融ダッシュボードアプリのメイン API。Python + SAM (API Gateway + Lambda)。

## 構成

```
src/                          # Lambda ソースコード (CodeUri)
├── app.py                    # Lambda ハンドラー (Powertools)
├── custom_chart_sources.json # カスタムチャートのソース定義
├── common/                   # 共通ユーティリティ
├── routes/                   # ルーティング (users, finance)
├── services/                 # ビジネスロジック
└── repositories/             # DynamoDB アクセス
bin/                          # 運用スクリプト
├── load_data.py              # DynamoDB データ投入
└── requirements.txt          # bin/ 用の依存
tests/                        # ユニットテスト (pytest + moto)
├── pytest.ini
└── local/
docs/                         # サブモジュール内設計ドキュメント
template.yaml                 # SAM テンプレート
samconfig.toml                # SAM CLI 設定
buildspec.yml                 # CodeBuild 用
```

## セットアップ

```bash
# テスト用 venv
python3 -m venv env
source env/bin/activate
pip install -r requirements-dev.txt

# bin/ 用 venv（データ投入スクリプト）
cd bin
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
```

## ローカル開発

```bash
# SAM ローカル API 起動
sam build && sam local start-api

# テスト実行
source env/bin/activate
python -m pytest tests/ -v
```

## デプロイ

```bash
sam build
sam deploy --parameter-overrides Env=dev
```

CI/CD では `buildspec.yml` により自動デプロイされる。

## データ投入 (bin/load_data.py)

DynamoDB に金融データを投入するスクリプト。FRED / yfinance からデータを取得し、半月次にリサンプリングして格納する。

```bash
cd bin
source env/bin/activate

# 全ソース投入
./load_data.py --env dev

# 特定ソースのみ
./load_data.py --env dev --sources target_rate,dgs10

# dry-run（DynamoDB に書き込まず stdout に出力）
./load_data.py --env dev --dry-run

# データ削除
./load_data.py --env dev --remove-all --sources sp500
```

## テスト

```bash
source env/bin/activate
python -m pytest tests/ -v
```

pytest + moto で DynamoDB・Cognito をモックしてテストする。外部 API（FRED, yfinance）はモックする。

## 環境変数

Lambda 実行時に以下の環境変数が設定される（`template.yaml` で定義）。

| 変数名 | 用途 |
|--------|------|
| `TABLE_NAME` | DynamoDB テーブル名 |
| `USER_POOL_ID` | Cognito User Pool ID |
| `CLIENT_ID` | Cognito App Client ID |
