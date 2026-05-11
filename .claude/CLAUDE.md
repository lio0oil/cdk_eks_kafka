# CLAUDE.md

AWS CDK（Python）で EKS クラスター上に Kafka 基盤を構築するプロジェクト。

## 出力規約

- **言語**: ユーザーとのやり取りは日本語。識別子・ライブラリ名・エラーメッセージなど一次情報は原文のまま。
- **強調は `**bold**` と箇条書き**。絵文字（装飾アイコン・チェックマーク・警告マーク含む）は応答・コミット・コード・ドキュメントすべてで使わない。明示要求があった場合のみ解除。

## 作業ルール

### 変更の最小化

- バグ修正にリファクタを混ぜない。1 コミット／PR は 1 テーマ。
- 「将来必要そう」な抽象化を先回りしない。実需が出てから足す。
- 既存ファイル編集を優先。新規ファイル作成は必要なときだけ。

### Test-First

- 新機能・バグ修正は**失敗するテストを先に書く**。red を確認してから green に進む。
- バグ修正は**再現テスト**を先に書いて失敗を確認 → 修正で通す。
- テストが書きづらい設計に当たったら、テスト可能な構造へリファクタしてから実装。「テスト不可だから諦める」を選ばない。
- **モックは `pytest-mock` を使う**（`unittest.mock` を直接 import しない）。`mocker` フィクスチャ経由で `mocker.Mock(spec=...)` / `mocker.patch(...)` を呼ぶ。テスト終了時に自動でクリーンアップされ、テスト間のリーク・後始末漏れを防げる。
- 例外（test-first を強制しない）: 1 回限りのスクリプト、データ移行、UI スパイク、外部 API 挙動確認の実験コード。本実装に昇格する時点でテストを書く。

### コミット・PR

- **`git commit` / `git push` はユーザーが明示依頼したときだけ実行**。ステージングは `git add path/to/file` でファイル指定（`-A` / `.` は `.env` 等を巻き込みやすい）。
- **コミットメッセージは [Conventional Commits](https://www.conventionalcommits.org/) 形式**: `<type>(<scope>): <description>`。type は `feat / fix / docs / style / refactor / test / chore / perf / ci / build / revert`。破壊的変更は `<type>!:` または body に `BREAKING CHANGE:` を入れる。pre-commit の commit-msg フック（commitizen）で機械検証されるため形式違反はコミット失敗。`uv run cz commit` で対話的に作成可能。
- description（1 行目）は「何を」を構造化。**「なぜ」は body に書く**（差分から読める「何を」は body には最小限）。
- フックが落ちたら**新規コミット**で修正（`--amend` は前コミットを書き換えるリスクあり）。`--no-verify` / `--force` / `reset --hard` は明示依頼があったときのみ。
- リモート・共有状態に影響する操作（push、PR 作成、Issue コメント、外部送信）は事前確認。

### コミット前の自動検証（pre-commit hook）

`.pre-commit-config.yaml` で以下を機械的に強制している。クローン直後に `uv sync --all-groups && uv run pre-commit install` でフックを有効化すること（実行しないと検証が効かない）。

- **gitleaks**: AWS / GCP / GitHub Token 等のシークレットを検出。
- **commitizen**（commit-msg ステージ）: コミットメッセージが Conventional Commits 形式に従っているか検証。違反するとコミット失敗。
- **ruff (check + format)**: lint と整形（`--fix` で自動修正）。設定は `pyproject.toml` の `[tool.ruff]`。
- **pyright**: 静的型チェック（`typeCheckingMode = "standard"`）。
- **pytest**: 全テストが通過しないとコミット失敗。設定は `pyproject.toml` の `[tool.pytest.ini_options]`。CDK のテストは大半が CloudFormation 構造に対する宣言的検査で行カバレッジと品質が相関しにくいため、カバレッジ閾値は強制していない。代わりにロジックを持つモジュール（`_manifest.py` 等）と AWS リソース構成の重要 invariant（NLB listener 数 / SG ingress / IAM trust 等）にテストを書く方針。

`--no-verify` での回避は前述ルール通り**明示依頼があったときのみ**。フックが落ちたら原因を修正して新規コミットを作る（amend は使わない）。

### コメント・ドキュメント

- コメントは**コードから読めない理由**だけ書く（隠れた制約・回避策・外部 API の癖など）。「何をしているか」は識別子で表現。
- タスク番号・PR 番号・呼び出し元の名前はコメントに残さない（陳腐化する）。PR 説明欄に書く。
- README・ドキュメントの新規作成はユーザー依頼があったときのみ。

### エラーハンドリング

- 起こり得ないケースの防御コード（過剰 `try/except`、無意味な fallback）を入れない。
- バリデーションは**システム境界**（ユーザー入力・外部 API レスポンス）でだけ厳密に。内部呼び出しは型・契約を信頼する。

### セキュリティ

- 環境変数・シークレットをコード／コミットに混入させない（`.env` は `.gitignore`、テンプレートは `.env.example`）。
- 外部入力をシェル・SQL・HTML に直接渡さない。OWASP Top 10 を意識。
- 認証・認可・暗号系の変更は前後でユーザーに伝える。

## 実行環境ルール

### 破壊的・不可逆操作は事前確認

- ファイル・ブランチ削除、`rm -rf`、`git reset --hard`、強制 push
- パッケージのダウングレード・削除
- CI / インフラ設定の変更
- 外部サービス（Slack、メール、GitHub Issue/PR コメント）への送信

過去に許可された操作も**スコープ外には拡張しない**。

## コマンド

```bash
uv sync                      # 依存関係インストール
uv run pytest                # テスト実行
uv run pytest tests/unit/test_ekscdk_stack.py::test_stack_synthesizes  # 単一テスト
cdk synth                                                              # synth
```

## 非自明な設計判断

設計理由は`SPEC.md`を参照する