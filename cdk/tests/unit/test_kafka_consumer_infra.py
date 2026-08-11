import zipfile

from ekscdk.constructs.kafka_consumer_infra import build_lambda_zip


def test_build_lambda_zip_places_files_at_zip_root(tmp_path):
    (tmp_path / "index.py").write_text("def handler(event, context):\n    pass\n")
    output_zip = str(tmp_path / "build" / "function.zip")

    build_lambda_zip(str(tmp_path), output_zip)

    with zipfile.ZipFile(output_zip) as zf:
        assert zf.namelist() == ["index.py"]


def test_build_lambda_zip_excludes_build_dir_from_itself(tmp_path):
    # build/ 配下に出力するため、2 回目のビルドで前回の zip 自身を巻き込まないこと。
    (tmp_path / "index.py").write_text("def handler(event, context):\n    pass\n")
    output_zip = str(tmp_path / "build" / "function.zip")

    build_lambda_zip(str(tmp_path), output_zip)
    build_lambda_zip(str(tmp_path), output_zip)

    with zipfile.ZipFile(output_zip) as zf:
        assert zf.namelist() == ["index.py"]


def test_build_lambda_zip_is_deterministic(tmp_path):
    # zipfile はデフォルトでエントリの mtime を保存するため、ソースに変更が無くても
    # 毎回バイト列が変わると同一ソースからのアップロードのたびに無意味な差分が出る。
    # date_time 固定によりソース不変なら 2 回目のビルドもバイト単位で一致すること。
    (tmp_path / "index.py").write_text("def handler(event, context):\n    pass\n")
    output_zip = str(tmp_path / "build" / "function.zip")

    build_lambda_zip(str(tmp_path), output_zip)
    with open(output_zip, "rb") as f:
        first = f.read()

    build_lambda_zip(str(tmp_path), output_zip)
    with open(output_zip, "rb") as f:
        second = f.read()

    assert first == second
