#!/usr/bin/env bash
# PySpark 3.5.6 wheel が同梱する Hadoop 3.3.4 client jar を 3.4.1 に差し替える。
# hadoop-aws:3.4.1 (AWS SDK v2 採用版) と pyspark 内 Hadoop のクラスパスを揃えるため。
# 本番 EMR 7.13.0 では Hadoop 3.4.x が同梱されているのでこの処理は不要。
# 冪等。.venv を再生成するたび uv sync の後に実行する想定。

set -euo pipefail

HADOOP_OLD=3.3.4
HADOOP_NEW=3.4.1
MAVEN_REPO=https://repo1.maven.org/maven2

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JARS_DIR="${SCRIPT_DIR}/../.venv/lib/python3.11/site-packages/pyspark/jars"

if [[ ! -d "$JARS_DIR" ]]; then
  echo "ERROR: pyspark jars directory not found: $JARS_DIR" >&2
  echo "Hint: run 'uv sync --all-groups' first." >&2
  exit 1
fi

JARS_DIR="$(cd "$JARS_DIR" && pwd)"

for ARTIFACT in hadoop-client-api hadoop-client-runtime; do
  NEW_JAR="${JARS_DIR}/${ARTIFACT}-${HADOOP_NEW}.jar"
  OLD_JAR="${JARS_DIR}/${ARTIFACT}-${HADOOP_OLD}.jar"

  if [[ -f "$NEW_JAR" ]]; then
    echo "skip: ${ARTIFACT}-${HADOOP_NEW}.jar already present"
    continue
  fi

  if [[ -f "$OLD_JAR" ]]; then
    mv "$OLD_JAR" "${OLD_JAR}.bak"
    echo "moved: ${ARTIFACT}-${HADOOP_OLD}.jar -> ${ARTIFACT}-${HADOOP_OLD}.jar.bak"
  fi

  URL="${MAVEN_REPO}/org/apache/hadoop/${ARTIFACT}/${HADOOP_NEW}/${ARTIFACT}-${HADOOP_NEW}.jar"
  echo "downloading: $URL"
  curl -fsSL -o "$NEW_JAR" "$URL"
  echo "placed: ${ARTIFACT}-${HADOOP_NEW}.jar"
done

echo "Done. Hadoop client jars patched to ${HADOOP_NEW}."
