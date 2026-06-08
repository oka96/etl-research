#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL_SETUP_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
CONNECT_SRC="${LOCAL_SETUP_DIR}/rocketmq-connect-src"
CONNECT_REPO="${ROCKETMQ_CONNECT_REPO:-https://github.com/apache/rocketmq-connect.git}"
CONNECT_REF="${ROCKETMQ_CONNECT_REF:-85da68c92f599f0a289c0e63de919ca417289f69}"
TRANSFORM_TARGET="${CONNECT_SRC}/rocketmq-connect-sample/src/main/java/org/apache/rocketmq/connect/file/FilterTransform.java"

if [[ ! -d "${CONNECT_SRC}" ]]; then
  git clone --filter=blob:none "${CONNECT_REPO}" "${CONNECT_SRC}"
fi

(
  cd "${CONNECT_SRC}"
  git fetch --depth 1 origin "${CONNECT_REF}"
  git checkout --detach "${CONNECT_REF}"
)

if [[ ! -f "${TRANSFORM_TARGET}" ]]; then
  echo "Missing ${TRANSFORM_TARGET}; RocketMQ Connect source layout changed." >&2
  exit 1
fi

cp "${SCRIPT_DIR}/FilterTransform.java" "${TRANSFORM_TARGET}"

(
  cd "${CONNECT_SRC}"
  mvn -q -DskipTests -pl rocketmq-connect-sample,distribution -am package
)

docker build \
  -t etl-research/rocketmq-connect:phase2 \
  -f "${LOCAL_SETUP_DIR}/phase2-k8s/rocketmq-connect.Dockerfile" \
  "${LOCAL_SETUP_DIR}"
