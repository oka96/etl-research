#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL_SETUP_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DIST_DIR="${SCRIPT_DIR}/dist"
EVENTBRIDGE_VERSION="${ROCKETMQ_EVENTBRIDGE_VERSION:-1.1.0}"
EVENTBRIDGE_ZIP="${LOCAL_SETUP_DIR}/rocketmq-eventbridge-${EVENTBRIDGE_VERSION}-bin-release.zip"
EVENTBRIDGE_URL="${ROCKETMQ_EVENTBRIDGE_URL:-https://archive.apache.org/dist/rocketmq/rocketmq-eventbridge/${EVENTBRIDGE_VERSION}/rocketmq-eventbridge-${EVENTBRIDGE_VERSION}-bin-release.zip}"

if [[ ! -f "${EVENTBRIDGE_ZIP}" ]]; then
  curl -fL "${EVENTBRIDGE_URL}" -o "${EVENTBRIDGE_ZIP}"
fi

rm -rf "${DIST_DIR}"
mkdir -p "${DIST_DIR}"
unzip -q "${EVENTBRIDGE_ZIP}" -d "${DIST_DIR}"

docker build \
  -t etl-research/rocketmq-eventbridge:phase2 \
  -f "${LOCAL_SETUP_DIR}/phase2-k8s/rocketmq-eventbridge.Dockerfile" \
  "${DIST_DIR}"
