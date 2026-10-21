#!/usr/bin/env bash
# Downloads aeron-all-${AERON_VERSION}.jar (default 1.53.3) for the in-process systests driver.
# Uses the Google Cloud Storage mirror of Maven Central (repo1.maven.org rate limits with 429s)
# and verifies the published SHA-1.
#
#   scripts/fetch-aeron-jar.sh [dest-dir]     # default dest: systests/driver
set -euo pipefail

AERON_VERSION="${AERON_VERSION:-1.53.3}"
MAVEN_MIRROR="${MAVEN_MIRROR:-https://maven-central.storage-download.googleapis.com/maven2}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEST_DIR="${1:-${ROOT}/systests/driver}"
JAR="aeron-all-${AERON_VERSION}.jar"
URL="${MAVEN_MIRROR}/io/aeron/aeron-all/${AERON_VERSION}/${JAR}"

mkdir -p "${DEST_DIR}"
if [[ -f "${DEST_DIR}/${JAR}" ]]; then
  echo "${DEST_DIR}/${JAR} already present"
  exit 0
fi

tmp="$(mktemp "${DEST_DIR}/.${JAR}.XXXXXX")"
trap 'rm -f "${tmp}"' EXIT
echo "Fetching ${URL}"
curl -sSfL --retry 5 --retry-delay 2 -o "${tmp}" "${URL}"
expected="$(curl -sSfL --retry 5 --retry-delay 2 "${URL}.sha1" | awk '{print $1}')"
actual="$(sha1sum "${tmp}" | awk '{print $1}')"
if [[ "${expected}" != "${actual}" ]]; then
  echo "SHA-1 mismatch for ${JAR}: expected ${expected}, got ${actual}" >&2
  exit 1
fi
chmod 644 "${tmp}"
mv "${tmp}" "${DEST_DIR}/${JAR}"
trap - EXIT
echo "Saved ${DEST_DIR}/${JAR}"
