#!/bin/sh
set -eu
# Disable pathname expansion; JAVA_OPTS is split only on whitespace, never eval'd.
set -f

BACKEND_SERVER_PORT=${SERVER_PORT:-${BACKEND_SERVER_PORT:-${BACKEND_PORT:-6688}}}
case "$BACKEND_SERVER_PORT" in
    ''|*[!0-9]*) echo 'Backend port must be an integer from 1 to 65535.' >&2; exit 64 ;;
esac
if [ "${#BACKEND_SERVER_PORT}" -gt 5 ] || [ "$BACKEND_SERVER_PORT" -lt 1 ] || [ "$BACKEND_SERVER_PORT" -gt 65535 ]; then
    echo 'Backend port must be an integer from 1 to 65535.' >&2
    exit 64
fi
export BACKEND_SERVER_PORT
export SPRING_PROFILES_ACTIVE=${SPRING_PROFILES_ACTIVE:-prod}

mkdir -p /hoj/file /hoj/testcase /hoj/log/backend
# Use JAVA_TOOL_OPTIONS for JVM options containing spaces. Quotes or shell
# commands inside JAVA_OPTS are deliberately not interpreted as shell syntax.
# shellcheck disable=SC2086
exec java ${JAVA_OPTS:-} -Djava.awt.headless=true -jar /opt/hoj/backend.jar "$@"
