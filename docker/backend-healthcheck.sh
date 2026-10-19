#!/bin/sh
set -eu
port=${SERVER_PORT:-${BACKEND_SERVER_PORT:-${BACKEND_PORT:-6688}}}
context=${SERVER_SERVLET_CONTEXT_PATH:-}
context=${context%/}
# This checks the anonymous HTTP/config route, without a mail health dependency.
# A healthy result is not proof of DB, Redis, remote judge or sandbox readiness.
response=$(curl --noproxy '*' --fail --silent --show-error --max-time 5 \
    "http://127.0.0.1:${port}${context}/api/get-website-config")
printf '%s\n' "$response" | grep -Eq '"status"[[:space:]]*:[[:space:]]*200[[:space:]]*[,}]'
