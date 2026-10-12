#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Tests for bin/single-node/examples/load-examples.sh. Run from the repo root:
#   bash bin/single-node/tests/test_load_examples_sh.sh
# Exits 0 if every check passes, 1 otherwise.
#
# The loader only talks to Texera over HTTP, so a stub `curl` placed ahead of
# the real one on PATH is enough to drive every branch deterministically -- no
# docker, no network, no sleeping. The two things guarded here both regressed
# silently once (#8721):
#
#   * the ownerEmail sent to file-service must be the email the server issued
#     in the token, not one concatenated from the username; and
#   * a failed upload must make the loader exit non-zero instead of printing
#     "complete".

set -u

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
LOADER="$REPO_ROOT/bin/single-node/examples/load-examples.sh"

PASS=0
FAIL=0

_pass() { printf "  \e[32m✓\e[0m %s\n" "$1"; PASS=$((PASS+1)); }
_fail() {
    printf "  \e[31m✗\e[0m %s\n" "$1"
    [[ $# -ge 2 ]] && printf "      %s\n" "$2"
    FAIL=$((FAIL+1))
}

# The loader itself needs jq, so a runner without it cannot exercise anything.
if ! command -v jq >/dev/null 2>&1; then
    _pass "skip: jq not installed — load-examples.sh cannot be exercised"
    printf "\n%d passed, %d failed\n" "$PASS" "$FAIL"
    exit 0
fi

# base64url, the JWT flavour: standard base64 with a different alphabet and no
# padding. Built with jq so no `base64` binary is needed (GNU takes -d, BSD -D).
_b64url() { jq -Rr '@base64' | tr '+/' '-_' | tr -d '='; }

# A syntactically real JWT whose payload is $1. Never verified by anything —
# the loader only reads claims out of a token its own login call returned.
_mk_jwt() {
    printf '%s.%s.%s' \
        "$(printf '%s' '{"alg":"HS256","typ":"JWT"}' | _b64url)" \
        "$(printf '%s' "$1" | _b64url)" \
        "c2lnbmF0dXJl"
}

# One isolated run of the loader: a scratch tree with one dataset and one
# workflow, a stub curl on PATH, and the loader copied in so that its
# SCRIPT_DIR-derived datasets/ and workflows/ point at the scratch tree.
# Behaviour knobs are read from the environment by the stub (STUB_*).
# Called directly (never in a command substitution, which would lose the
# variables): sets $RUN_RC, $RUN_OUT and $RUN_LOG for the caller. The login
# token is $1; any further KEY=VAL arguments become stub knobs.
_run_loader() {
    local token="$1"; shift
    local tmp
    tmp=$(mktemp -d)
    mkdir -p "$tmp/bin" "$tmp/examples/datasets/iris-species" "$tmp/examples/workflows"
    printf 'sepal,species\n1.0,setosa\n' > "$tmp/examples/datasets/iris-species/Iris.csv"
    printf 'an example\n' > "$tmp/examples/datasets/iris-species/description.txt"
    printf '{"operators":[],"links":[]}\n' > "$tmp/examples/workflows/Example Workflow.json"
    cp "$LOADER" "$tmp/examples/load-examples.sh"

    RUN_LOG="$tmp/requests.log"
    : > "$RUN_LOG"

    cat > "$tmp/bin/curl" <<STUB
#!/usr/bin/env bash
# Stub curl: log the requested URL, then answer with the shape the loader
# greps for. Only the cases the loader actually calls are handled.
url=""
for a in "\$@"; do case "\$a" in http*) url="\$a"; break;; esac; done
printf '%s\n' "\$url" >> "$RUN_LOG"

case "\$url" in
    *healthcheck*)                 printf '200' ;;
    */auth/login)
        if [ -n "\${STUB_NO_LOGIN:-}" ]; then printf '{"error":"no such user"}'
        else printf '{"accessToken":"%s"}' "\${STUB_LOGIN_TOKEN}"; fi ;;
    */auth/register)               printf '{"accessToken":"%s"}' "\${STUB_REGISTER_TOKEN:-\${STUB_LOGIN_TOKEN}}" ;;
    */dataset/list|*/workflow/list) printf '[]' ;;
    */dataset/create)              printf '{"did":1}' ;;
    *multipart-upload/part*)       printf '200' ;;
    *type=init*)
        if [ -n "\${STUB_FAIL_INIT:-}" ]; then printf '{"code":400,"message":"Dataset not found"}'
        else printf '{"missingParts":[1],"completedPartsCount":0}'; fi ;;
    *type=finish*)                 printf '{"message":"upload finished"}' ;;
    *type=abort*)                  printf '{"message":"aborted"}' ;;
    */version/create)
        if [ -n "\${STUB_FAIL_VERSION:-}" ]; then printf '{"code":500}'
        else printf '{"datasetVersion":{"dvid":1}}'; fi ;;
    */workflow/create)             printf '{"wid":1}' ;;
    *)                             printf '{}' ;;
esac
STUB
    chmod +x "$tmp/bin/curl"

    RUN_RC=0
    RUN_OUT=$(PATH="$tmp/bin:$PATH" STUB_LOGIN_TOKEN="$token" \
        env "$@" bash "$tmp/examples/load-examples.sh" 2>&1) || RUN_RC=$?
}

ADMIN_TOKEN=$(_mk_jwt '{"sub":"texera","userId":1,"email":"texera","role":"ADMIN"}')

# 1) The owner email used for dataset lookups comes from the token, so an
#    account whose email is not "<username>@example.com" still gets its files.
#    This is the #8721 regression itself: every upload 400'd because the
#    loader guessed texera@example.com for an admin stored as "texera".
_run_loader "$ADMIN_TOKEN"
if [[ "$RUN_RC" == 0 ]]; then
    _pass "all-success run exits 0"
else
    _fail "all-success run should exit 0" "rc=$RUN_RC out=$(echo "$RUN_OUT" | tail -3)"
fi

if grep -q 'ownerEmail=texera&' "$RUN_LOG"; then
    _pass "ownerEmail is taken from the token's email claim"
else
    _fail "ownerEmail should be the token's email claim" \
          "$(grep -o 'ownerEmail=[^&]*' "$RUN_LOG" | sort -u | tr '\n' ' ')"
fi

if grep -q 'ownerEmail=texera@example.com' "$RUN_LOG"; then
    _fail "ownerEmail must not be concatenated from the username" \
          "$(grep -o 'ownerEmail=[^&]*' "$RUN_LOG" | sort -u | tr '\n' ' ')"
else
    _pass "ownerEmail is never guessed as <username>@example.com"
fi

# 2) A failed upload has to surface in the exit status. The loader keeps going
#    so one bad file does not abandon the rest, but it must not claim success.
_run_loader "$ADMIN_TOKEN" STUB_FAIL_INIT=1
if [[ "$RUN_RC" != 0 ]]; then
    _pass "failed upload makes the loader exit non-zero"
else
    _fail "failed upload should exit non-zero" "rc=$RUN_RC"
fi
if [[ "$RUN_OUT" == *"error(s)"* && "$RUN_OUT" != *"loading complete"* ]]; then
    _pass "failed run reports the error count, not 'complete'"
else
    _fail "failed run should not report completion" "$(echo "$RUN_OUT" | tail -2)"
fi

# 3) Same for a failed version create — the other formerly-silent path.
_run_loader "$ADMIN_TOKEN" STUB_FAIL_VERSION=1
if [[ "$RUN_RC" != 0 ]]; then
    _pass "failed version create makes the loader exit non-zero"
else
    _fail "failed version create should exit non-zero" "rc=$RUN_RC"
fi

# 4) The register fallback still sends an @-shaped address (the server rejects
#    anything else), and the owner email then comes from the register token.
REG_TOKEN=$(_mk_jwt '{"sub":"texera","userId":2,"email":"texera@example.com","role":"REGULAR"}')
_run_loader "$ADMIN_TOKEN" STUB_NO_LOGIN=1 STUB_REGISTER_TOKEN="$REG_TOKEN"
if [[ "$RUN_RC" == 0 ]] && grep -q '/auth/register' "$RUN_LOG"; then
    _pass "register fallback runs when login fails"
else
    _fail "register fallback should run and succeed" "rc=$RUN_RC out=$(echo "$RUN_OUT" | tail -3)"
fi
if grep -q 'ownerEmail=texera%40example.com\|ownerEmail=texera@example.com' "$RUN_LOG"; then
    _pass "a registered account's own email is used for lookups"
else
    _fail "registered account should use its register email" \
          "$(grep -o 'ownerEmail=[^&]*' "$RUN_LOG" | sort -u | tr '\n' ' ')"
fi

# 5) A token with no email claim must abort with a clear message rather than
#    sending an empty ownerEmail and getting an opaque 400 per file.
_run_loader "$(_mk_jwt '{"sub":"texera","userId":1,"role":"ADMIN"}')"
if [[ "$RUN_RC" != 0 ]] && [[ "$RUN_OUT" == *"email"* ]]; then
    _pass "token without an email claim aborts with a clear message"
else
    _fail "missing email claim should abort clearly" "rc=$RUN_RC out=$(echo "$RUN_OUT" | tail -2)"
fi

printf "\n%d passed, %d failed\n" "$PASS" "$FAIL"
(( FAIL == 0 ))
