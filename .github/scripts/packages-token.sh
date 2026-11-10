#!/usr/bin/env bash
# Keep a live nora OIDC token in a file for the image build (make docker-build / docker-buildx). The token lives minutes, and BuildKit
# reads a secret FILE each time a RUN mounts it, so a RUN late in a long build (arm64 under
# QEMU) still gets a live one. Same loop as github-actions dotnet-service-build.yml.
# Needs `id-token: write`. Exports PRIVATE_PACKAGES_TOKEN_FILE to the job.
set -euo pipefail
dir="$RUNNER_TEMP/packages-token"; mkdir -p "$dir"; chmod 700 "$dir"
file="$dir/PRIVATE_PACKAGES_TOKEN"
write() { printf '%s' "$1" > "$dir/.t" && mv -f "$dir/.t" "$file"; } # atomic for a reader
mint() {
  curl -sSf --retry 3 -H "Authorization: bearer ${ACTIONS_ID_TOKEN_REQUEST_TOKEN}" \
    "${ACTIONS_ID_TOKEN_REQUEST_URL}&audience=packages.maestra.io" | jq -r '.value // empty'
}
t=$(mint) && [ -n "$t" ] || { echo "::error::cannot mint the packages OIDC token"; exit 1; }
echo "::add-mask::$t"
write "$t"
echo "PRIVATE_PACKAGES_TOKEN_FILE=$file" >> "$GITHUB_ENV"
# Lives until the runner's orphan cleanup at the end of the job. Its output goes to a file, not
# the step log: the runner stops reading this step's pipe when the step ends, so a later write
# there would SIGPIPE the loop, and an ::add-mask:: from it would not be applied anyway. The
# token itself is never printed; failed refreshes land in $dir/refresh.log (no token in it),
# which the workflow prints when the job fails.
( while sleep 60; do
    if t=$(mint) && [ -n "$t" ]; then write "$t"
    else echo "$(date -u +%FT%TZ) token refresh failed" >> "$dir/refresh.log"; fi
  done ) </dev/null >/dev/null 2>&1 &
