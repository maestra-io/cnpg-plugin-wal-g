#!/usr/bin/env bash
# Docker credential helper for nora (packages.maestra.io): BuildKit pulls the FROM images
# with a GitHub OIDC token that lives minutes. A `docker login` stores ONE token, so a FROM
# or layer pulled late in a long build gets a 401. buildx re-reads the credentials every
# ~5 min, and this helper mints a fresh token on each read. Copy of the nora-cred-helper
# block in maestra-io/github-actions dotnet-service-build.yml. Needs `id-token: write`.
set -euo pipefail
bin="$RUNNER_TEMP/nora-bin"; mkdir -p "$bin"
( umask 077; printf 'url=%q\ntoken=%q\naud=%q\n' "$ACTIONS_ID_TOKEN_REQUEST_URL" \
  "$ACTIONS_ID_TOKEN_REQUEST_TOKEN" packages.maestra.io > "$bin/oidc.env" )
cat > "$bin/docker-credential-nora" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[ "${1:-}" = get ] || exit 0
server=$(cat) # docker sends it without a newline
. "${BASH_SOURCE[0]%/*}/oidc.env"
t=$(curl -sSf --retry 3 --connect-timeout 10 --max-time 30 -H "Authorization: bearer $token" \
  "$url&audience=$aud" | jq -r '.value // empty')
[ -n "$t" ]
jq -cn --arg s "$server" --arg t "$t" '{ServerURL: $s, Username: "token", Secret: $t}'
EOF
chmod 755 "$bin/docker-credential-nora"
echo "$bin" >> "$GITHUB_PATH"
export PATH="$bin:$PATH"
cfg="${DOCKER_CONFIG:-$HOME/.docker}/config.json"; mkdir -p "$(dirname "$cfg")"
[ -s "$cfg" ] || echo '{}' > "$cfg"
# credHelpers wins over a stored auths entry for the host; drop a stale one anyway.
jq '.credHelpers["packages.maestra.io"] = "nora" | del(.auths["packages.maestra.io"])' "$cfg" > "$cfg.t" \
  && mv "$cfg.t" "$cfg"
# Prove the helper answers before the build depends on it.
printf 'packages.maestra.io' | docker-credential-nora get | jq -e '.Secret | length > 0' >/dev/null
