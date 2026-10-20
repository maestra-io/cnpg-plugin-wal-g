#!/usr/bin/env bash
# End-to-end test of the plugin image against S3 with temporary (STS) credentials:
# kind + CNPG + cert-manager + MinIO, the setup PR #1 (spec.storage.s3.sessionToken)
# was validated with, scripted so CI can run it (.github/workflows/e2e-sessiontoken.yaml).
#
# Proves, with the wal-g build inside IMAGE:
#   positive (namespace pg-pos, BackupConfig WITH sessionToken)
#     - continuous WAL archiving with an STS triple (key, secret, session token)
#     - backup-push through a plugin Backup
#     - wal-g backup-list and wal-verify run in the sidecar (as the chart's archive check does)
#     - credential rotation: a new STS triple written into the Secret in place, no restart,
#       then archiving and a second Backup with it
#     - restore through the plugin into a new Cluster, replaying WAL archived after the backup
#   negative control (namespace pg-neg, same Secret, BackupConfig WITHOUT sessionToken)
#     - archiving fails with an S3 auth error, archived_count stays 0, a Backup fails
#
# Needs: docker (IMAGE already built), kind, kubectl, helm, aws (CLI v2), jq.
#   IMAGE=docker.io/local/cnpg-plugin-wal-g:e2e hack/e2e/sessiontoken-e2e.sh
#   hack/e2e/sessiontoken-e2e.sh diagnostics   # dump cluster state (CI runs it on failure)
set -euo pipefail

IMAGE=${IMAGE:-docker.io/local/cnpg-plugin-wal-g:e2e}
KIND_CLUSTER=${KIND_CLUSTER:-walg-e2e}
CNPG_MANIFEST=${CNPG_MANIFEST:-https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.30/releases/cnpg-1.30.0.yaml}
CERT_MANAGER_MANIFEST=${CERT_MANAGER_MANIFEST:-https://github.com/cert-manager/cert-manager/releases/download/v1.18.2/cert-manager.yaml}
MINIO_IMAGE=${MINIO_IMAGE:-docker.io/bitnamilegacy/minio:2025.4.22}
PG_IMAGE=${PG_IMAGE:-ghcr.io/cloudnative-pg/postgresql:18.4-system-trixie}
PG_MAJOR=18
PLUGIN=cnpg-extensions.yandex.cloud
BUCKET=walg-test
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin-e2e-pass
WALG_USER=walguser
WALG_PASSWORD=walguser-e2e-pass
PF_PORT=19000
REPO_ROOT=$(cd "$(dirname "$0")/../.." && pwd)

log()  { echo "[$(date -u '+%H:%M:%S')] $*"; }
pass() { echo "[$(date -u '+%H:%M:%S')] PASS: $*"; RESULTS+=("PASS: $*"); }
fail() { echo "[$(date -u '+%H:%M:%S')] FAIL: $*" >&2; exit 1; }
RESULTS=()

# wait_for <description> <timeout seconds> <command...>: retry until the command succeeds.
wait_for() {
    local desc=$1 timeout=$2; shift 2
    local deadline=$((SECONDS + timeout))
    until "$@" >/dev/null 2>&1; do
        if (( SECONDS >= deadline )); then fail "timed out after ${timeout}s waiting for: $desc"; fi
        sleep 5
    done
    log "ok: $desc"
}

diagnostics() {
    set +e
    kubectl get nodes -o wide
    kubectl get pods -A -o wide
    kubectl get clusters.postgresql.cnpg.io,backups.postgresql.cnpg.io,backupconfigs.cnpg-extensions.yandex.cloud -A -o yaml
    kubectl get events -A --sort-by=.lastTimestamp | tail -150
    kubectl -n cnpg-system logs deploy/cnpg-controller-manager --tail=300
    kubectl -n cnpg-system logs deploy/cnpg-plugin-wal-g-controller-manager --tail=300
    for ns in pg-pos pg-neg; do
        for p in $(kubectl -n "$ns" get pods -o name 2>/dev/null); do
            echo "=== $ns $p"
            kubectl -n "$ns" describe "$p" | tail -40
            kubectl -n "$ns" logs "$p" --all-containers --tail=200
        done
    done
}
if [ "${1:-}" = diagnostics ]; then diagnostics; exit 0; fi

psql_in() { # psql_in <ns> <pod> <sql>
    kubectl -n "$1" exec "$2" -c postgres -- psql -U postgres -qtAX -c "$3"
}

# Runs wal-g in the pod's plugin-wal-g sidecar with the config the plugin resolves
# (cnpg-plugin-wal-g dump-config), exactly like the chart's walg-archive-check.
walg_in() { # walg_in <ns> <pod> <backupconfig> <wal-g args...>
    local ns=$1 pod=$2 bc=$3; shift 3
    kubectl -n "$ns" exec "$pod" -c plugin-wal-g -- bash -c '
        set -euo pipefail; umask 077
        d=$(mktemp -d); trap "rm -rf $d" EXIT
        cnpg-plugin-wal-g dump-config "$1" "$2" -o "$d/walg.json" >&2
        shift 2
        wal-g --config "$d/walg.json" "$@"' walg "$ns/$bc" "$PG_MAJOR" "$@"
}

archiver_stats() { psql_in "$1" "$2" "select archived_count, failed_count, coalesce(last_archived_wal, ''), coalesce(last_failed_wal, '') from pg_stat_archiver"; }

cluster_condition() { # cluster_condition <ns> <cluster> <type> -> "status|reason|message"
    kubectl -n "$1" get clusters.postgresql.cnpg.io "$2" -o json \
        | jq -r --arg t "$3" '.status.conditions[]? | select(.type == $t) | "\(.status)|\(.reason)|\(.message)"'
}

backup_phase() { kubectl -n "$1" get backups.postgresql.cnpg.io "$2" -o jsonpath='{.status.phase}' 2>/dev/null; }

mint_sts() { # prints "key secret token" of a fresh MinIO STS AssumeRole triple
    AWS_ACCESS_KEY_ID=$WALG_USER AWS_SECRET_ACCESS_KEY=$WALG_PASSWORD AWS_SESSION_TOKEN='' \
        aws sts assume-role --endpoint-url "http://127.0.0.1:$PF_PORT" --region us-east-1 \
        --role-arn arn:xxx:xxx:xxx:xxxx --role-session-name walg-e2e --duration-seconds 3600 \
        --output json | jq -r '.Credentials | "\(.AccessKeyId) \(.SecretAccessKey) \(.SessionToken)"'
}

apply_creds_secret() { # apply_creds_secret <ns> <key> <secret> <token>
    kubectl -n "$1" create secret generic sa-creds-walg-test \
        --from-literal=access_key_id="$2" --from-literal=secret_access_key="$3" --from-literal=session_token="$4" \
        --dry-run=client -o yaml | kubectl apply -f -
}

backup_config() { # backup_config <ns> <name> <prefix> <with_token:true|false>
    local token=""
    if [ "$4" = true ]; then
        token="
      sessionToken: {name: sa-creds-walg-test, key: session_token}"
    fi
    kubectl apply -f - <<YAML
apiVersion: cnpg-extensions.yandex.cloud/v1beta1
kind: BackupConfig
metadata: {name: $2, namespace: $1}
spec:
  storage:
    type: s3
    s3:
      prefix: s3://$BUCKET/$3
      region: us-east-1
      endpointUrl: http://minio.minio.svc:9000
      forcePathStyle: true
      accessKeyId: {name: sa-creds-walg-test, key: access_key_id}
      accessKeySecret: {name: sa-creds-walg-test, key: secret_access_key}$token
  encryption:
    method: libsodium
    encryptionSecret: walg-encryption
YAML
}

pg_cluster() { # pg_cluster <ns> <name> <backupconfig> [extra spec yaml]
    kubectl apply -f - <<YAML
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata: {name: $2, namespace: $1}
spec:
  instances: 1
  imageName: $PG_IMAGE
  imagePullPolicy: IfNotPresent
  storage: {size: 1Gi}
  plugins:
  - name: $PLUGIN
    isWALArchiver: true
    parameters: {backupConfig: $3}
${4:-}
YAML
}

plugin_backup() { # plugin_backup <ns> <name> <cluster>
    kubectl apply -f - <<YAML
apiVersion: postgresql.cnpg.io/v1
kind: Backup
metadata: {name: $2, namespace: $1}
spec:
  method: plugin
  cluster: {name: $3}
  pluginConfiguration: {name: $PLUGIN}
YAML
}

# ---------------------------------------------------------------- infrastructure
log "kind cluster $KIND_CLUSTER, image $IMAGE"
kind create cluster --name "$KIND_CLUSTER" --wait 120s
kind load docker-image "$IMAGE" --name "$KIND_CLUSTER"
docker run --rm --entrypoint /usr/local/bin/wal-g "$IMAGE" --version

kubectl apply -f "$CERT_MANAGER_MANIFEST"
kubectl apply --server-side -f "$CNPG_MANIFEST"

kubectl create namespace minio
kubectl -n minio apply -f - <<YAML
apiVersion: apps/v1
kind: Deployment
metadata: {name: minio}
spec:
  selector: {matchLabels: {app: minio}}
  template:
    metadata: {labels: {app: minio}}
    spec:
      containers:
      - name: minio
        image: $MINIO_IMAGE
        env:
        - {name: MINIO_ROOT_USER, value: $MINIO_ROOT_USER}
        - {name: MINIO_ROOT_PASSWORD, value: $MINIO_ROOT_PASSWORD}
        ports: [{containerPort: 9000}]
        readinessProbe: {httpGet: {path: /minio/health/ready, port: 9000}, periodSeconds: 3}
        volumeMounts: [{name: data, mountPath: /bitnami/minio/data}]
      volumes: [{name: data, emptyDir: {}}]
---
apiVersion: v1
kind: Service
metadata: {name: minio}
spec:
  selector: {app: minio}
  ports: [{port: 9000, targetPort: 9000}]
YAML

kubectl -n cert-manager rollout status deploy/cert-manager-webhook --timeout=300s
kubectl -n cnpg-system rollout status deploy/cnpg-controller-manager --timeout=300s
kubectl -n minio rollout status deploy/minio --timeout=300s

# Non-root MinIO user that may AssumeRole (root credentials cannot).
mc_in() { kubectl -n minio exec deploy/minio -- mc --config-dir /tmp/mc "$@"; }  # HOME=/ is read-only for uid 1001
# Retried as a whole: Ready on the Deployment does not mean the server stays up
# (the Bitnami entrypoint can run a temporary MinIO for its init and restart it,
# which refused `mc alias set` right after rollout status in one CI run). Every
# step is idempotent; policy attach errors when the policy is already attached,
# so that case is accepted via `user info`.
minio_setup() {
    mc_in alias set local http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" &&
    mc_in mb --ignore-existing "local/$BUCKET" &&
    mc_in admin user add local "$WALG_USER" "$WALG_PASSWORD" &&
    { mc_in admin policy attach local readwrite --user "$WALG_USER" ||
      mc_in admin user info local "$WALG_USER" | grep -qw readwrite; }
}
wait_for "MinIO bucket + AssumeRole user" 120 minio_setup

kubectl -n minio port-forward svc/minio "$PF_PORT:9000" >/tmp/minio-pf.log 2>&1 &
PF_PID=$!
trap 'kill $PF_PID 2>/dev/null || true' EXIT
wait_for "MinIO port-forward" 60 curl -sf "http://127.0.0.1:$PF_PORT/minio/health/live"

read -r KEY1 SECRET1 TOKEN1 <<<"$(mint_sts)"
[ -n "$TOKEN1" ] || fail "MinIO STS returned no session token"
log "STS triple #1: key=$KEY1 token_len=${#TOKEN1}"
# The temporary key alone must be rejected: the shape of R2's Vault-minted credentials.
if AWS_ACCESS_KEY_ID=$KEY1 AWS_SECRET_ACCESS_KEY=$SECRET1 AWS_SESSION_TOKEN='' \
    aws s3 ls "s3://$BUCKET/" --endpoint-url "http://127.0.0.1:$PF_PORT" --region us-east-1 >/dev/null 2>&1; then
    fail "MinIO accepted the STS key without its session token; the negative control would prove nothing"
fi
AWS_ACCESS_KEY_ID=$KEY1 AWS_SECRET_ACCESS_KEY=$SECRET1 AWS_SESSION_TOKEN=$TOKEN1 \
    aws s3 ls "s3://$BUCKET/" --endpoint-url "http://127.0.0.1:$PF_PORT" --region us-east-1 >/dev/null
pass "MinIO STS: key+secret+token accepted, key+secret without token rejected"

helm upgrade --install cnpg-plugin-wal-g "$REPO_ROOT/chart" -n cnpg-system \
    --set image.repository="${IMAGE%:*}" --set image.tag="${IMAGE##*:}" --set image.pullPolicy=Never --wait --timeout 300s
kubectl -n cnpg-system rollout status deploy/cnpg-plugin-wal-g-controller-manager --timeout=300s

for ns in pg-pos pg-neg; do
    kubectl create namespace "$ns"
    apply_creds_secret "$ns" "$KEY1" "$SECRET1" "$TOKEN1"
    kubectl -n "$ns" create secret generic walg-encryption --from-literal=libsodiumKey="$(openssl rand -hex 32)"
done
backup_config pg-pos walg pg-pos true
backup_config pg-neg walg pg-neg false
pg_cluster pg-pos pg walg
pg_cluster pg-neg pg walg

# ---------------------------------------------------------------- positive
kubectl -n pg-pos wait clusters.postgresql.cnpg.io/pg --for=condition=Ready --timeout=900s
sidecar=$(kubectl -n pg-pos get pod pg-1 -o json | jq -c '.spec.initContainers[] | select(.name == "plugin-wal-g") | {image, restartPolicy, startupProbe}')
log "pg-pos sidecar: $sidecar"
[ "$(jq -r .image <<<"$sidecar")" = "$IMAGE" ] || fail "sidecar does not run $IMAGE"
psql_in pg-pos pg-1 "create table e2e(v text); insert into e2e values ('row-1-before-backup-1')"
psql_in pg-pos pg-1 "select pg_switch_wal()" >/dev/null
wait_for "pg-pos ContinuousArchiving=True" 300 bash -c "[[ \$(kubectl -n pg-pos get clusters.postgresql.cnpg.io pg -o jsonpath='{.status.conditions[?(@.type==\"ContinuousArchiving\")].status}') == True ]]"
wait_for "pg-pos archived_count >= 1" 300 bash -c "[[ \$(kubectl -n pg-pos exec pg-1 -c postgres -- psql -U postgres -qtAX -c 'select archived_count from pg_stat_archiver') -ge 1 ]]"
IFS='|' read -r archived failed last_ok _ <<<"$(archiver_stats pg-pos pg-1)"
[ "$failed" = 0 ] || fail "pg-pos archiver failed_count=$failed (expected 0)"
mc_in ls "local/$BUCKET/pg-pos/$PG_MAJOR/wal_005/" | grep -q "$last_ok" || fail "$last_ok not in MinIO"
pass "WAL archive with STS creds: archived_count=$archived failed_count=0 last_archived_wal=$last_ok (in MinIO), ContinuousArchiving=True"

plugin_backup pg-pos backup-1 pg
wait_for "Backup backup-1 completed" 600 bash -c "[[ \$(kubectl -n pg-pos get backups.postgresql.cnpg.io backup-1 -o jsonpath='{.status.phase}') =~ ^(completed|failed)$ ]]"
[ "$(backup_phase pg-pos backup-1)" = completed ] || fail "backup-1 phase=$(backup_phase pg-pos backup-1): $(kubectl -n pg-pos get backups.postgresql.cnpg.io backup-1 -o jsonpath='{.status.error}')"
B1=$(kubectl -n pg-pos get backups.postgresql.cnpg.io backup-1 -o jsonpath='{.status.backupId}')
pass "backup-push via plugin Backup: backup-1 completed, backupId=$B1"

walg_in pg-pos pg-1 walg backup-list --detail --json > /tmp/backup-list.json
jq -c '[.[] | {backup_name, wal_file_name, is_permanent}]' /tmp/backup-list.json
jq -e --arg b "$B1" 'any(.[]; .backup_name == $b)' /tmp/backup-list.json >/dev/null || fail "wal-g backup-list does not list $B1"
pass "wal-g backup-list (in the sidecar): lists $B1"

walg_in pg-pos pg-1 walg wal-verify integrity timeline --json > /tmp/wal-verify.json
jq -c 'to_entries | map({check: .key, status: .value.status})' /tmp/wal-verify.json
jq -e '[.[] | .status] | length >= 2 and all(. != "FAILURE")' /tmp/wal-verify.json >/dev/null || fail "wal-verify reports FAILURE: $(cat /tmp/wal-verify.json)"
pass "wal-g wal-verify integrity timeline (in the sidecar): $(jq -c 'to_entries | map("\(.key)=\(.value.status)") | join(" ")' /tmp/wal-verify.json)"

# Rotation, as VSO does it: a fresh triple written into the same Secret, no pod restart.
read -r KEY2 SECRET2 TOKEN2 <<<"$(mint_sts)"
[ "$KEY2" != "$KEY1" ] || fail "STS returned the same key twice"
apply_creds_secret pg-pos "$KEY2" "$SECRET2" "$TOKEN2"
restarts_before=$(kubectl -n pg-pos get pod pg-1 -o jsonpath='{.status.initContainerStatuses[?(@.name=="plugin-wal-g")].restartCount}')
psql_in pg-pos pg-1 "insert into e2e values ('row-2-after-rotation')"
before=$archived
psql_in pg-pos pg-1 "select pg_switch_wal()" >/dev/null
wait_for "pg-pos archives with rotated creds" 300 bash -c "[[ \$(kubectl -n pg-pos exec pg-1 -c postgres -- psql -U postgres -qtAX -c 'select archived_count from pg_stat_archiver') -gt $before ]]"
plugin_backup pg-pos backup-2 pg
wait_for "Backup backup-2 completed" 600 bash -c "[[ \$(kubectl -n pg-pos get backups.postgresql.cnpg.io backup-2 -o jsonpath='{.status.phase}') =~ ^(completed|failed)$ ]]"
[ "$(backup_phase pg-pos backup-2)" = completed ] || fail "backup-2 phase=$(backup_phase pg-pos backup-2)"
B2=$(kubectl -n pg-pos get backups.postgresql.cnpg.io backup-2 -o jsonpath='{.status.backupId}')
IFS='|' read -r archived failed last_ok _ <<<"$(archiver_stats pg-pos pg-1)"
[ "$failed" = 0 ] || fail "pg-pos failed_count=$failed after rotation"
restarts_after=$(kubectl -n pg-pos get pod pg-1 -o jsonpath='{.status.initContainerStatuses[?(@.name=="plugin-wal-g")].restartCount}')
[ "$restarts_after" = "$restarts_before" ] || fail "sidecar restarted during rotation ($restarts_before -> $restarts_after)"
pass "rotation (key $KEY1 -> $KEY2 in place, sidecar restarts unchanged=$restarts_after): archived_count=$archived failed_count=0, backup-2 completed backupId=$B2"

# A row only in WAL archived after backup-2: the restore must replay it.
psql_in pg-pos pg-1 "insert into e2e values ('row-3-wal-only')"
seg=$(psql_in pg-pos pg-1 "select pg_walfile_name(pg_current_wal_lsn())")
psql_in pg-pos pg-1 "select pg_switch_wal()" >/dev/null
wait_for "segment $seg archived" 300 bash -c "[[ \$(kubectl -n pg-pos exec pg-1 -c postgres -- psql -U postgres -qtAX -c 'select last_archived_wal from pg_stat_archiver') > '$seg' || \$(kubectl -n pg-pos exec pg-1 -c postgres -- psql -U postgres -qtAX -c 'select last_archived_wal from pg_stat_archiver') == '$seg' ]]"

backup_config pg-pos walg-restore pg-pos-restore true
pg_cluster pg-pos pg-restore walg-restore "  bootstrap:
    recovery:
      source: walg-source
  externalClusters:
  - name: walg-source
    plugin:
      name: $PLUGIN
      parameters: {backupConfig: walg}"
kubectl -n pg-pos wait clusters.postgresql.cnpg.io/pg-restore --for=condition=Ready --timeout=900s
rows=$(psql_in pg-pos pg-restore-1 "select string_agg(v, ',' order by v) from e2e")
[ "$rows" = "row-1-before-backup-1,row-2-after-rotation,row-3-wal-only" ] || fail "restored rows: '$rows'"
pass "restore via the plugin into new Cluster pg-restore: rows=$rows (row-3 came from WAL replay)"

# ---------------------------------------------------------------- negative control
wait_for "pg-neg postgres up" 600 kubectl -n pg-neg exec pg-1 -c postgres -- psql -U postgres -qtAX -c 'select 1'
psql_in pg-neg pg-1 "select pg_switch_wal()" >/dev/null || true
wait_for "pg-neg ContinuousArchiving=False" 300 bash -c "[[ \$(kubectl -n pg-neg get clusters.postgresql.cnpg.io pg -o jsonpath='{.status.conditions[?(@.type==\"ContinuousArchiving\")].status}') == False ]]"
cond=$(cluster_condition pg-neg pg ContinuousArchiving)
log "pg-neg ContinuousArchiving: $cond"
autherr=$(grep -oE 'InvalidTokenId|SignatureDoesNotMatch|AccessDenied|InvalidAccessKeyId|ExpiredToken|InvalidToken' <<<"$cond" | sort -u | paste -sd, -)
[ -n "$autherr" ] || fail "pg-neg archiving fails, but not with an S3 auth error: $cond"
IFS='|' read -r archived failed _ last_failed <<<"$(archiver_stats pg-neg pg-1)"
[ "$archived" = 0 ] && [ "$failed" -gt 0 ] || fail "pg-neg archiver archived_count=$archived failed_count=$failed"
plugin_backup pg-neg backup-neg pg
wait_for "Backup backup-neg finished" 600 bash -c "[[ \$(kubectl -n pg-neg get backups.postgresql.cnpg.io backup-neg -o jsonpath='{.status.phase}') =~ ^(completed|failed)$ ]]"
[ "$(backup_phase pg-neg backup-neg)" = failed ] || fail "backup-neg phase=$(backup_phase pg-neg backup-neg), expected failed"
pass "negative control (no sessionToken): S3 error $autherr, archived_count=0 failed_count=$failed last_failed_wal=$last_failed, backup-neg failed"

kubectl get backupconfigs.cnpg-extensions.yandex.cloud -A -o wide || true

echo
echo "================ e2e summary ($IMAGE)"
docker run --rm --entrypoint /usr/local/bin/wal-g "$IMAGE" --version
printf '%s\n' "${RESULTS[@]}"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    {
        echo "### e2e: kind + CNPG + MinIO STS"
        echo
        echo '```'
        docker run --rm --entrypoint /usr/local/bin/wal-g "$IMAGE" --version
        printf '%s\n' "${RESULTS[@]}"
        echo '```'
    } >> "$GITHUB_STEP_SUMMARY"
fi
