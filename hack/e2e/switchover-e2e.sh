#!/usr/bin/env bash
# End-to-end test of the switchover WAL guard (internal/instance/switchover_guard.go),
# cloudnative-pg/cloudnative-pg#11715: a switchover whose old primary's fast shutdown
# stalls on a logical walsender, so neither standby streams the shutdown XLOG_SWITCH
# while the old primary archives the segment that holds it.
#
# Setup: kind + CNPG + cert-manager + MinIO + the plugin image; a 3-instance PG 18
# Cluster archiving through the plugin, wal_sender_timeout=5s, a failover logical
# slot (synchronizeLogicalDecoding) and a pgoutput consumer that, like Debezium,
# confirms flush only up to the last COMMIT it received, never the trailing
# non-transactional WAL (hack/e2e/cdc-consumer.py).
#
# Two switchovers (pg-1 -> pg-2, then pg-2 -> pg-3), each with the consumer
# attached. After each one it asserts:
#   - no instance logs "forked off current database system timeline" or
#     "is not in this server's history";
#   - both replicas stream from the new primary, on the new timeline;
#   - when the old primary's shutdown stalled and both standbys had lost their
#     stream before its shutdown checkpoint (the precondition of the incident;
#     the test cuts the streams itself, see STREAM_CUT_AFTER): the new timeline forks at a
#     WAL segment boundary, i.e. the target replayed the old primary's final
#     XLOG_SWITCH from the archive before promoting, and the non-target
#     replica's sidecar withheld old-timeline segments meanwhile.
#
# Needs: docker (IMAGE already built), kind, kubectl, helm, jq.
#   IMAGE=docker.io/local/cnpg-plugin-wal-g:e2e hack/e2e/switchover-e2e.sh
#   hack/e2e/switchover-e2e.sh diagnostics   # dump cluster state (CI runs it on failure)
set -euo pipefail

IMAGE=${IMAGE:-docker.io/local/cnpg-plugin-wal-g:e2e}
KIND_CLUSTER=${KIND_CLUSTER:-walg-e2e-switchover}
CNPG_MANIFEST=${CNPG_MANIFEST:-https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.30/releases/cnpg-1.30.1.yaml}
CERT_MANAGER_MANIFEST=${CERT_MANAGER_MANIFEST:-https://github.com/cert-manager/cert-manager/releases/download/v1.18.2/cert-manager.yaml}
MINIO_IMAGE=${MINIO_IMAGE:-docker.io/bitnamilegacy/minio:2025.4.22}
PG_IMAGE=${PG_IMAGE:-ghcr.io/cloudnative-pg/postgresql:18.4-system-trixie}
PLUGIN=cnpg-extensions.yandex.cloud
NS=pg-sw
BUCKET=walg-test
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin-e2e-pass
WALG_USER=walguser
WALG_PASSWORD=walguser-e2e-pass
CDC_PASSWORD=cdc-e2e-pass
# Seconds after the switchover starts at which the standbys' streams are cut.
STREAM_CUT_AFTER=${STREAM_CUT_AFTER:-6}
REPO_ROOT=$(cd "$(dirname "$0")/../.." && pwd)
OUT=${E2E_OUT:-/tmp/switchover-e2e}
mkdir -p "$OUT"

log()  { echo "[$(date -u '+%H:%M:%S')] $*"; }
pass() { echo "[$(date -u '+%H:%M:%S')] PASS: $*"; RESULTS+=("PASS: $*"); }
note() { echo "[$(date -u '+%H:%M:%S')] NOTE: $*"; RESULTS+=("NOTE: $*"); }
fail() { echo "[$(date -u '+%H:%M:%S')] FAIL: $*" >&2; exit 1; }
RESULTS=()

wait_for() { # wait_for <description> <timeout seconds> <command...>
    local desc=$1 timeout=$2; shift 2
    local deadline=$((SECONDS + timeout))
    until "$@" >/dev/null 2>&1; do
        if (( SECONDS >= deadline )); then fail "timed out after ${timeout}s waiting for: $desc"; fi
        sleep 3
    done
    log "ok: $desc"
}

diagnostics() {
    set +e
    kubectl get nodes -o wide
    kubectl get pods -A -o wide
    kubectl -n "$NS" get clusters.postgresql.cnpg.io -o yaml
    kubectl get events -A --sort-by=.lastTimestamp | tail -150
    kubectl -n cnpg-system logs deploy/cnpg-controller-manager --tail=400
    kubectl -n cnpg-system logs deploy/cnpg-plugin-wal-g-controller-manager --tail=200
    for p in $(kubectl -n "$NS" get pods -o name 2>/dev/null); do
        echo "=== $p"
        kubectl -n "$NS" describe "$p" | tail -40
        kubectl -n "$NS" logs "$p" --all-containers --tail=300
        kubectl -n "$NS" logs "$p" -c postgres --previous --tail=300 2>/dev/null
    done
}
if [ "${1:-}" = diagnostics ]; then diagnostics; exit 0; fi

psql_in() { # psql_in <pod> <sql> [db]
    kubectl -n "$NS" exec "$1" -c postgres -- psql -U postgres -d "${3:-postgres}" -qtAX -c "$2"
}

cluster_status() { kubectl -n "$NS" get clusters.postgresql.cnpg.io pg -o json | jq -r ".status.$1 // empty"; }

# pg_logs <pod> <since RFC3339>: the postgres container's log (current and, if the
# container restarted, the previous one) since the given time.
pg_logs() {
    { kubectl -n "$NS" logs "$1" -c postgres --previous --since-time="$2" 2>/dev/null || true
      kubectl -n "$NS" logs "$1" -c postgres --since-time="$2" 2>/dev/null || true; }
}
sidecar_logs() { kubectl -n "$NS" logs "$1" -c plugin-wal-g --since-time="$2" 2>/dev/null || true; }

# json_msgs: the PostgreSQL messages of a CNPG JSON log, "ts<TAB>app<TAB>message".
json_msgs() {
    jq -Rr 'fromjson? | select(.logger == "postgres") | .record as $r
        | [($r.log_time // .ts // ""), ($r.application_name // ""), ($r.message // "")] | @tsv'
}

# ---------------------------------------------------------------- infrastructure
log "kind cluster $KIND_CLUSTER, image $IMAGE"
kind create cluster --name "$KIND_CLUSTER" --wait 120s
kind load docker-image "$IMAGE" --name "$KIND_CLUSTER"
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

mc_in() { kubectl -n minio exec deploy/minio -- mc --config-dir /tmp/mc "$@"; }
minio_setup() {
    mc_in alias set local http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" &&
    mc_in mb --ignore-existing "local/$BUCKET" &&
    mc_in admin user add local "$WALG_USER" "$WALG_PASSWORD" &&
    { mc_in admin policy attach local readwrite --user "$WALG_USER" ||
      mc_in admin user info local "$WALG_USER" | grep -qw readwrite; }
}
wait_for "MinIO bucket + user" 120 minio_setup

helm upgrade --install cnpg-plugin-wal-g "$REPO_ROOT/chart" -n cnpg-system \
    --set image.repository="${IMAGE%:*}" --set image.tag="${IMAGE##*:}" --set image.pullPolicy=Never --wait --timeout 300s
kubectl -n cnpg-system rollout status deploy/cnpg-plugin-wal-g-controller-manager --timeout=300s

# ---------------------------------------------------------------- cluster + CDC consumer
kubectl create namespace "$NS"
kubectl -n "$NS" create secret generic walg-creds \
    --from-literal=access_key_id="$WALG_USER" --from-literal=secret_access_key="$WALG_PASSWORD"
kubectl -n "$NS" create secret generic walg-encryption --from-literal=libsodiumKey="$(openssl rand -hex 32)"
kubectl -n "$NS" create secret generic cdc-password --type=kubernetes.io/basic-auth \
    --from-literal=username=cdc --from-literal=password="$CDC_PASSWORD"
# The plugin's webhook may refuse connections for a few seconds after the rollout.
apply_backup_config() {
    kubectl apply -f - <<YAML
apiVersion: cnpg-extensions.yandex.cloud/v1beta1
kind: BackupConfig
metadata: {name: walg, namespace: $NS}
spec:
  storage:
    type: s3
    s3:
      prefix: s3://$BUCKET/$NS
      region: us-east-1
      endpointUrl: http://minio.minio.svc:9000
      forcePathStyle: true
      accessKeyId: {name: walg-creds, key: access_key_id}
      accessKeySecret: {name: walg-creds, key: secret_access_key}
  encryption:
    method: libsodium
    encryptionSecret: walg-encryption
YAML
}
wait_for "BackupConfig accepted by the plugin webhook" 180 apply_backup_config
kubectl apply -f - <<YAML
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata: {name: pg, namespace: $NS}
spec:
  instances: 3
  imageName: $PG_IMAGE
  imagePullPolicy: IfNotPresent
  storage: {size: 1Gi}
  primaryUpdateStrategy: unsupervised
  primaryUpdateMethod: switchover
  plugins:
  - name: $PLUGIN
    isWALArchiver: true
    parameters: {backupConfig: walg}
  replicationSlots:
    highAvailability: {enabled: true, synchronizeLogicalDecoding: true}
    synchronizeReplicas: {enabled: true}
  managed:
    roles:
    - {name: cdc, ensure: present, login: true, superuser: true, replication: true, passwordSecret: {name: cdc-password}}
  postgresql:
    parameters:
      wal_level: logical
      archive_timeout: 5min
      hot_standby_feedback: "on"
      sync_replication_slots: "on"
      wal_sender_timeout: 5s
      wal_receiver_timeout: 5s
      log_min_messages: info
YAML
kubectl -n "$NS" wait clusters.postgresql.cnpg.io/pg --for=condition=Ready --timeout=900s
wait_for "3 instances ready" 600 bash -c "[[ \$(kubectl -n $NS get clusters.postgresql.cnpg.io pg -o jsonpath='{.status.readyInstances}') == 3 ]]"
primary=$(cluster_status currentPrimary)
[ "$primary" = pg-1 ] || fail "expected pg-1 as the first primary, got '$primary'"
sidecar_image=$(kubectl -n "$NS" get pod pg-1 -o json | jq -r '.spec.initContainers[] | select(.name == "plugin-wal-g") | .image')
[ "$sidecar_image" = "$IMAGE" ] || fail "sidecar does not run $IMAGE ($sidecar_image)"

psql_in pg-1 "create table cdc_e2e(id bigserial primary key, v text); create publication cdc_pub for table cdc_e2e" app
psql_in pg-1 "select pg_create_logical_replication_slot('cdc_slot', 'pgoutput', false, false, true)" app >/dev/null
wait_for "archiving works" 300 bash -c "[[ \$(kubectl -n $NS get clusters.postgresql.cnpg.io pg -o jsonpath='{.status.conditions[?(@.type==\"ContinuousArchiving\")].status}') == True ]]"

kubectl -n "$NS" create configmap cdc-consumer --from-file=cdc-consumer.py="$REPO_ROOT/hack/e2e/cdc-consumer.py"
kubectl apply -f - <<YAML
apiVersion: apps/v1
kind: Deployment
metadata: {name: cdc-consumer, namespace: $NS}
spec:
  selector: {matchLabels: {app: cdc-consumer}}
  template:
    metadata: {labels: {app: cdc-consumer}}
    spec:
      containers:
      - name: consumer
        image: $PG_IMAGE
        imagePullPolicy: IfNotPresent
        command: [python3, -u, /scripts/cdc-consumer.py]
        env:
        - {name: PGHOST, value: pg-rw}
        - {name: PGDATABASE, value: app}
        - {name: PGUSER, value: cdc}
        - {name: PGPASSWORD, valueFrom: {secretKeyRef: {name: cdc-password, key: password}}}
        - {name: PGSSLMODE, value: require}
        volumeMounts: [{name: scripts, mountPath: /scripts}]
      volumes: [{name: scripts, configMap: {name: cdc-consumer}}]
YAML
kubectl -n "$NS" rollout status deploy/cdc-consumer --timeout=300s

# A light write load, so the consumer has commits to confirm.
writer() {
    while :; do
        kubectl -n "$NS" exec svc/pg-rw -c postgres -- psql -U postgres -d app -qtAX \
            -c "insert into cdc_e2e(v) values (now()::text)" >/dev/null 2>&1 || true
        sleep 1
    done
}
writer &
WRITER_PID=$!
trap 'kill $WRITER_PID 2>/dev/null || true' EXIT

consumer_attached() { # consumer_attached <primary>: the consumer streams from <primary>
    [[ $(psql_in "$1" "select count(*) from pg_stat_replication where application_name = 'cdc-e2e' and state = 'streaming'") == 1 ]]
}

# ---------------------------------------------------------------- switchovers
switchover() { # switchover <old primary> <target> <third replica>
    local old=$1 target=$2 third=$3
    wait_for "consumer attached to $old" 300 consumer_attached "$old"
    # Let the consumer confirm a commit and fall behind on the trailing WAL.
    sleep 3
    local old_tl since t0
    old_tl=$(cluster_status timelineID)
    since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    t0=$(date -u +%Y-%m-%dT%H:%M:%S.%6NZ)
    log "switchover $old -> $target (third replica $third), timeline $old_tl"
    # The consumer answers a few more seconds, then goes silent (see cdc-consumer.py).
    kubectl -n "$NS" exec deploy/cdc-consumer -- sh -c 'kill -USR1 1'
    # What 'kubectl cnpg promote' does.
    kubectl -n "$NS" patch clusters.postgresql.cnpg.io pg --subresource=status --type=merge -p \
        "{\"status\":{\"targetPrimary\":\"$target\",\"targetPrimaryTimestamp\":\"$t0\",\"phase\":\"Switchover in progress\",\"phaseReason\":\"Switching over to $target\"}}"

    # Cut both standbys' streams while the old primary's shutdown is stalled, as
    # wal_sender_timeout did in production (~1.5 x wal_sender_timeout after the
    # shutdown request; on kind the physical walsenders survive the stall). The old
    # primary refuses new connections while it shuts down, so neither standby gets
    # the shutdown XLOG_SWITCH by streaming.
    sleep "$STREAM_CUT_AFTER"
    local cut_at cut=0 pod
    cut_at=$(date -u '+%Y-%m-%d %H:%M:%S.%3N UTC')
    for pod in "$target" "$third"; do
        if kubectl -n "$NS" exec "$pod" -c postgres -- sh -c \
            'pid=$(psql -U postgres -qtAX -c "select pid from pg_stat_wal_receiver"); [ -n "$pid" ] && kill -TERM "$pid"'; then
            cut=$((cut + 1))
        fi
    done
    log "cut the WAL streams of $cut standbys at $cut_at"

    wait_for "$target is the current primary" 300 bash -c "[[ \$(kubectl -n $NS get clusters.postgresql.cnpg.io pg -o jsonpath='{.status.currentPrimary}') == $target ]]"
    local new_tl
    new_tl=$(psql_in "$target" "select timeline_id from pg_control_checkpoint()")
    (( new_tl > old_tl )) || fail "new primary $target is on timeline $new_tl, not past $old_tl"

    # Both replicas must stream from the new primary on the new timeline.
    replica_streams() { # replica_streams <pod>
        [[ $(psql_in "$1" "select status || ':' || received_tli from pg_stat_wal_receiver") == "streaming:$new_tl" ]] &&
        [[ $(psql_in "$target" "select count(*) from pg_stat_replication where application_name = '$1' and state = 'streaming'") == 1 ]]
    }
    wait_for "$third streams timeline $new_tl from $target" 180 replica_streams "$third"
    wait_for "$old streams timeline $new_tl from $target" 300 replica_streams "$old"
    # A diverged replica keeps retrying; give it time to say so.
    sleep 20

    local diverged=""
    for pod in pg-1 pg-2 pg-3; do
        pg_logs "$pod" "$since" > "$OUT/$pod-tl$new_tl.log"
        sidecar_logs "$pod" "$since" > "$OUT/$pod-tl$new_tl-sidecar.log"
        if grep -E "forked off current database system timeline|is not in this server's history" "$OUT/$pod-tl$new_tl.log"; then
            diverged+=" $pod"
        fi
    done
    [ -z "$diverged" ] || fail "timeline divergence after switchover to $target on:$diverged"
    pass "switchover $old -> $target: timeline $old_tl -> $new_tl, $third and $old stream from $target, no instance diverged"

    # Did the incident's precondition occur? The old primary's shutdown stalled on the
    # logical walsender and both physical walsenders were reaped before the
    # shutdown checkpoint.
    json_msgs < "$OUT/$old-tl$new_tl.log" > "$OUT/$old-tl$new_tl.msgs"
    local shut_req shut_ckpt reaped_phys reaped_cdc
    shut_req=$(awk -F'\t' '$3 ~ /received fast shutdown request/ {print $1; exit}' "$OUT/$old-tl$new_tl.msgs")
    shut_ckpt=$(awk -F'\t' '$3 ~ /checkpoint starting: shutdown/ {print $1; exit}' "$OUT/$old-tl$new_tl.msgs")
    reaped_phys=$(awk -F'\t' -v a="$target" -v b="$third" '$3 ~ /replication timeout/ && ($2 == a || $2 == b) {print $2}' "$OUT/$old-tl$new_tl.msgs" | sort -u | wc -l)
    reaped_cdc=$(awk -F'\t' '$3 ~ /replication timeout/ && $2 == "cdc-e2e"' "$OUT/$old-tl$new_tl.msgs" | wc -l)
    log "old primary $old: fast shutdown '$shut_req', shutdown checkpoint '$shut_ckpt', physical walsenders reaped=$reaped_phys, cdc walsender reaped=$reaped_cdc"

    # Where did the new timeline fork? The history file's last line: "<parent tli>\t<switchpoint>\t<reason>".
    local history switchpoint offset
    history=$(printf '%08X.history' "$new_tl")
    switchpoint=$(psql_in "$target" "select pg_read_file('pg_wal/$history')" | awk -F'\t' 'NF >= 2 {sp = $2} END {print sp}')
    offset=$(psql_in "$target" "select ('$switchpoint'::pg_lsn - '0/0'::pg_lsn)::numeric % (select setting::numeric from pg_settings where name = 'wal_segment_size')")
    log "timeline $new_tl forked at $switchpoint (offset $offset in its segment)"

    # The precondition: the shutdown stalled on the cdc walsender, and both streams
    # were gone before the shutdown checkpoint (so before the XLOG_SWITCH).
    if (( reaped_cdc > 0 && cut == 2 )) && [ -n "$shut_ckpt" ] && [[ "$cut_at" < "$shut_ckpt" ]]; then
        [ "$offset" = 0 ] || fail "stalled shutdown, streams reaped, and the fork $switchpoint is mid-segment: the target promoted without the old primary's final segment"
        grep -q "Switchover target primary: " "$OUT/$target-tl$new_tl-sidecar.log" ||
            grep -q '"switchoverGuard":"retry-on-miss"' "$OUT/$target-tl$new_tl-sidecar.log" ||
            fail "stall reproduced but the target's sidecar never took the switchover retry path"
        grep -q "Switchover in progress: not serving" "$OUT/$third-tl$new_tl-sidecar.log" ||
            note "$third never asked the archive for an old-timeline segment during the switchover"
        pass "stalled shutdown reproduced (cdc walsender reaped, streams cut at $cut_at, shutdown checkpoint at $shut_ckpt): the target replayed the old primary's final segment, fork at segment boundary $switchpoint"
        STALLS=$((STALLS + 1))
    else
        note "switchover to $target: shutdown did not reproduce the incident's precondition (cdc reaped=$reaped_cdc, streams cut=$cut at $cut_at, shutdown checkpoint '$shut_ckpt'); divergence checks above still apply"
    fi
    grep -h -E 'Switchover (in progress|target primary)' "$OUT/"*"-tl$new_tl-sidecar.log" | jq -Rr 'fromjson? | "\(.ts // "") \(.msg)"' | head -20 || true

    # How long the target's PostgreSQL took to act on CNPG's promote request: the
    # restore request in flight then is what the target retry can lengthen.
    local promote_delay
    promote_delay=$(python3 - "$OUT/$target-tl$new_tl.log" <<'PY'
import json, sys
from datetime import datetime
def ts(v):
    if isinstance(v, (int, float)):
        from datetime import timezone
        return datetime.fromtimestamp(v, timezone.utc)
    v = v.replace(" UTC", "+00:00").replace("Z", "+00:00")
    if "." in v:
        head, rest = v.split(".", 1)
        frac, tz = rest[:-6], rest[-6:]
        v = f"{head}.{frac[:6].ljust(6, '0')}{tz}"
    return datetime.fromisoformat(v.replace(" ", "T"))
call = done = None
def scan():
    global call, done
    for line in open(sys.argv[1]):
        handle(line)
def handle(line):
    global call, done
    try:
        e = json.loads(line)
    except ValueError:
        return
    if not isinstance(e, dict):
        return
    msg = e.get("msg", "")
    rec = e.get("record") or {}
    if call is None and "applying WALs and promoting" in msg:
        call = ts(e["ts"])
    if call is not None and done is None and "received promote request" in rec.get("message", ""):
        done = ts(rec.get("log_time") or e["ts"])
try:
    scan()
    print(f"{(done - call).total_seconds():.1f}s" if call and done else "n/a")
except Exception as err:
    print(f"n/a ({err.__class__.__name__})")
PY
)
    note "switchover to $target: PostgreSQL acted on the promote request $promote_delay after CNPG issued it"
}

STALLS=0
switchover pg-1 pg-2 pg-3
switchover pg-2 pg-3 pg-1
(( STALLS > 0 )) || fail "neither switchover reproduced the stalled shutdown; the test proves nothing about the race"

echo
echo "================ switchover e2e summary ($IMAGE)"
printf '%s\n' "${RESULTS[@]}"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    {
        echo "### e2e: switchover with a stalled CDC client"
        echo
        echo '```'
        printf '%s\n' "${RESULTS[@]}"
        echo '```'
    } >> "$GITHUB_STEP_SUMMARY"
fi
