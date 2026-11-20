# CloudNativePG WAL-G Backup Plugin

**Status:** EXPERIMENTAL

This plugin adds backup and restore functionality to [CloudNativePG](https://cloudnative-pg.io/) by leveraging [WAL-G](https://github.com/wal-g/wal-g). It communicates with CloudNativePG through the `cnpg-i` interface, enabling seamless integration for managing PostgreSQL backups and restores in Kubernetes environments.

## Features

#### Backup

- [x] Full and incremental backups creation
- [x] Continuous WAL archivation/restoration
- [x] Encryption with symmetric key via `libsodium`
- [ ] Encryption with asymmetric key via `GPG` (planned)

#### Restore

- [x] Restore to any `wal-g`-created backup
- [x] Point-in-time Recovery (PITR) supporting timestamp/transaction XID/LSN

#### Storage

- [x] S3-compatible storage support (AWS S3, MinIO)
- [x] Google Cloud Storage (GCS) support
- [ ] Azure (currently not planned, need community support)

#### Lifecycle

- [x] Retention and auto-removal of outdated backups and WALs
- [x] Marking Backups as persistent (protect from removing from storage, even if `Backup` or `BackupConfig` custom resource deleted)
- [x] Monitoring
- [x] `BackupConfig` Status displaying for last recoverability point, last successful backup, storage consumption

## Switchover WAL guard

During a switchover (`status.targetPrimary` differs from `status.currentPrimary`) the
plugin changes how it serves WAL segments of the cluster's current timeline
(`status.timelineID`, still the old primary's), to close the race of
[cloudnative-pg#11715](https://github.com/cloudnative-pg/cloudnative-pg/issues/11715),
where a replica restored the old primary's final segment from the archive and diverged
past the new timeline's fork:

- a replica that is not the target gets "not found" without an archive lookup, so it
  waits and streams from the new primary;
- the target primary polls the archive on "not found" (1 s apart, at most 5 s per
  switchover counted from its first miss), so it replays the old primary's final segment
  before it promotes. PostgreSQL cannot act on the promote request while the restore runs,
  so a promotion waits up to that long when the segment never comes (the common case of
  a switchover whose standbys streamed everything).

Timeline history, `.partial` and `.backup` files, replica clusters and full-recovery jobs
are not affected. Knobs:

| Where | Name | Default | Effect |
|---|---|---|---|
| Cluster `spec.plugins[].parameters` | `switchoverReplicaWALGuard` | `true` | `false` serves non-target replicas as before |
| Cluster `spec.plugins[].parameters` | `switchoverTargetWALRetry` | `true` | `false` answers the target primary at once, as before |
| sidecar env (e.g. Cluster `spec.env`) | `SWITCHOVER_TARGET_RETRY_TIMEOUT` | `5s` | polling budget per switchover; `0` disables it |
| sidecar env | `SWITCHOVER_TARGET_RETRY_INTERVAL` | `1s` | pause between polls |

Durations take Go syntax (`15s`, `500ms`); a bare number means seconds.

## Storage availability check

The controller's BackupConfig status loop (every 2 minutes) runs `wal-g st check read`
and `wal-g st check write` against each BackupConfig's storage and sets the conditions
`StorageReadable` / `StorageWritable`; the instance sidecar exports them as
`cnpg_plugin_wal_g_s3_read_availability` / `cnpg_plugin_wal_g_s3_write_availability`.

So that a storage latency blip does not flip them to `False` while WAL archiving is healthy:

- each call has a **60 s** budget (`--storage-check-timeout`, env `STORAGE_CHECK_TIMEOUT`
  of the controller; Go duration syntax);
- a failed check is **retried once, 5 s later**, before its condition goes `False`. The
  first failure is logged at Info (`Storage write check attempt 1/2 failed, retrying`),
  only the final one at Error (`Storage write check failed`). Both attempts stay within
  the 10-minute reconcile timeout;
- a check killed by its own deadline reports `storage write check timed out after 60s`
  instead of `signal: killed`.

A storage that fails both attempts gives the same result as before: the condition is
`False`, with the last error as its message, and the metric is `0`.

## Backup names

wal-g names a backup after the WAL segment it starts in (`base_<segment>`). A standby
starts a backup at its last restart point, which moves only when the primary writes a
checkpoint: two Backups from a standby of an idle database, with no checkpoint between
them, get the same name. The second one fails right after `pg_backup_start`, before it
uploads anything, with `backup base_<segment> already exists in storage`; the first one
stays as it was. Before `-maestra.2` of the bundled wal-g it overwrote the first one (under
an R2 bucket lock: added objects to it until a PutObject was refused). The first backup
plus the archived WAL covers everything since it; a Backup taken after the next
checkpoint (at most `checkpoint_timeout` later, if anything was written) succeeds.

## Deleting backups

A deleted `Backup` keeps the finalizer `cnpg-plugin-wal-g.yandex.cloud/backup-cleanup` until
its data is gone from storage, because retention works from `Backup` resources only: data
whose resource is gone is never pruned again. The finalizer is released without deleting
data only when:

- the Backup has no physical backup (`status.backupID` empty);
- the bucket does not exist (S3 `NoSuchBucket`);
- `retention.ignoreForBackupDeletion: true`;
- the annotation `cnpg-plugin-wal-g.yandex.cloud/skip-storage-cleanup: "true"` is on the
  Backup (or on its BackupConfig, while that exists).

Anything else keeps the finalizer and marks the Backup
`cnpg-plugin-wal-g.yandex.cloud/deletion-failing` (time of the first failure). The
BackupConfig gets condition `BackupDeletionFailing` and the sidecar exports
`cnpg_plugin_wal_g_backup_deletion_failing 1`; retries back off from 7 minutes to 1 hour.
A deletion refused by an R2 bucket lock is not a failure: `retained-by-bucket-lock`,
condition `BackupDeletionRetainedByLock`, metric `..._backup_deletion_retained_by_lock`.

| Condition reason | Cause | What to do |
|---|---|---|
| `DeletionFailing` | storage unreachable or refusing (403, 5xx, timeout, DNS, missing credentials) | fix the storage or its credentials; the retry deletes the data |
| `BackupConfigNotFound` | the Backup's BackupConfig was deleted first, or re-created as another object (new UID) | see below |

### The BackupConfig is gone

A BackupConfig deleted before its Backups (a namespace teardown, a delete by hand; the
owner reference then makes the garbage collector delete the Backups) leaves no storage
settings to delete with. The Backups stay in `Terminating` with the deletion-failing mark,
and every other BackupConfig of the namespace reports them under reason
`BackupConfigNotFound`. A BackupConfig re-created under the same name is another object and
does not delete them: deleting a BackupConfig never deletes backup data by itself.

A namespace being deleted stays `Terminating` with condition `NamespaceFinalizersRemaining`
naming the finalizer above, and nothing in it (no BackupConfig, no sidecar, no alert rule)
reports it any more. List what blocks it with
`kubectl get backups.postgresql.cnpg.io -n <ns> -o custom-columns=NAME:.metadata.name,ID:.status.backupID,OWNER:.metadata.ownerReferences[0].name`.

Then decide per Backup:

- keep the data (a restore point, a move to another namespace): annotate the Backup
  `kubectl annotate backups.postgresql.cnpg.io -n <ns> <name> cnpg-plugin-wal-g.yandex.cloud/skip-storage-cleanup=true`.
  The data stays in the bucket outside retention; delete it later with `wal-g delete target`
  or let the bucket lifecycle expire it;
- delete the data: run `wal-g delete target <backupID>` with the storage credentials, then
  annotate as above.

To tear a namespace down with its backups deleted, delete the Backups first and wait until
they are gone (`kubectl delete backups.postgresql.cnpg.io -n <ns> --all --wait`), then the
namespace. Under an R2 bucket lock, Backups younger than the lock wait until they are older.

## Dependencies

- [Kubernetes](https://kubernetes.io/releases/) version 1.28 or higher
- [CloudNative PG](https://cloudnative-pg.io/releases/) version 1.28 or higher
- [Cert-manager](https://cert-manager.io/docs/releases/) version 1.13 or higher

## Quickstart

1) Install latest stable `cloudnative-pg` release and `cert-manager` (later is needed to generate certificates for secure communication between CNPG and plugin)

    ```sh
    # Install CNPG
    kubectl apply --server-side -f \
    https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.28/releases/cnpg-1.28.0.yaml

    # Install cert-manager
    kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.18.2/cert-manager.yaml
    ```

2) Install latest plugin release into `cnpg-system` namespace (that should be the same namespace where `cloudnative-pg` installed)
- via Helm:
    ```sh
    helm -n cnpg-system upgrade --install cnpg-plugin-wal-g oci://ghcr.io/wal-g/cnpg-plugin-wal-g:0.3.0-helm-chart 
    ```
- or via static manifest
    ```sh
    kubectl apply -f https://raw.githubusercontent.com/wal-g/cnpg-plugin-wal-g/v0.3.0/dist/install.yaml
    ```

3) **Adjust** sample manifests from `config/samples/new-cluster` and apply
    ```sh
    kubectl apply -f ./config/samples/new-cluster
    ```

You should encounter new CNPG `Cluster` with encrypted WAL archivation and periodic auto-backups performed by plugin and `WAL-G`.

## Development

### Prerequisites
- go version v1.23.0+
- docker version 17.03+.
- kubectl version v1.11.3+.
- [kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation) version v0.27.0+ or access to existing K8s v1.11.3+ cluster.

### Run extension with Kind cluster

Bootstrap local k8s cluster with kind

```sh
./hack/kind/boostrap_kind_with_cnpg.sh
```

Build docker image locally. Base images, apt and Go modules come from nora (`packages.maestra.io`), so log in and export an `nra_` token first:
```sh
docker login packages.maestra.io -u token   # paste the nra_ token
read -rs PRIVATE_PACKAGES_TOKEN && export PRIVATE_PACKAGES_TOKEN   # paste the nra_ token again
make docker-build
```

Deploy the Manager to the cluster with the image specified by `IMG` (using deploy-kind, which injects `imagePullPolicy: Never`):
```sh
make deploy-kind
```

### Remove && cleanup Kind cluster

```sh
./hack/kind/cleanup_kind.sh
```

### Run extension with the external cluster
**Build and push your image to the location specified by `IMG`:**

```sh
make docker-build docker-push IMG=<some-registry>/cnpg-plugin-wal-g:tag
```

**NOTE:** This image ought to be published in the personal registry you specified.
And it is required to have access to pull the image from the working environment.
Make sure you have the proper permission to the registry if the above commands don’t work.

**Install the CRDs into the cluster:**

```sh
make install
```

**Deploy the Manager to the cluster with the image specified by `IMG`:**

```sh
make deploy IMG=<some-registry>/cnpg-plugin-wal-g:tag
```

> **NOTE**: If you encounter RBAC errors, you may need to grant yourself cluster-admin
privileges or be logged in as admin.

**Create instances of your solution**
You can apply the samples (examples) from the config/sample:

```sh
kubectl apply -f config/samples/new-cluster
```

>**NOTE**: Ensure that the samples has default values to test it out.

### To Uninstall
**Delete the instances (CRs) from the cluster:**

```sh
kubectl delete -f config/samples/new-cluster
```

**Delete the APIs(CRDs) from the cluster:**

```sh
make uninstall
```

**UnDeploy the controller from the cluster:**

```sh
make undeploy
```

### Releasing new version

```sh
# 1) Choose new tag
export GIT_TAG=v0.2.0
# 2) Make installer file (./dist/install.yaml) with new tag and commit
make build-installer && git add ./dist/install.yaml && git commit -m "Bump installer to version $GIT_TAG" && git push
# 3) Create new tag
git tag -m "Version $GIT_TAG" $GIT_TAG
# 4) Push new tag and commit
git push origin $GIT_TAG
# 5) Unset GIT_TAG variable
unset GIT_TAG
```

