package v1beta1

// Finalizer added to CNPG Backup resources managed by plugin
// It is used to ensure that backup removed from storage before Backup resource deleted
const BackupFinalizerName = "cnpg-plugin-wal-g.yandex.cloud/backup-cleanup"

// Labels added to CNPG Backup resources managed by plugin:

// PGVersionLabel represents PG major version from which backup was created
const BackupPgVersionLabelName = "cnpg-plugin-wal-g.yandex.cloud/pg-major"

// BackupTypeLabel represents type of created backup (full / incremental)
const BackupTypeLabelName = "cnpg-plugin-wal-g.yandex.cloud/backup-type"

// Annotations added to CNPG Backup resources managed by plugin:

// BackupAllDependentsAnnotation represents all incremental backups (both direct and indirect)
// depending on current backup
const BackupAllDependentsAnnotationName = "cnpg-plugin-wal-g.yandex.cloud/dependent-backups-all"

// BackupDirectDependentsAnnotation represents incremental backups which are created from current backup
const BackupDirectDependentsAnnotationName = "cnpg-plugin-wal-g.yandex.cloud/dependent-backups-direct"

// BackupType is string enumeration for backup types, can only be "full" or "incremental"
type BackupType string

const BackupTypeFull BackupType = "full"
const BackupTypeIncremental BackupType = "incremental"

// BackupRetainedByLockAnnotationName is set on a Backup being deleted while an R2 bucket
// lock refuses to delete it from storage. The value is the time of the first refusal.
// The finalizer stays and the deletion is retried until the lock age has passed.
const BackupRetainedByLockAnnotationName = "cnpg-plugin-wal-g.yandex.cloud/retained-by-bucket-lock"

// BackupRetainedUntilAnnotationName is set next to BackupRetainedByLockAnnotationName when
// the lock age is known (the bucket's R2BucketLock): the time the lock releases the newest
// object of the backup. The deletion is not retried before it.
const BackupRetainedUntilAnnotationName = "cnpg-plugin-wal-g.yandex.cloud/retained-by-bucket-lock-until"

// BackupDeletionFailingAnnotationName is set on a Backup being deleted while its deletion
// from storage fails for any reason other than a bucket lock (storage unreachable, 403,
// signature error, timeout, 5xx, missing credentials). The value is the time of the first
// failure. The finalizer stays and the deletion is retried: an outage of the storage must
// not turn a deletion into an orphaned backup.
const BackupDeletionFailingAnnotationName = "cnpg-plugin-wal-g.yandex.cloud/deletion-failing"

// SkipStorageCleanupAnnotationName, set to "true" on a Backup or on its BackupConfig,
// makes the plugin remove its finalizers without deleting anything from storage. It is
// the explicit escape hatch for a teardown whose storage will never be reachable again
// (credentials revoked for good, endpoint decommissioned). The data stays in the bucket.
const SkipStorageCleanupAnnotationName = "cnpg-plugin-wal-g.yandex.cloud/skip-storage-cleanup"
