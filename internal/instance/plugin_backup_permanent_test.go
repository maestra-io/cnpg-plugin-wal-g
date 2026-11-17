package instance

import (
	"testing"

	"github.com/wal-g/cnpg-plugin-wal-g/internal/common"
	"github.com/wal-g/cnpg-plugin-wal-g/pkg/walg"
)

func TestBackupResultMetadata(t *testing.T) {
	if got := backupResultMetadata(&walg.BackupMetadata{IsPermanent: true}); got[common.BackupPermanentMetadataKey] != "true" {
		t.Errorf("a backup pushed permanent must say so in pluginMetadata, got %v", got)
	}
	if got := backupResultMetadata(&walg.BackupMetadata{}); got != nil {
		t.Errorf("an ordinary backup carries no metadata, got %v", got)
	}
}
