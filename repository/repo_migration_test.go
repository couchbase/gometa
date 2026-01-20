package repository

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	c "github.com/couchbase/gometa/common"
)

var gMetadata map[string][]byte

func getOrLoadMetadata() map[string][]byte {
	if len(gMetadata) > 0 {
		return gMetadata
	}
	gMetadata = make(map[string][]byte)
	gMetadata["empty_key"] = []byte{}
	gMetadata["nil_key"] = nil

	for i := 0; i < 1000; i++ {
		gMetadata[fmt.Sprintf("string_%d", i)] = genData(rand.Int63n(1000))
	}
	return gMetadata
}

// seedForestDB writes the provided payload into a fresh ForestDB repository.
// Each RepoKind in payload is populated with its own set of key/value pairs.
func seedForestDB(t *testing.T, dir string, payload map[RepoKind]map[string][]byte) {
	t.Helper()

	fdbRepo := getOpenForestDBRepo(dir)
	defer fdbRepo.Close()

	for kind, kvs := range payload {
		for k, v := range kvs {
			if err := fdbRepo.Set(kind, k, v); err != nil {
				t.Fatalf(
					"failed to seed forestdb key %s for kind %v: %v",
					k, kind, err,
				)
			}
		}
	}
}

func getForestDBFiles(t *testing.T, dir string) []string {
	t.Helper()
	files := make([]string, 0)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("failed to list directory %s: %v", dir, err)
	}
	for _, ent := range entries {
		if strings.Contains(ent.Name(), c.FDB_REPOSITORY_NAME) {
			files = append(files, ent.Name())
		}
	}
	return files
}

func TestMagmaMigration_Simple(t *testing.T) {
	rand.Seed(time.Now().Unix())
	var metadata = getOrLoadMetadata()

	dir := t.TempDir()
	{
		seedForestDB(t, dir, map[RepoKind]map[string][]byte{
			MAIN:          metadata,
			SERVER_CONFIG: metadata,
			COMMIT_LOG:    metadata,
			LOCAL:         metadata,
		})
	}

	{
		magmaRepo := getOpenRepo(dir)
		verifyMigrationMarkerExists(t, dir)

		getRandomKind := func() RepoKind {
			kint := rand.Intn(4)
			switch kint {
			case 0:
				return MAIN
			case 1:
				return SERVER_CONFIG
			case 2:
				return COMMIT_LOG
			case 3:
				return LOCAL
			}
			return 0
		}

		for k, ev := range metadata {
			kind := getRandomKind()
			av, err := magmaRepo.Get(kind, k)
			handleError(
				t, err,
				fmt.Sprintf("failed to read error key %v from store %v", k, kind),
			)
			if !assertEqual(av, ev) {
				t.Errorf(
					"migration verification failed for key %v in store %v: fdb %v, magma %v",
					k, kind, av, ev,
				)
			}
		}

		magmaRepo.Close()

	}

}

func TestMagmaMigration_Empty(t *testing.T) {
	dir := t.TempDir()
	{
		fdbRepo := getOpenForestDBRepo(dir)
		fdbRepo.Close()
	}

	{
		magmaRepo := getOpenRepo(dir)
		verifyMigrationMarkerExists(t, dir)
		stats := magmaRepo.GetStoreStats()
		if stats.ItemsCount > 0 {
			t.Errorf(
				"%v items exist in magma store but forestDB store was empty before migration",
				stats.ItemsCount,
			)
		}
		magmaRepo.Close()
	}
}

func TestMagmaMigration_IdempotentWithMarker(t *testing.T) {
	dir := t.TempDir()

	payload := map[RepoKind]map[string][]byte{
		MAIN: {
			"main-key": []byte("main-val"),
		},
		COMMIT_LOG: {
			"commit-key": []byte("commit-val"),
		},
	}
	extraVal := []byte("magma-only-val")
	extraKey := "magma-only-key"
	markerPath := filepath.Join(dir, c.MAGMA_SUB_DIR, c.MAGMA_MIGRATION_MARKER)

	{

		seedForestDB(t, dir, payload)
	}

	var firstMarkerInfo os.FileInfo

	{
		// First open performs migration and writes marker.
		repo := getOpenRepo(dir)
		handleError(
			t,
			repo.Set(MAIN, extraKey, extraVal),
			"failed to set magma-only key after migration",
		)
		repo.Close()

		var err error

		firstMarkerInfo, err = os.Stat(markerPath)
		if err != nil {
			t.Fatalf("expected migration marker after first open: %v", err)
		}

		if len(getForestDBFiles(t, dir)) == 0 {
			t.Fatalf("expected forestdb files to exist before second open")
		}
	}

	{
		// Second open should skip migration, retain marker timestamp, and remove old forestdb files.
		repo := getOpenRepo(dir)
		defer repo.Close()

		verifyMigrationMarkerExists(t, dir)

		secondMarkerInfo, err := os.Stat(markerPath)
		if err != nil {
			t.Fatalf("marker missing after second open: %v", err)
		}
		if !firstMarkerInfo.ModTime().Equal(secondMarkerInfo.ModTime()) {
			t.Errorf(
				"marker timestamp changed on second open, migration should be skipped. "+
					"before=%v after=%v",
				firstMarkerInfo.ModTime(),
				secondMarkerInfo.ModTime(),
			)
		}

		for kind, kvs := range payload {
			for k, expected := range kvs {
				got, err := repo.Get(kind, k)
				handleError(
					t,
					err,
					fmt.Sprintf(
						"failed to read migrated key %s from kind %v after reopen",
						k,
						kind,
					),
				)
				if !assertEqual(got, expected) {
					t.Errorf(
						"value mismatch after reopen for %s in %v: expected %v, got %v",
						k, kind, expected, got,
					)
				}
			}
		}

		got, err := repo.Get(MAIN, extraKey)
		handleError(t, err, "failed to read magma-only key after reopen")
		if !assertEqual(got, extraVal) {
			t.Errorf("magma-only data lost after reopen: expected %v, got %v",
				extraVal, got)
		}
	}

	{
		// ForestDB files should be destroyed on second open once marker exists.
		if len(getForestDBFiles(t, dir)) > 0 {
			t.Errorf("forestdb files still exist after marker-driven reopen; expected destroy")
		}
	}
}

func TestMagmaMigration_NoForestDBCreatesMarker(t *testing.T) {
	dir := t.TempDir()

	repo := getOpenRepo(dir)
	defer repo.Close()

	verifyMigrationMarkerExists(t, dir)

	stats := repo.GetStoreStats()
	if stats.ItemsCount != 0 {
		t.Errorf("expected zero items without forestdb migration, found %d",
			stats.ItemsCount)
	}

	if len(getForestDBFiles(t, dir)) > 0 {
		t.Errorf("forestdb files should not be created when none existed")
	}
}

func TestMagmaMigration_RejectsNonMagmaStore(t *testing.T) {
	dir := t.TempDir()
	params := RepoFactoryParams{
		Dir:       dir,
		StoreType: FDbStoreType,
	}

	repo, err := OpenMagmaRepositoryAndUpgrade(params)
	if repo != nil || err == nil {
		t.Fatalf("expected OpenMagmaRepositoryAndUpgrade to fail for non-magma store")
	}

	storeErr, ok := err.(*StoreError)
	if !ok {
		t.Fatalf("expected StoreError, got %T: %v", err, err)
	}
	if storeErr.Code() != ErrNotSupported {
		t.Fatalf("expected ErrNotSupported, got %v", storeErr.Code())
	}

	// Ensure marker was not created as operation should fail immediately.
	markerPath := filepath.Join(dir, c.MAGMA_SUB_DIR, c.MAGMA_MIGRATION_MARKER)
	if _, statErr := os.Stat(markerPath); statErr == nil {
		t.Fatalf("migration marker should not exist when store type is unsupported")
	} else if !os.IsNotExist(statErr) {
		t.Fatalf("unexpected stat error for marker: %v", statErr)
	}
}

func TestMagmaMigration_CorruptForestDBFailsUpgrade(t *testing.T) {
	dir := t.TempDir()

	payload := map[RepoKind]map[string][]byte{
		MAIN: {
			"corrupt-key": []byte("corrupt-val"),
		},
	}
	var corrupted int

	{
		// load base data
		seedForestDB(t, dir, payload)
	}
	{
		// corrupt base data
		for _, fdbPath := range getForestDBFiles(t, dir) {
			if err := os.WriteFile(
				filepath.Join(dir, fdbPath),
				[]byte("corrupt-data"),
				0o666,
			); err != nil {
				t.Fatalf("failed to corrupt forestdb file %s: %v", fdbPath, err)
			}
			corrupted++
		}
		if corrupted == 0 {
			t.Fatalf("expected to find forestdb files to corrupt under %s", dir)
		}
	}

	{
		// open repo. this will fail as store is corrupted
		params := RepoFactoryParams{
			Dir:                        dir,
			MemoryQuota:                4 * 1024 * 1024,
			CompactionMinFileSize:      0,
			CompactionThresholdPercent: 30,
			CompactionTimerDur:         60000,
			EnableWAL:                  true,
			StoreType:                  MagmaStoreType,
		}

		repo, err := OpenMagmaRepositoryAndUpgrade(params)
		if repo != nil {
			defer repo.Close()
		}
		if err == nil {
			t.Fatalf("expected upgrade to fail for corrupt forestdb files")
		}
	}

	{
		// stale file continues to exist
		magmaPath := filepath.Join(dir, c.MAGMA_SUB_DIR)
		if _, statErr := os.Stat(magmaPath); statErr != nil {
			t.Fatalf(
				"expected magma metadata dir to exist despite failure: %v",
				statErr,
			)
		}
	}
}

// TestMagmaMigration_CopyAndCorruptWithFix - the test is aimed at testing failure/crash during
// first migration attempt followed by a restart which works in migrating the data
func TestMagmaMigration_CopyAndCorruptWithFix(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	rand.Seed(time.Now().Unix())
	var metadata = getOrLoadMetadata()

	{
		seedForestDB(t, dir1, map[RepoKind]map[string][]byte{
			MAIN: metadata,
		})
	}

	{
		// backup files from dir1 to dir2
		dir1f := os.DirFS(dir1)

		err := os.CopyFS(dir2, dir1f)
		handleError(t, err, fmt.Sprintf("copy failed from %v to %v", dir1, dir2))
	}

	{
		var corrupted int = 0
		// corrupt base data
		for _, fdbPath := range getForestDBFiles(t, dir1) {
			if err := os.WriteFile(
				filepath.Join(dir1, fdbPath),
				[]byte("corrupt-data"),
				0o666,
			); err != nil {
				t.Fatalf("failed to corrupt forestdb file %s: %v", fdbPath, err)
			}
			corrupted++
		}
		if corrupted == 0 {
			t.Fatalf("expected to find forestdb files to corrupt under %s", dir1)
		}
	}

	{
		// open repo. this will fail as store is corrupted
		params := RepoFactoryParams{
			Dir:                        dir1,
			MemoryQuota:                4 * 1024 * 1024,
			CompactionMinFileSize:      0,
			CompactionThresholdPercent: 30,
			CompactionTimerDur:         60000,
			EnableWAL:                  true,
			StoreType:                  MagmaStoreType,
		}

		repo, err := OpenMagmaRepositoryAndUpgrade(params)
		if repo != nil {
			defer repo.Close()
		}
		if err == nil {
			t.Fatalf("expected upgrade to fail for corrupt forestdb files")
		}

		// stale file continues to exist
		magmaPath := filepath.Join(dir1, c.MAGMA_SUB_DIR)
		if _, statErr := os.Stat(magmaPath); statErr != nil {
			t.Fatalf(
				"expected magma metadata dir to exist despite failure: %v",
				statErr,
			)
		}
	}

	{
		// correct the files
		for _, fdbPath := range getForestDBFiles(t, dir1) {
			dbfilePath := filepath.Join(dir2, fdbPath)
			cnt, err := os.ReadFile(dbfilePath)
			handleError(t,
				err,
				fmt.Sprintf("failed to read backup of %v from dir2", dbfilePath),
			)

			dbfilePath = filepath.Join(dir1, fdbPath)
			handleError(
				t,
				os.WriteFile(dbfilePath, cnt, 0x0777),
				fmt.Sprintf("failed to write backup to file %v", dbfilePath),
			)
		}

		{
			repo := getOpenRepo(dir1)
			defer repo.Close()
			if repo.GetStoreStats().ItemsCount != uint64(len(metadata)) {
				t.Fatalf("items post migration %v don't match items pre-migration %v",
					repo.GetStoreStats().ItemsCount, len(metadata))
			}
			verifyMigrationMarkerExists(t, dir1)
		}

	}
}
