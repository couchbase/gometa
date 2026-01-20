//go:build !community
// +build !community

package repository

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	c "github.com/couchbase/gometa/common"
	"github.com/couchbase/gometa/log"
	"github.com/couchbase/indexing/secondary/logging"
)

func init() {
	log.Current = &logging.SystemLogger
	logging.SetLogLevel(logging.Info)
}

func getMagmaRepo(params RepoFactoryParams) IRepository {
	repo, err := OpenMagmaRepositoryAndUpgrade(params)
	if err != nil {
		panic(err)
	}
	return repo
}

func getOpenRepo(path string) IRepository {
	params := RepoFactoryParams{
		Dir:                        path,
		MemoryQuota:                4 * 1024 * 1024,
		CompactionMinFileSize:      0,
		CompactionThresholdPercent: 30,
		CompactionTimerDur:         60000,
		EnableWAL:                  true,
		StoreType:                  MagmaStoreType,
	}

	return getMagmaRepo(params)
}

// verifyMagmaStoreError verifies that an error is a StoreError with MagmaStoreType
func verifyMagmaStoreError(t *testing.T, err error, operation string) *StoreError {
	if err == nil {
		return nil
	}
	storeErr, ok := err.(*StoreError)
	if !ok {
		t.Errorf("%s: expected StoreError, got %T: %v", operation, err, err)
		return nil
	}
	if storeErr.sType != MagmaStoreType {
		t.Errorf("%s: expected MagmaStoreType, got %s", operation, storeErr.sType)
	}
	return storeErr
}

func TestMagmaRepository_Set(t *testing.T) {
	dir := t.TempDir()
	repo := getOpenRepo(dir)
	verifyMigrationMarkerExists(t, dir)
	defer repo.Close()

	utilRepoSet(t, repo, repo.Set, t.Name())

	// Test that Set fails if we write after repo is closed
	repo.Close()
	err := repo.Set(MAIN, "key_after_close", []byte("val"))
	storeErr := verifyMagmaStoreError(t, err, "Set after Close")
	if storeErr != nil && storeErr.Code() != ErrRepoClosedCode {
		t.Errorf(
			"Set should fail after repo is closed, expected ErrRepoClosedCode, got: %v",
			storeErr.Code(),
		)
	}
}

func TestMagmaRepository_SetNoCommit(t *testing.T) {
	dir := t.TempDir()
	repo := getOpenRepo(dir)
	verifyMigrationMarkerExists(t, dir)
	defer repo.Close()

	utilRepoSet(t, repo, repo.SetNoCommit, t.Name())

	// Test that SetNoCommit fails if we write after repo is closed
	repo.Close()
	err := repo.SetNoCommit(MAIN, "key_after_close", []byte("val"))
	storeErr := verifyMagmaStoreError(t, err, "SetNoCommit after Close")
	if storeErr != nil && storeErr.Code() != ErrRepoClosedCode {
		t.Errorf(
			"SetNoCommit should fail after repo is closed, expected ErrRepoClosedCode, got: %v",
			storeErr.Code(),
		)
	}
}

func TestMagmaRepository_Delete(t *testing.T) {
	dir := t.TempDir()
	repo := getOpenRepo(dir)
	verifyMigrationMarkerExists(t, dir)
	defer repo.Close()

	utilRepoDelete(t, repo, repo.Set, repo.Delete, t.Name())

	// Ensure Delete fails after Close
	repo.Close()
	err := repo.Delete(MAIN, "key_after_close")
	storeErr := verifyMagmaStoreError(t, err, "Delete after Close")
	if storeErr != nil && storeErr.Code() != ErrRepoClosedCode {
		t.Errorf(
			"Delete should fail after repo is closed, expected ErrRepoClosedCode, got: %v",
			storeErr.Code(),
		)
	}
}

func TestMagmaRepository_DeleteNoCommit(t *testing.T) {
	dir := t.TempDir()
	repo := getOpenRepo(dir)
	verifyMigrationMarkerExists(t, dir)
	defer repo.Close()

	utilRepoDelete(t, repo, repo.Set, repo.DeleteNoCommit, t.Name())

	// Ensure DeleteNoCommit fails after Close
	repo.Close()
	err := repo.DeleteNoCommit(MAIN, "key_after_close")
	storeErr := verifyMagmaStoreError(t, err, "DeleteNoCommit after Close")
	if storeErr != nil && storeErr.Code() != ErrRepoClosedCode {
		t.Errorf(
			"DeleteNoCommit should fail after repo is closed, expected ErrRepoClosedCode, got: %v",
			storeErr.Code(),
		)
	}
}

func TestMagmaRepository_Get(t *testing.T) {
	dir := t.TempDir()
	repo := getOpenRepo(dir)
	verifyMigrationMarkerExists(t, dir)
	defer repo.Close()
	utilRepoGet(t, repo)
}

func TestMagmaRepository_EmptyValue(t *testing.T) {
	dir := t.TempDir()
	repo := getOpenRepo(dir)
	verifyMigrationMarkerExists(t, dir)
	defer repo.Close()

	// Test 1: Insert empty value using empty byte slice
	emptyKey1 := "empty_value_key1"
	emptyVal1 := []byte{}
	if err := repo.Set(MAIN, emptyKey1, emptyVal1); err != nil {
		t.Fatalf("Set failed for empty value key=%s: %v", emptyKey1, err)
	}
	res, err := repo.Get(MAIN, emptyKey1)
	if err != nil {
		t.Fatalf("Get failed for empty value key=%s: %v", emptyKey1, err)
	}
	// Verify that the retrieved value is empty (length 0)
	if len(res) != 0 {
		t.Errorf(
			"Get returned non-empty value for empty value key=%s: expected length 0, got length %d, value: %v",
			emptyKey1,
			len(res),
			res,
		)
	}
	// Also verify it's equal to empty byte slice
	if !reflect.DeepEqual(res, emptyVal1) {
		t.Errorf(
			"Get mismatch for empty value key=%s: expected %v, got %v",
			emptyKey1,
			emptyVal1,
			res,
		)
	}

	// Test 2: Insert empty value using nil
	emptyKey2 := "empty_value_key2"
	var emptyVal2 []byte = nil
	if err := repo.Set(MAIN, emptyKey2, emptyVal2); err != nil {
		t.Fatalf("Set failed for nil value key=%s: %v", emptyKey2, err)
	}
	res, err = repo.Get(MAIN, emptyKey2)
	if err != nil {
		t.Fatalf("Get failed for nil value key=%s: %v", emptyKey2, err)
	}
	// Verify that the retrieved value is empty (length 0)
	if len(res) != 0 {
		t.Errorf(
			"Get returned non-empty value for nil value key=%s: expected length 0, got length %d, value: %v",
			emptyKey2,
			len(res),
			res,
		)
	}

	// Test 3: Overwrite non-empty value with empty value
	overwriteKey := "overwrite_with_empty"
	nonEmptyVal := []byte("non_empty_value")
	if err := repo.Set(MAIN, overwriteKey, nonEmptyVal); err != nil {
		t.Fatalf("Set failed for non-empty value key=%s: %v", overwriteKey, err)
	}
	res, err = repo.Get(MAIN, overwriteKey)
	if err != nil {
		t.Fatalf("Get failed for non-empty value key=%s: %v", overwriteKey, err)
	}
	if !reflect.DeepEqual(res, nonEmptyVal) {
		t.Fatalf(
			"Get mismatch before overwrite for key=%s: expected %v, got %v",
			overwriteKey,
			nonEmptyVal,
			res,
		)
	}
	// Now overwrite with empty value
	if err := repo.Set(MAIN, overwriteKey, []byte{}); err != nil {
		t.Fatalf("Set failed for overwriting with empty value key=%s: %v", overwriteKey, err)
	}
	res, err = repo.Get(MAIN, overwriteKey)
	if err != nil {
		t.Fatalf("Get failed after overwrite with empty value key=%s: %v", overwriteKey, err)
	}
	if len(res) != 0 {
		t.Errorf(
			"Get returned non-empty value after overwrite with empty value key=%s: expected length 0, got length %d, value: %v",
			overwriteKey,
			len(res),
			res,
		)
	}
}

func TestMagmaRepository_Reopen(t *testing.T) {
	dir := t.TempDir()

	repo := getOpenRepo(dir)
	verifyMigrationMarkerExists(t, dir)
	utilRepoSet(t, repo, repo.Set, t.Name())
	itemCount := repo.(*Magma_Repository).getRepoStats(MAIN).TotalItemCount
	repo.Close()

	// verify repo closed
	_, err := repo.Get(MAIN, "maybe_key")
	storeErr := verifyMagmaStoreError(t, err, "Get after Close")
	if storeErr == nil || storeErr.Code() != ErrRepoClosedCode {
		t.Fatal("repo should have been closed but it is not")
	}

	repo = getOpenRepo(dir)
	verifyMigrationMarkerExists(t, dir)
	reopenItemsCount := repo.(*Magma_Repository).getRepoStats(MAIN).TotalItemCount
	if itemCount != reopenItemsCount {
		t.Errorf("item count mismatch on magma re-instantiation. Old - %v, New - %v",
			itemCount, reopenItemsCount)
	}
	repo.Close()
}

func verifyMigrationMarkerExists(t *testing.T, path string) {
	t.Helper()

	checkptFile := filepath.Join(path, c.MAGMA_SUB_DIR, c.MAGMA_MIGRATION_MARKER)
	if _, err := os.ReadFile(checkptFile); err != nil {
		t.Errorf("failed to read marker file with error %v", err)
	}
}

func verifyNoMigrationMarkerExists(t *testing.T, path string) {
	t.Helper()

	checkptFile := filepath.Join(path, c.MAGMA_SUB_DIR, c.MAGMA_MIGRATION_MARKER)
	if cnt, err := os.ReadFile(checkptFile); err != nil && !os.IsNotExist(err) {
		t.Errorf("failed to read marker file with error %v", err)
	} else if err == nil {
		t.Errorf("expected magma marker file to exist but it exists with content - %v",
			cnt)
	}
}
