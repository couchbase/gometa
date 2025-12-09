//go:build !community
// +build !community

package repository

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	c "github.com/couchbase/gometa/common"
	"github.com/couchbase/gometa/log"
	"github.com/couchbase/indexing/secondary/logging"
)

func init() {
	l = &logging.SystemLogger
	log.Current = l
	logging.SetLogLevel(logging.Debug)
}

// comparisonError records a difference between the two implementations
type comparisonError struct {
	operation string
	message   string
}

// testRepositories holds both repository implementations for comparison
type testRepositories struct {
	fdb   IRepository
	magma IRepository
}

// setupRepositories creates both FDB and Magma repositories for testing
func setupRepositories(t *testing.T) *testRepositories {
	dir := t.TempDir()

	// Create FDB repository
	fdbPath := filepath.Join(dir, "fdb_metadata.fdb")
	fdbRepo, err := OpenRepositoryWithName(fdbPath, 4*1024*1024*1024)
	if err != nil {
		t.Fatalf("Failed to open FDB repository: %v", err)
	}

	// Create Magma repository
	magmaPath := filepath.Join(dir, "magma_metadata")
	magmaRepo, err := OpenMagmaRepository(magmaPath)
	if err != nil {
		fdbRepo.Close()
		t.Fatalf("Failed to open Magma repository: %v", err)
	}

	return &testRepositories{
		fdb:   fdbRepo,
		magma: magmaRepo,
	}
}

// cleanup closes both repositories
func (tr *testRepositories) cleanup() {
	if tr.fdb != nil {
		tr.fdb.Close()
	}
	if tr.magma != nil {
		tr.magma.Close()
	}
}

// verifyStoreError verifies that an error is a StoreError with the expected store type
func verifyStoreError(
	operation string,
	err error,
	expectedType StoreType,
	storeName string,
) *comparisonError {
	if err == nil {
		return nil // No error to verify
	}

	storeErr, ok := err.(*StoreError)
	if !ok {
		return &comparisonError{
			operation: operation,
			message: fmt.Sprintf("%s returned non-StoreError - %v (%T)",
				storeName, err, err),
		}
	}

	// Access sType directly since we're in the same package
	if storeErr.sType != expectedType {
		return &comparisonError{
			operation: operation,
			message: fmt.Sprintf("%s StoreError has wrong store type - expected %s, got %s",
				storeName, expectedType, storeErr.sType),
		}
	}

	return nil
}

// compareErrors compares two errors and returns a comparisonError if they differ
func compareErrors(operation string, fErr, mErr error) *comparisonError {
	if fErr == nil && mErr == nil {
		return nil
	}

	// Verify both errors are StoreError with correct store types
	if err := verifyStoreError(operation, fErr, FDbStoreType, "FDB"); err != nil {
		return err
	}
	if err := verifyStoreError(operation, mErr, MagmaStoreType, "Magma"); err != nil {
		return err
	}

	fdbErr, _ := fErr.(*StoreError)
	magmaErr, _ := mErr.(*StoreError)

	if fdbErr == nil && magmaErr != nil {
		return &comparisonError{
			operation: operation,
			message:   fmt.Sprintf("FDB returned nil error, but Magma returned: %v", magmaErr),
		}
	}
	if fdbErr != nil && magmaErr == nil {
		return &comparisonError{
			operation: operation,
			message:   fmt.Sprintf("FDB returned error: %v, but Magma returned nil", fdbErr),
		}
	}

	if fdbErr.Code() != magmaErr.Code() {
		return &comparisonError{
			operation: operation,
			message: fmt.Sprintf("Error codes differ - FDB: %s, Magma: %s",
				fdbErr.Code().Error(), magmaErr.Code().Error()),
		}
	}

	return nil
}

// compareBytes compares two byte slices and returns a comparisonError if they differ
func compareBytes(operation string, fdbVal, magmaVal []byte) *comparisonError {
	if !bytes.Equal(fdbVal, magmaVal) {
		return &comparisonError{
			operation: operation,
			message: fmt.Sprintf(
				"Values differ - FDB length: %d, Magma length: %d, FDB: %v, Magma: %v",
				len(fdbVal),
				len(magmaVal),
				fdbVal,
				magmaVal,
			),
		}
	}
	return nil
}

// recordError records a comparison error in the test
func recordError(t *testing.T, err *comparisonError) {
	if err != nil {
		t.Errorf("[COMPARISON ERROR] Operation: %s - %s", err.operation, err.message)
	}
}

// TestRepositoryComparison_Set tests Set operation comparison
func TestRepositoryComparison_Set(t *testing.T) {
	tr := setupRepositories(t)
	defer tr.cleanup()

	testCases := []struct {
		kind  RepoKind
		key   string
		value []byte
	}{
		{MAIN, "key1", []byte("value1")},
		{MAIN, "key2", []byte("value2")},
		{COMMIT_LOG, "log1", []byte("logvalue1")},
		{SERVER_CONFIG, "config1", []byte("configvalue1")},
		{LOCAL, "local1", []byte("localvalue1")},
		{MAIN, "empty", []byte{}},
		{MAIN, "special-!@#", []byte("special value")},
		{MAIN, "key1", []byte("updated_value1")}, // Overwrite
	}

	var errors []*comparisonError

	for _, tc := range testCases {
		fdbErr := tr.fdb.Set(tc.kind, tc.key, tc.value)
		magmaErr := tr.magma.Set(tc.kind, tc.key, tc.value)

		if err := compareErrors(fmt.Sprintf("Set(%s, %s)", tc.kind, tc.key), fdbErr, magmaErr); err != nil {
			errors = append(errors, err)
		}
	}

	for _, err := range errors {
		recordError(t, err)
	}
}

// TestRepositoryComparison_Get tests Get operation comparison
func TestRepositoryComparison_Get(t *testing.T) {
	tr := setupRepositories(t)
	defer tr.cleanup()

	// First, set some values
	testData := []struct {
		kind  RepoKind
		key   string
		value []byte
	}{
		{MAIN, "get_key1", []byte("get_value1")},
		{MAIN, "get_key2", []byte("get_value2")},
		{COMMIT_LOG, "get_log1", []byte("get_logvalue1")},
		{MAIN, "get_empty", []byte{}},
	}

	// Set values in both repositories
	for _, td := range testData {
		if err := tr.fdb.Set(td.kind, td.key, td.value); err != nil {
			t.Fatalf("FDB Set failed: %v", err)
		}
		if err := tr.magma.Set(td.kind, td.key, td.value); err != nil {
			t.Fatalf("Magma Set failed: %v", err)
		}
	}

	var errors []*comparisonError

	// Test Get for existing keys
	for _, td := range testData {
		fdbVal, fdbErr := tr.fdb.Get(td.kind, td.key)
		magmaVal, magmaErr := tr.magma.Get(td.kind, td.key)

		if err := compareErrors(fmt.Sprintf("Get(%s, %s) error", td.kind, td.key), fdbErr, magmaErr); err != nil {
			errors = append(errors, err)
		}

		if fdbErr == nil && magmaErr == nil {
			if err := compareBytes(fmt.Sprintf("Get(%s, %s) value", td.kind, td.key), fdbVal, magmaVal); err != nil {
				errors = append(errors, err)
			}
		}
	}

	// Test Get for non-existent keys
	nonExistentKeys := []struct {
		kind RepoKind
		key  string
	}{
		{MAIN, "non_existent_key"},
		{COMMIT_LOG, "non_existent_log"},
	}

	for _, nek := range nonExistentKeys {
		fdbVal, fdbErr := tr.fdb.Get(nek.kind, nek.key)
		magmaVal, magmaErr := tr.magma.Get(nek.kind, nek.key)

		if err := compareErrors(fmt.Sprintf("Get(%s, %s) error for non-existent key", nek.kind, nek.key), fdbErr, magmaErr); err != nil {
			errors = append(errors, err)
		}

		if fdbErr == nil && magmaErr == nil {
			if err := compareBytes(fmt.Sprintf("Get(%s, %s) value for non-existent key", nek.kind, nek.key), fdbVal, magmaVal); err != nil {
				errors = append(errors, err)
			}
		}
	}

	for _, err := range errors {
		recordError(t, err)
	}
}

// TestRepositoryComparison_Delete tests Delete operation comparison
func TestRepositoryComparison_Delete(t *testing.T) {
	tr := setupRepositories(t)
	defer tr.cleanup()

	// Set up test data
	testData := []struct {
		kind  RepoKind
		key   string
		value []byte
	}{
		{MAIN, "delete_key1", []byte("delete_value1")},
		{MAIN, "delete_key2", []byte("delete_value2")},
		{COMMIT_LOG, "delete_log1", []byte("delete_logvalue1")},
	}

	// Set values in both repositories
	for _, td := range testData {
		if err := tr.fdb.Set(td.kind, td.key, td.value); err != nil {
			t.Fatalf("FDB Set failed: %v", err)
		}
		if err := tr.magma.Set(td.kind, td.key, td.value); err != nil {
			t.Fatalf("Magma Set failed: %v", err)
		}
	}

	var errors []*comparisonError

	// Test Delete for existing keys
	for _, td := range testData {
		fdbErr := tr.fdb.Delete(td.kind, td.key)
		magmaErr := tr.magma.Delete(td.kind, td.key)

		if err := compareErrors(fmt.Sprintf("Delete(%s, %s)", td.kind, td.key), fdbErr, magmaErr); err != nil {
			errors = append(errors, err)
		}

		// Verify deletion by trying to get the key
		fdbVal, fdbGetErr := tr.fdb.Get(td.kind, td.key)
		magmaVal, magmaGetErr := tr.magma.Get(td.kind, td.key)

		if err := compareErrors(fmt.Sprintf("Get after Delete(%s, %s) error", td.kind, td.key), fdbGetErr, magmaGetErr); err != nil {
			errors = append(errors, err)
		}

		if fdbGetErr == nil && magmaGetErr == nil {
			if err := compareBytes(fmt.Sprintf("Get after Delete(%s, %s) value", td.kind, td.key), fdbVal, magmaVal); err != nil {
				errors = append(errors, err)
			}
		}
	}

	// Test Delete for non-existent keys
	fdbErr := tr.fdb.Delete(MAIN, "non_existent_delete_key")
	magmaErr := tr.magma.Delete(MAIN, "non_existent_delete_key")

	if err := compareErrors("Delete(non-existent key)", fdbErr, magmaErr); err != nil {
		errors = append(errors, err)
	}

	for _, err := range errors {
		recordError(t, err)
	}
}

// TestRepositoryComparison_SetNoCommit tests SetNoCommit operation comparison
func TestRepositoryComparison_SetNoCommit(t *testing.T) {
	tr := setupRepositories(t)
	defer tr.cleanup()

	testCases := []struct {
		kind  RepoKind
		key   string
		value []byte
	}{
		{MAIN, "nocommit_key1", []byte("nocommit_value1")},
		{MAIN, "nocommit_key2", []byte("nocommit_value2")},
		{COMMIT_LOG, "nocommit_log1", []byte("nocommit_logvalue1")},
	}

	var errors []*comparisonError

	for _, tc := range testCases {
		fdbErr := tr.fdb.SetNoCommit(tc.kind, tc.key, tc.value)
		magmaErr := tr.magma.SetNoCommit(tc.kind, tc.key, tc.value)

		if err := compareErrors(fmt.Sprintf("SetNoCommit(%s, %s)", tc.kind, tc.key), fdbErr, magmaErr); err != nil {
			errors = append(errors, err)
		}
	}

	// Commit both and verify values are the same
	fdbCommitErr := tr.fdb.Commit()
	magmaCommitErr := tr.magma.Commit()

	if err := compareErrors("Commit after SetNoCommit", fdbCommitErr, magmaCommitErr); err != nil {
		errors = append(errors, err)
	}

	// Verify values after commit
	for _, tc := range testCases {
		fdbVal, fdbErr := tr.fdb.Get(tc.kind, tc.key)
		magmaVal, magmaErr := tr.magma.Get(tc.kind, tc.key)

		if err := compareErrors(fmt.Sprintf("Get after SetNoCommit+Commit(%s, %s) error", tc.kind, tc.key), fdbErr, magmaErr); err != nil {
			errors = append(errors, err)
		}

		if fdbErr == nil && magmaErr == nil {
			if err := compareBytes(fmt.Sprintf("Get after SetNoCommit+Commit(%s, %s) value", tc.kind, tc.key), fdbVal, magmaVal); err != nil {
				errors = append(errors, err)
			}
		}
	}

	for _, err := range errors {
		recordError(t, err)
	}
}

// TestRepositoryComparison_DeleteNoCommit tests DeleteNoCommit operation comparison
func TestRepositoryComparison_DeleteNoCommit(t *testing.T) {
	tr := setupRepositories(t)
	defer tr.cleanup()

	// Set up test data
	testData := []struct {
		kind  RepoKind
		key   string
		value []byte
	}{
		{MAIN, "delete_nocommit_key1", []byte("delete_nocommit_value1")},
		{MAIN, "delete_nocommit_key2", []byte("delete_nocommit_value2")},
	}

	// Set values in both repositories
	for _, td := range testData {
		if err := tr.fdb.Set(td.kind, td.key, td.value); err != nil {
			t.Fatalf("FDB Set failed: %v", err)
		}
		if err := tr.magma.Set(td.kind, td.key, td.value); err != nil {
			t.Fatalf("Magma Set failed: %v", err)
		}
	}

	var errors []*comparisonError

	// Test DeleteNoCommit
	for _, td := range testData {
		fdbErr := tr.fdb.DeleteNoCommit(td.kind, td.key)
		magmaErr := tr.magma.DeleteNoCommit(td.kind, td.key)

		if err := compareErrors(fmt.Sprintf("DeleteNoCommit(%s, %s)", td.kind, td.key), fdbErr, magmaErr); err != nil {
			errors = append(errors, err)
		}
	}

	// Commit both
	fdbCommitErr := tr.fdb.Commit()
	magmaCommitErr := tr.magma.Commit()

	if err := compareErrors("Commit after DeleteNoCommit", fdbCommitErr, magmaCommitErr); err != nil {
		errors = append(errors, err)
	}

	// Verify deletion after commit
	for _, td := range testData {
		fdbVal, fdbErr := tr.fdb.Get(td.kind, td.key)
		magmaVal, magmaErr := tr.magma.Get(td.kind, td.key)

		if err := compareErrors(fmt.Sprintf("Get after DeleteNoCommit+Commit(%s, %s) error", td.kind, td.key), fdbErr, magmaErr); err != nil {
			errors = append(errors, err)
		}

		if fdbErr == nil && magmaErr == nil {
			if err := compareBytes(fmt.Sprintf("Get after DeleteNoCommit+Commit(%s, %s) value", td.kind, td.key), fdbVal, magmaVal); err != nil {
				errors = append(errors, err)
			}
		}
	}

	for _, err := range errors {
		recordError(t, err)
	}
}

// TestRepositoryComparison_Commit tests Commit operation comparison
func TestRepositoryComparison_Commit(t *testing.T) {
	tr := setupRepositories(t)
	defer tr.cleanup()

	// Set up data with SetNoCommit
	testData := []struct {
		kind  RepoKind
		key   string
		value []byte
	}{
		{MAIN, "commit_key1", []byte("commit_value1")},
		{MAIN, "commit_key2", []byte("commit_value2")},
	}

	for _, td := range testData {
		if err := tr.fdb.SetNoCommit(td.kind, td.key, td.value); err != nil {
			t.Fatalf("FDB SetNoCommit failed: %v", err)
		}
		if err := tr.magma.SetNoCommit(td.kind, td.key, td.value); err != nil {
			t.Fatalf("Magma SetNoCommit failed: %v", err)
		}
	}

	var errors []*comparisonError

	// Test Commit
	fdbErr := tr.fdb.Commit()
	magmaErr := tr.magma.Commit()

	if err := compareErrors("Commit", fdbErr, magmaErr); err != nil {
		errors = append(errors, err)
	}

	// Verify values are persisted after commit
	for _, td := range testData {
		fdbVal, fdbGetErr := tr.fdb.Get(td.kind, td.key)
		magmaVal, magmaGetErr := tr.magma.Get(td.kind, td.key)

		if err := compareErrors(fmt.Sprintf("Get after Commit(%s, %s) error", td.kind, td.key), fdbGetErr, magmaGetErr); err != nil {
			errors = append(errors, err)
		}

		if fdbGetErr == nil && magmaGetErr == nil {
			if err := compareBytes(fmt.Sprintf("Get after Commit(%s, %s) value", td.kind, td.key), fdbVal, magmaVal); err != nil {
				errors = append(errors, err)
			}
		}
	}

	for _, err := range errors {
		recordError(t, err)
	}
}

// TestRepositoryComparison_CreateSnapshot tests CreateSnapshot operation comparison
func TestRepositoryComparison_CreateSnapshot(t *testing.T) {
	tr := setupRepositories(t)
	defer tr.cleanup()

	// Set up some data
	testData := []struct {
		kind  RepoKind
		key   string
		value []byte
	}{
		{MAIN, "snapshot_key1", []byte("snapshot_value1")},
		{MAIN, "snapshot_key2", []byte("snapshot_value2")},
	}

	for _, td := range testData {
		if err := tr.fdb.Set(td.kind, td.key, td.value); err != nil {
			t.Fatalf("FDB Set failed: %v", err)
		}
		if err := tr.magma.Set(td.kind, td.key, td.value); err != nil {
			t.Fatalf("Magma Set failed: %v", err)
		}
	}

	var errors []*comparisonError

	// Test CreateSnapshot
	testTxnID := c.Txnid(1)
	fdbErr := tr.fdb.CreateSnapshot(MAIN, testTxnID)
	magmaErr := tr.magma.CreateSnapshot(MAIN, testTxnID)

	if err := compareErrors("CreateSnapshot", fdbErr, magmaErr); err != nil {
		errors = append(errors, err)
	}

	// Test multiple snapshots
	testTxnID2 := c.Txnid(2)
	fdbErr2 := tr.fdb.CreateSnapshot(MAIN, testTxnID2)
	magmaErr2 := tr.magma.CreateSnapshot(MAIN, testTxnID2)

	if err := compareErrors("CreateSnapshot (second)", fdbErr2, magmaErr2); err != nil {
		errors = append(errors, err)
	}

	for _, err := range errors {
		recordError(t, err)
	}
}

// TestRepositoryComparison_AcquireSnapshot tests AcquireSnapshot operation comparison
func TestRepositoryComparison_AcquireSnapshot(t *testing.T) {
	tr := setupRepositories(t)
	defer tr.cleanup()

	// Set up some data
	testData := []struct {
		kind  RepoKind
		key   string
		value []byte
	}{
		{MAIN, "acquire_key1", []byte("acquire_value1")},
		{MAIN, "acquire_key2", []byte("acquire_value2")},
	}

	for _, td := range testData {
		if err := tr.fdb.Set(td.kind, td.key, td.value); err != nil {
			t.Fatalf("FDB Set failed: %v", err)
		}
		if err := tr.magma.Set(td.kind, td.key, td.value); err != nil {
			t.Fatalf("Magma Set failed: %v", err)
		}
	}

	// Create snapshot
	testTxnID := c.Txnid(1)
	if err := tr.fdb.CreateSnapshot(MAIN, testTxnID); err != nil {
		t.Fatalf("FDB CreateSnapshot failed: %v", err)
	}
	if err := tr.magma.CreateSnapshot(MAIN, testTxnID); err != nil {
		t.Fatalf("Magma CreateSnapshot failed: %v", err)
	}

	var errors []*comparisonError

	// Test AcquireSnapshot
	fdbTxnID, fdbIter, fdbErr := tr.fdb.AcquireSnapshot(MAIN)
	magmaTxnID, magmaIter, magmaErr := tr.magma.AcquireSnapshot(MAIN)

	if err := compareErrors("AcquireSnapshot error", fdbErr, magmaErr); err != nil {
		errors = append(errors, err)
	}

	// Compare transaction IDs
	if fdbTxnID != magmaTxnID {
		errors = append(errors, &comparisonError{
			operation: "AcquireSnapshot txnid",
			message: fmt.Sprintf(
				"Transaction IDs differ - FDB: %d, Magma: %d",
				fdbTxnID,
				magmaTxnID,
			),
		})
	}

	// Compare iterators (both should be non-nil or both nil)
	if (fdbIter == nil) != (magmaIter == nil) {
		errors = append(errors, &comparisonError{
			operation: "AcquireSnapshot iterator",
			message: fmt.Sprintf(
				"Iterator nil status differs - FDB: %v, Magma: %v",
				fdbIter == nil,
				magmaIter == nil,
			),
		})
	}

	// If both iterators are non-nil, compare their output
	if fdbIter != nil && magmaIter != nil {
		// Collect all key-value pairs from both iterators
		fdbResults := make(map[string][]byte)
		magmaResults := make(map[string][]byte)

		for {
			key, val, err := fdbIter.Next()
			if err != nil {
				break
			}
			fdbResults[key] = val
		}

		for {
			key, val, err := magmaIter.Next()
			if err != nil {
				break
			}
			magmaResults[key] = val
		}

		// Compare results
		if len(fdbResults) != len(magmaResults) {
			errors = append(errors, &comparisonError{
				operation: "AcquireSnapshot iterator count",
				message: fmt.Sprintf(
					"Iterator result count differs - FDB: %d, Magma: %d",
					len(fdbResults),
					len(magmaResults),
				),
			})
		}

		for key, fdbVal := range fdbResults {
			magmaVal, exists := magmaResults[key]
			if !exists {
				errors = append(errors, &comparisonError{
					operation: "AcquireSnapshot iterator key",
					message:   fmt.Sprintf("Key %s exists in FDB but not in Magma", key),
				})
			} else if err := compareBytes(fmt.Sprintf("AcquireSnapshot iterator key %s", key), fdbVal, magmaVal); err != nil {
				errors = append(errors, err)
			}
		}

		for key := range magmaResults {
			if _, exists := fdbResults[key]; !exists {
				errors = append(errors, &comparisonError{
					operation: "AcquireSnapshot iterator key",
					message:   fmt.Sprintf("Key %s exists in Magma but not in FDB", key),
				})
			}
		}

		fdbIter.Close()
		magmaIter.Close()
	}

	for _, err := range errors {
		recordError(t, err)
	}
}

// TestRepositoryComparison_ReleaseSnapshot tests ReleaseSnapshot operation comparison
func TestRepositoryComparison_ReleaseSnapshot(t *testing.T) {
	tr := setupRepositories(t)
	defer tr.cleanup()

	// Set up some data
	if err := tr.fdb.Set(MAIN, "release_key1", []byte("release_value1")); err != nil {
		t.Fatalf("FDB Set failed: %v", err)
	}
	if err := tr.magma.Set(MAIN, "release_key1", []byte("release_value1")); err != nil {
		t.Fatalf("Magma Set failed: %v", err)
	}

	// Create snapshot
	testTxnID := c.Txnid(1)
	if err := tr.fdb.CreateSnapshot(MAIN, testTxnID); err != nil {
		t.Fatalf("FDB CreateSnapshot failed: %v", err)
	}
	if err := tr.magma.CreateSnapshot(MAIN, testTxnID); err != nil {
		t.Fatalf("Magma CreateSnapshot failed: %v", err)
	}

	// Acquire snapshot
	_, fdbIter, _ := tr.fdb.AcquireSnapshot(MAIN)
	_, magmaIter, _ := tr.magma.AcquireSnapshot(MAIN)

	// Release snapshot (this should not return errors, but we compare behavior)
	tr.fdb.ReleaseSnapshot(MAIN, testTxnID)
	tr.magma.ReleaseSnapshot(MAIN, testTxnID)

	// Close iterators
	if fdbIter != nil {
		fdbIter.Close()
	}
	if magmaIter != nil {
		magmaIter.Close()
	}

	// This test mainly ensures both implementations handle ReleaseSnapshot without panicking
	// There's no return value to compare, so we just verify it completes
}

// TestRepositoryComparison_NewIterator tests NewIterator operation comparison
func TestRepositoryComparison_NewIterator(t *testing.T) {
	tr := setupRepositories(t)
	defer tr.cleanup()

	// Set up test data with known keys for iteration
	testData := []struct {
		kind  RepoKind
		key   string
		value []byte
	}{
		{MAIN, "iter_key1", []byte("iter_value1")},
		{MAIN, "iter_key2", []byte("iter_value2")},
		{MAIN, "iter_key3", []byte("iter_value3")},
		{MAIN, "a_key", []byte("a_value")},
		{MAIN, "z_key", []byte("z_value")},
	}

	for _, td := range testData {
		if err := tr.fdb.Set(td.kind, td.key, td.value); err != nil {
			t.Fatalf("FDB Set failed: %v", err)
		}
		if err := tr.magma.Set(td.kind, td.key, td.value); err != nil {
			t.Fatalf("Magma Set failed: %v", err)
		}
	}

	var errors []*comparisonError

	// Test NewIterator with empty start and end keys (all keys)
	fdbIter, fdbErr := tr.fdb.NewIterator(MAIN, "", "")
	magmaIter, magmaErr := tr.magma.NewIterator(MAIN, "", "")

	if err := compareErrors("NewIterator(empty, empty) error", fdbErr, magmaErr); err != nil {
		errors = append(errors, err)
	}

	if fdbErr == nil && magmaErr == nil {
		if (fdbIter == nil) != (magmaIter == nil) {
			errors = append(errors, &comparisonError{
				operation: "NewIterator iterator nil status",
				message: fmt.Sprintf(
					"Iterator nil status differs - FDB: %v, Magma: %v",
					fdbIter == nil,
					magmaIter == nil,
				),
			})
		}

		if fdbIter != nil && magmaIter != nil {
			// Compare iterator results
			fdbResults := make(map[string][]byte)
			magmaResults := make(map[string][]byte)

			for {
				key, val, err := fdbIter.Next()
				if err != nil {
					break
				}
				fdbResults[key] = val
			}

			for {
				key, val, err := magmaIter.Next()
				if err != nil {
					break
				}
				magmaResults[key] = val
			}

			// Compare results
			if len(fdbResults) != len(magmaResults) {
				errors = append(errors, &comparisonError{
					operation: "NewIterator result count",
					message: fmt.Sprintf(
						"Iterator result count differs - FDB: %d, Magma: %d",
						len(fdbResults),
						len(magmaResults),
					),
				})
			}

			for key, fdbVal := range fdbResults {
				magmaVal, exists := magmaResults[key]
				if !exists {
					errors = append(errors, &comparisonError{
						operation: "NewIterator key",
						message:   fmt.Sprintf("Key %s exists in FDB but not in Magma", key),
					})
				} else if err := compareBytes(fmt.Sprintf("NewIterator key %s", key), fdbVal, magmaVal); err != nil {
					errors = append(errors, err)
				}
			}

			for key := range magmaResults {
				if _, exists := fdbResults[key]; !exists {
					errors = append(errors, &comparisonError{
						operation: "NewIterator key",
						message:   fmt.Sprintf("Key %s exists in Magma but not in FDB", key),
					})
				}
			}

			fdbIter.Close()
			magmaIter.Close()
		}
	}

	// Test NewIterator with range
	fdbIter2, fdbErr2 := tr.fdb.NewIterator(MAIN, "iter_key1", "iter_key2")
	magmaIter2, magmaErr2 := tr.magma.NewIterator(MAIN, "iter_key1", "iter_key2")

	if err := compareErrors("NewIterator(range) error", fdbErr2, magmaErr2); err != nil {
		errors = append(errors, err)
	}

	if fdbErr2 == nil && magmaErr2 == nil && fdbIter2 != nil && magmaIter2 != nil {
		fdbResults2 := make(map[string][]byte)
		magmaResults2 := make(map[string][]byte)

		for {
			key, val, err := fdbIter2.Next()
			if err != nil {
				break
			}
			fdbResults2[key] = val
		}

		for {
			key, val, err := magmaIter2.Next()
			if err != nil {
				break
			}
			magmaResults2[key] = val
		}

		if len(fdbResults2) != len(magmaResults2) {
			errors = append(errors, &comparisonError{
				operation: "NewIterator(range) result count",
				message: fmt.Sprintf(
					"Iterator result count differs - FDB: %d, Magma: %d",
					len(fdbResults2),
					len(magmaResults2),
				),
			})
		}

		for key, fdbVal := range fdbResults2 {
			magmaVal, exists := magmaResults2[key]
			if !exists {
				errors = append(errors, &comparisonError{
					operation: "NewIterator(range) key",
					message:   fmt.Sprintf("Key %s exists in FDB but not in Magma", key),
				})
			} else if err := compareBytes(fmt.Sprintf("NewIterator(range) key %s", key), fdbVal, magmaVal); err != nil {
				errors = append(errors, err)
			}
		}

		fdbIter2.Close()
		magmaIter2.Close()
	}

	for _, err := range errors {
		recordError(t, err)
	}
}

// TestRepositoryComparison_IteratorNext tests IRepoIterator Next() method comparison
func TestRepositoryComparison_IteratorNext(t *testing.T) {
	tr := setupRepositories(t)
	defer tr.cleanup()

	// Set up test data
	testData := []struct {
		kind  RepoKind
		key   string
		value []byte
	}{
		{MAIN, "next_key1", []byte("next_value1")},
		{MAIN, "next_key2", []byte("next_value2")},
		{MAIN, "next_key3", []byte("next_value3")},
	}

	for _, td := range testData {
		if err := tr.fdb.Set(td.kind, td.key, td.value); err != nil {
			t.Fatalf("FDB Set failed: %v", err)
		}
		if err := tr.magma.Set(td.kind, td.key, td.value); err != nil {
			t.Fatalf("Magma Set failed: %v", err)
		}
	}

	var errors []*comparisonError

	// Create iterators
	fdbIter, fdbErr := tr.fdb.NewIterator(MAIN, "", "")
	magmaIter, magmaErr := tr.magma.NewIterator(MAIN, "", "")

	if fdbErr != nil || magmaErr != nil {
		t.Fatalf("Failed to create iterators - FDB: %v, Magma: %v", fdbErr, magmaErr)
	}

	// Compare Next() results
	for {
		fdbKey, fdbVal, fdbErr := fdbIter.Next()
		magmaKey, magmaVal, magmaErr := magmaIter.Next()

		// Verify StoreError types if errors are returned
		if fdbErr != nil {
			if err := verifyStoreError("Iterator Next() FDB", fdbErr, FDbStoreType, "FDB"); err != nil {
				errors = append(errors, err)
			}
		}
		if magmaErr != nil {
			if err := verifyStoreError("Iterator Next() Magma", magmaErr, MagmaStoreType, "Magma"); err != nil {
				errors = append(errors, err)
			}
		}

		// If both return errors, check if they're the same
		if fdbErr != nil && magmaErr != nil {
			// Compare error codes
			fdbStoreErr, fOk := fdbErr.(*StoreError)
			magmaStoreErr, mOk := magmaErr.(*StoreError)
			if fOk && mOk {
				if fdbStoreErr.storeCode != magmaStoreErr.storeCode {
					errors = append(errors, &comparisonError{
						operation: "Iterator Next() error code",
						message: fmt.Sprintf(
							"Error codes differ - FDB: %v, Magma: %v",
							fdbStoreErr.storeCode,
							magmaStoreErr.storeCode,
						),
					})
				}
			} else if fdbErr.Error() != magmaErr.Error() {
				errors = append(errors, &comparisonError{
					operation: "Iterator Next() error",
					message:   fmt.Sprintf("Error messages differ - FDB: %v, Magma: %v", fdbErr, magmaErr),
				})
			}
			break
		}

		// If one returns error and the other doesn't, that's a mismatch
		if fdbErr != nil || magmaErr != nil {
			errors = append(errors, &comparisonError{
				operation: "Iterator Next() error status",
				message: fmt.Sprintf(
					"Error status differs - FDB error: %v, Magma error: %v",
					fdbErr,
					magmaErr,
				),
			})
			break
		}

		// Compare keys and values
		if fdbKey != magmaKey {
			errors = append(errors, &comparisonError{
				operation: "Iterator Next() key",
				message:   fmt.Sprintf("Keys differ - FDB: %s, Magma: %s", fdbKey, magmaKey),
			})
		}

		if err := compareBytes(fmt.Sprintf("Iterator Next() value for key %s", fdbKey), fdbVal, magmaVal); err != nil {
			errors = append(errors, err)
		}
	}

	fdbIter.Close()
	magmaIter.Close()

	for _, err := range errors {
		recordError(t, err)
	}
}

// TestRepositoryComparison_Close tests Close operation comparison
func TestRepositoryComparison_Close(t *testing.T) {
	tr := setupRepositories(t)

	// Set up some data
	if err := tr.fdb.Set(MAIN, "close_key1", []byte("close_value1")); err != nil {
		t.Fatalf("FDB Set failed: %v", err)
	}
	if err := tr.magma.Set(MAIN, "close_key1", []byte("close_value1")); err != nil {
		t.Fatalf("Magma Set failed: %v", err)
	}

	// Close both repositories
	tr.fdb.Close()
	tr.magma.Close()

	// After close, operations should fail with ErrRepoClosed
	var errors []*comparisonError

	_, fdbErr := tr.fdb.Get(MAIN, "close_key1")
	_, magmaErr := tr.magma.Get(MAIN, "close_key1")

	// Verify both errors are StoreError with correct store types
	if err := verifyStoreError("Get after Close FDB", fdbErr, FDbStoreType, "FDB"); err != nil {
		errors = append(errors, err)
	}
	if err := verifyStoreError("Get after Close Magma", magmaErr, MagmaStoreType, "Magma"); err != nil {
		errors = append(errors, err)
	}

	// Compare the errors
	if err := compareErrors("Get after Close", fdbErr, magmaErr); err != nil {
		errors = append(errors, err)
	}

	// Both should return ErrRepoClosed with correct code
	if fdbErr != nil {
		fdbStoreErr, ok := fdbErr.(*StoreError)
		if ok && fdbStoreErr.storeCode != ErrRepoClosedCode {
			errors = append(errors, &comparisonError{
				operation: "Get after Close FDB",
				message: fmt.Sprintf(
					"Expected ErrRepoClosedCode, got code: %v",
					fdbStoreErr.storeCode,
				),
			})
		}
	}

	if magmaErr != nil {
		magmaStoreErr, ok := magmaErr.(*StoreError)
		if ok && magmaStoreErr.storeCode != ErrRepoClosedCode {
			errors = append(errors, &comparisonError{
				operation: "Get after Close Magma",
				message: fmt.Sprintf(
					"Expected ErrRepoClosedCode, got code: %v",
					magmaStoreErr.storeCode,
				),
			})
		}
	}

	for _, err := range errors {
		recordError(t, err)
	}
}

// TestRepositoryComparison_ComplexScenario tests a complex scenario with multiple operations
func TestRepositoryComparison_ComplexScenario(t *testing.T) {
	tr := setupRepositories(t)
	defer tr.cleanup()

	var errors []*comparisonError

	// Scenario: Set, Get, Delete, SetNoCommit, Commit, CreateSnapshot, AcquireSnapshot, NewIterator

	// 1. Set multiple keys
	keys := []string{"complex_key1", "complex_key2", "complex_key3"}
	values := [][]byte{[]byte("complex_value1"), []byte("complex_value2"), []byte("complex_value3")}

	for idx, key := range keys {
		fdbErr := tr.fdb.Set(MAIN, key, values[idx])
		magmaErr := tr.magma.Set(MAIN, key, values[idx])
		if err := compareErrors(fmt.Sprintf("Set(%s)", key), fdbErr, magmaErr); err != nil {
			errors = append(errors, err)
		}
	}

	// 2. Get all keys and compare
	for _, key := range keys {
		fdbVal, fdbErr := tr.fdb.Get(MAIN, key)
		magmaVal, magmaErr := tr.magma.Get(MAIN, key)
		if err := compareErrors(fmt.Sprintf("Get(%s) error", key), fdbErr, magmaErr); err != nil {
			errors = append(errors, err)
		}
		if fdbErr == nil && magmaErr == nil {
			if err := compareBytes(fmt.Sprintf("Get(%s) value", key), fdbVal, magmaVal); err != nil {
				errors = append(errors, err)
			}
		}
	}

	// 3. Delete one key
	fdbErr := tr.fdb.Delete(MAIN, keys[0])
	magmaErr := tr.magma.Delete(MAIN, keys[0])
	if err := compareErrors(fmt.Sprintf("Delete(%s)", keys[0]), fdbErr, magmaErr); err != nil {
		errors = append(errors, err)
	}

	// 4. SetNoCommit and Commit
	fdbErr = tr.fdb.SetNoCommit(MAIN, "complex_nocommit", []byte("complex_nocommit_value"))
	magmaErr = tr.magma.SetNoCommit(MAIN, "complex_nocommit", []byte("complex_nocommit_value"))
	if err := compareErrors("SetNoCommit(complex_nocommit)", fdbErr, magmaErr); err != nil {
		errors = append(errors, err)
	}

	fdbCommitErr := tr.fdb.Commit()
	magmaCommitErr := tr.magma.Commit()
	if err := compareErrors("Commit", fdbCommitErr, magmaCommitErr); err != nil {
		errors = append(errors, err)
	}

	// 5. CreateSnapshot
	testTxnID := c.Txnid(100)
	fdbErr = tr.fdb.CreateSnapshot(MAIN, testTxnID)
	magmaErr = tr.magma.CreateSnapshot(MAIN, testTxnID)
	if err := compareErrors("CreateSnapshot", fdbErr, magmaErr); err != nil {
		errors = append(errors, err)
	}

	// 6. AcquireSnapshot and compare iterator results
	fdbTxnID, fdbIter, fdbErr := tr.fdb.AcquireSnapshot(MAIN)
	magmaTxnID, magmaIter, magmaErr := tr.magma.AcquireSnapshot(MAIN)
	if err := compareErrors("AcquireSnapshot error", fdbErr, magmaErr); err != nil {
		errors = append(errors, err)
	}

	if fdbTxnID != magmaTxnID {
		errors = append(errors, &comparisonError{
			operation: "AcquireSnapshot txnid",
			message: fmt.Sprintf(
				"Transaction IDs differ - FDB: %d, Magma: %d",
				fdbTxnID,
				magmaTxnID,
			),
		})
	}

	if fdbIter != nil && magmaIter != nil {
		fdbResults := make(map[string][]byte)
		magmaResults := make(map[string][]byte)

		for {
			key, val, err := fdbIter.Next()
			if err != nil {
				break
			}
			fdbResults[key] = val
		}

		for {
			key, val, err := magmaIter.Next()
			if err != nil {
				break
			}
			magmaResults[key] = val
		}

		if len(fdbResults) != len(magmaResults) {
			errors = append(errors, &comparisonError{
				operation: "ComplexScenario iterator count",
				message: fmt.Sprintf(
					"Iterator result count differs - FDB: %d, Magma: %d",
					len(fdbResults),
					len(magmaResults),
				),
			})
		}

		for key, fdbVal := range fdbResults {
			magmaVal, exists := magmaResults[key]
			if !exists {
				errors = append(errors, &comparisonError{
					operation: "ComplexScenario iterator key",
					message:   fmt.Sprintf("Key %s exists in FDB but not in Magma", key),
				})
			} else if err := compareBytes(fmt.Sprintf("ComplexScenario iterator key %s", key), fdbVal, magmaVal); err != nil {
				errors = append(errors, err)
			}
		}

		fdbIter.Close()
		magmaIter.Close()
	}

	// 7. NewIterator and compare
	fdbIter2, fdbErr := tr.fdb.NewIterator(MAIN, "", "")
	magmaIter2, magmaErr := tr.magma.NewIterator(MAIN, "", "")
	if err := compareErrors("NewIterator error", fdbErr, magmaErr); err != nil {
		errors = append(errors, err)
	}

	if fdbIter2 != nil && magmaIter2 != nil {
		fdbResults2 := make(map[string][]byte)
		magmaResults2 := make(map[string][]byte)

		for {
			key, val, err := fdbIter2.Next()
			if err != nil {
				break
			}
			fdbResults2[key] = val
		}

		for {
			key, val, err := magmaIter2.Next()
			if err != nil {
				break
			}
			magmaResults2[key] = val
		}

		if len(fdbResults2) != len(magmaResults2) {
			errors = append(errors, &comparisonError{
				operation: "ComplexScenario NewIterator count",
				message: fmt.Sprintf(
					"Iterator result count differs - FDB: %d, Magma: %d",
					len(fdbResults2),
					len(magmaResults2),
				),
			})
		}

		for key, fdbVal := range fdbResults2 {
			magmaVal, exists := magmaResults2[key]
			if !exists {
				errors = append(errors, &comparisonError{
					operation: "ComplexScenario NewIterator key",
					message:   fmt.Sprintf("Key %s exists in FDB but not in Magma", key),
				})
			} else if err := compareBytes(fmt.Sprintf("ComplexScenario NewIterator key %s", key), fdbVal, magmaVal); err != nil {
				errors = append(errors, err)
			}
		}

		fdbIter2.Close()
		magmaIter2.Close()
	}

	for _, err := range errors {
		recordError(t, err)
	}
}
