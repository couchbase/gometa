package repository

import (
	"bytes"
	"fmt"
	"math/rand"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/couchbase/gometa/log"
	"github.com/couchbase/indexing/secondary/logging"
)

func init() {
	log.Current = &logging.SystemLogger
	logging.SetLogLevel(logging.Debug)
}

func (rk RepoKind) String() string {
	switch rk {
	case MAIN:
		return "Main"
	case COMMIT_LOG:
		return "Commit Log"
	case SERVER_CONFIG:
		return "Server Config"
	case LOCAL:
		return "Local"
	default:
		return "Unknown"
	}
}

func genData(size int64) []byte {
	var chars = []byte("abcdefghijklmnopqrstuvwxyz")
	rand.Seed(time.Now().Unix())
	var res = make([]byte, size)
	for i := int64(0); i < size; i++ {
		res[i] = chars[rand.Intn(len(chars))]
	}
	return res
}

const (
	kib = 1024
	mib = 1024 * kib
	gib = 1024 * mib
	tib = 1024 * gib
)

func usageInHuman(usage uint64) string {
	if usage < kib {
		return fmt.Sprintf("%d b", usage)
	} else if usage < mib {
		return fmt.Sprintf("%v KiB", float64(usage)/(kib))
	} else if usage < gib {
		return fmt.Sprintf("%v MiB", float64(usage)/(mib))
	} else if usage < tib {
		return fmt.Sprintf("%v TiB", float64(usage)/tib)
	}

	return fmt.Sprintf("%v GiB", float64(usage)/(gib))
}

func getOpenForestDBRepo(dir string) IRepository {
	path := filepath.Join(dir, "metadata.fdb")
	repo, err := OpenRepositoryWithName(path, 4*1024*1024*1024)
	if err != nil {
		panic(err)
	}
	return repo
}

// verifyFdbStoreError verifies that an error is a StoreError with FDbStoreType
func verifyFdbStoreError(t *testing.T, err error, operation string) *StoreError {
	if err == nil {
		return nil
	}
	storeErr, ok := err.(*StoreError)
	if !ok {
		t.Errorf("%s: expected StoreError, got %T: %v", operation, err, err)
		return nil
	}
	if storeErr.sType != FDbStoreType {
		t.Errorf("%s: expected FDbStoreType, got %s", operation, storeErr.sType)
	}
	return storeErr
}

type testKeyValPair struct {
	key   string
	value []byte
}

// Central test data for general operations.
var (
	setTestCases = []testKeyValPair{
		{"foo", genData(3 * kib)},
		{"special-!@#", genData(4 * kib)},
		{"small", genData(128 * 1024)},
		{"empty val", []byte{}},
		{"abc", nil},
	}
	setNegativeCases = []testKeyValPair{
		{"", []byte("empty key")},
	}
)

// setFunc and delFunc are for Set and Delete API signatures.
type setFunc func(kind RepoKind, key string, val []byte) error
type delFunc func(kind RepoKind, key string) error

// Shared Set logic.
func utilRepoSet(t *testing.T, repo IRepository, set setFunc, label string) {
	// Positive test cases
	for _, tc := range setTestCases {
		err := set(MAIN, tc.key, tc.value)
		if err != nil {
			verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("%s: Set(%s)", label, tc.key))
			t.Errorf("%s: Set failed for key=%s: %v", label, tc.key, err)
			continue
		}
	}

	// Negative test cases
	for _, tc := range setNegativeCases {
		err := set(MAIN, tc.key, tc.value)
		if err == nil {
			t.Errorf("%s: Set should have failed for key=%s", label, tc.key)
		} else {
			verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("%s: Set(negative case %s)", label, tc.key))
		}
	}

	// Overwriting existing key should succeed
	key := "dup"
	val1 := genData(5 * kib)
	val2 := genData(5 * kib)
	if err := set(MAIN, key, val1); err != nil {
		t.Fatalf("%s: Set (first) failed for key=%s: %v", label, key, err)
	}
	if err := set(MAIN, key, val2); err != nil {
		t.Fatalf("%s: Set (overwrite) failed for key=%s: %v", label, key, err)
	}

	// Large value test
	hugeVal := genData(21 * mib)
	hugeKey := "huge"
	err := set(MAIN, hugeKey, hugeVal)
	if err != nil {
		t.Fatalf("%s: Set with huge value failed for key=%s: %v", label, hugeKey, err)
	}
}

func TestForestDBRepository_Set(t *testing.T) {
	dir := t.TempDir()
	repo := getOpenForestDBRepo(dir)
	defer repo.Close()

	utilRepoSet(t, repo, repo.Set, t.Name())

	// Test that Set fails if we write after repo is closed
	repo.Close()
	err := repo.Set(MAIN, "key_after_close", []byte("val"))
	storeErr := verifyFdbStoreError(t, err, "Set after Close")
	if storeErr != nil && storeErr.Code() != ErrRepoClosedCode {
		t.Errorf(
			"Set should fail after repo is closed, expected ErrRepoClosedCode, got: %v",
			storeErr.Code(),
		)
	}
}

func TestForestDBRepository_SetNoCommit(t *testing.T) {
	dir := t.TempDir()
	repo := getOpenForestDBRepo(dir)
	defer repo.Close()

	utilRepoSet(t, repo, repo.SetNoCommit, t.Name())

	// Test that SetNoCommit fails if we write after repo is closed
	repo.Close()
	err := repo.SetNoCommit(MAIN, "key_after_close", []byte("val"))
	storeErr := verifyFdbStoreError(t, err, "SetNoCommit after Close")
	if storeErr != nil && storeErr.Code() != ErrRepoClosedCode {
		t.Errorf(
			"SetNoCommit should fail after repo is closed, expected ErrRepoClosedCode, got: %v",
			storeErr.Code(),
		)
	}

}

// Delete logic and tests

// Helper function for Delete tests
func utilRepoDelete(t *testing.T, repo IRepository, set setFunc, del delFunc, label string) {
	// Positive: Insert each key first, then delete it and ensure it's not retrievable
	for _, tc := range setTestCases {
		if err := set(MAIN, tc.key, tc.value); err != nil {
			verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("%s: Set(%s)", label, tc.key))
			t.Fatalf("%s: Set failed for key=%s: %v", label, tc.key, err)
		}
		if err := del(MAIN, tc.key); err != nil {
			verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("%s: Delete(%s)", label, tc.key))
			t.Errorf("%s: Delete failed for key=%s: %v", label, tc.key, err)
			continue
		}
	}

	// Negative: Delete with keys that don't exist or are invalid
	invalidKeys := []string{""}
	for _, key := range invalidKeys {
		err := del(MAIN, key)
		if err == nil {
			t.Errorf("%s: Delete should have failed for key=%s", label, key)
		} else {
			verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("%s: Delete(negative case %s)", label, key))
		}
	}

	// Also try to delete from stores like COMMIT_LOG without performing any set.
	// The delete operation should not return an error in these cases.
	storeList := []RepoKind{COMMIT_LOG /* add other stores here if desired */}
	for _, store := range storeList {
		err := del(store, "somekey")
		if err != nil {
			verifyStoreErrorForRepo(
				t,
				repo,
				err,
				fmt.Sprintf("%s: Delete(%s in %s)", label, "somekey", store),
			)
			t.Errorf("%s: Delete should not have failed for key=%s in store=%s err:%v",
				label, "somekey", store, err)
		}
		// Also test with an empty key in the store
		err = del(store, "")
		if err == nil {
			t.Errorf("%s: Delete should have failed for empty key in store=%s",
				label, store)
		} else {
			verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("%s: Delete(empty key in %s)", label, store))
		}
	}

	// Large key/value: Insert, delete, then check
	hugeKey := "huge-delete-test"
	hugeVal := genData(4 * mib)
	if err := set(MAIN, hugeKey, hugeVal); err != nil {
		t.Fatalf("%s: Setup Set for hugeKey failed: %v", label, err)
	}
	if err := del(MAIN, hugeKey); err != nil {
		t.Errorf("%s: Delete failed for hugeKey: %v", label, err)
	}
}

func TestForestDBRepository_Delete(t *testing.T) {
	dir := t.TempDir()
	repo := getOpenForestDBRepo(dir)
	defer repo.Close()

	utilRepoDelete(t, repo, repo.Set, repo.Delete, t.Name())

	// Ensure Delete fails after Close
	repo.Close()
	err := repo.Delete(MAIN, "key_after_close")
	storeErr := verifyFdbStoreError(t, err, "Delete after Close")
	if storeErr != nil && storeErr.Code() != ErrRepoClosedCode {
		t.Errorf(
			"Delete should fail after repo is closed, expected ErrRepoClosedCode, got: %v",
			storeErr.Code(),
		)
	}
}

func TestForestDBRepository_DeleteNoCommit(t *testing.T) {
	dir := t.TempDir()
	repo := getOpenForestDBRepo(dir)
	defer repo.Close()

	utilRepoDelete(t, repo, repo.Set, repo.DeleteNoCommit, t.Name())

	// Ensure DeleteNoCommit fails after Close
	repo.Close()
	err := repo.DeleteNoCommit(MAIN, "key_after_close")
	storeErr := verifyFdbStoreError(t, err, "DeleteNoCommit after Close")
	if storeErr != nil && storeErr.Code() != ErrRepoClosedCode {
		t.Errorf(
			"DeleteNoCommit should fail after repo is closed, expected ErrRepoClosedCode, got: %v",
			storeErr.Code(),
		)
	}
}

func assertEqual(res, val []byte) bool {
	if len(res) != len(val) {
		return false
	}
	return len(res) == 0 || reflect.DeepEqual(res, val)
}

func assertEmptyRes(res []byte, err error) bool {
	return !(err == nil && len(res) != 0)
}

// verifyStoreErrorForRepo verifies that an error is a StoreError with the correct store type for
// the repository
func verifyStoreErrorForRepo(t *testing.T, repo IRepository, err error, operation string) {
	if err == nil {
		return
	}
	expectedType := repo.Type()

	storeErr, ok := err.(*StoreError)
	if !ok {
		t.Errorf("%s: returned non-StoreError - %T: %v", operation, err, err)
		return
	}
	if storeErr.sType != expectedType {
		t.Errorf(
			"%s: returned StoreError with wrong store type - expected %s, got %s",
			operation,
			expectedType,
			storeErr.sType,
		)
	}
}

// Generalized Get test helper that works for any repository implementation
// repo: the main repository instance to test
func utilRepoGet(t *testing.T, repo IRepository) {

	// Test 1: Get existing keys with different value sizes in MAIN
	for _, tc := range setTestCases {
		err := repo.Set(MAIN, tc.key, tc.value)
		if err != nil {
			verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("Set(%s)", tc.key))
			t.Fatalf("Set failed for key=%s: %v", tc.key, err)
		}

		res, err := repo.Get(MAIN, tc.key)
		if err != nil {
			verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("Get(%s)", tc.key))
			t.Errorf("Get failed for key=%s: %v", tc.key, err)
			continue
		}
		if !assertEqual(res, tc.value) {
			t.Errorf("Get mismatch for key=%s: expected %s, got %s",
				tc.key, tc.value, res)
		}
	}

	// Test 2: Get after overwriting a key in MAIN
	key := "overwrite_test"
	val1 := []byte("value1")
	val2 := []byte("value2")

	if err := repo.Set(MAIN, key, val1); err != nil {
		verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("Set(first %s)", key))
		t.Fatalf("Set (first) failed for key=%s: %v", key, err)
	}
	res, err := repo.Get(MAIN, key)
	if err != nil {
		verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("Get(first %s)", key))
		t.Fatalf("Get (first) failed for key=%s: %v", key, err)
	}
	if !assertEqual(res, val1) {
		t.Fatalf("Get (first) mismatch for key=%s: expected %s , got %s",
			key, val1, res)
	}

	if err := repo.Set(MAIN, key, val2); err != nil {
		verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("Set(overwrite %s)", key))
		t.Fatalf("Set (overwrite) failed for key=%s: %v", key, err)
	}
	res, err = repo.Get(MAIN, key)
	if err != nil {
		verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("Get(overwrite %s)", key))
		t.Fatalf("Get (overwrite) failed for key=%s: %v", key, err)
	}
	if !assertEqual(res, val2) {
		t.Fatalf("Get (overwrite) mismatch for key=%s: expected %v, got %v", key, val2, res)
	}

	// Test 3: Get large value in MAIN
	hugeKey := "huge_value"
	hugeVal := genData(21 * mib)
	if err := repo.Set(MAIN, hugeKey, hugeVal); err != nil {
		verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("Set(huge %s)", hugeKey))
		t.Fatalf("Set with huge value failed for key=%s: %v", hugeKey, err)
	}
	res, err = repo.Get(MAIN, hugeKey)
	if err != nil {
		verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("Get(huge %s)", hugeKey))
		t.Fatalf("Get with huge value failed for key=%s: %v", hugeKey, err)
	}
	if !assertEqual(res, hugeVal) {
		t.Fatalf(
			"Get with huge value mismatch for key=%s: expected length %d, got %d",
			hugeKey,
			len(hugeVal),
			len(res),
		)
	}

	// Test 4: Get non-existent key in MAIN
	nonExistentKey := "non_existent_key_12345"
	res, err = repo.Get(MAIN, nonExistentKey)
	if err != nil {
		verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("Get(non-existent %s)", nonExistentKey))
	}
	if !assertEmptyRes(res, err) {
		t.Errorf(
			"Get should have failed for non-existent key=%s, but got error %v and result: %v",
			nonExistentKey,
			err,
			res,
		)
	}

	// Test 5: Get after delete in MAIN
	deleteKey := "to_delete"
	deleteVal := []byte("will_be_deleted")
	if err := repo.Set(MAIN, deleteKey, deleteVal); err != nil {
		verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("Set(delete test %s)", deleteKey))
		t.Fatalf("Set failed for key=%s: %v", deleteKey, err)
	}
	res, err = repo.Get(MAIN, deleteKey)
	if err != nil {
		verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("Get(before delete %s)", deleteKey))
		t.Fatalf("Get before delete failed for key=%s: %v", deleteKey, err)
	}
	if !assertEqual(res, deleteVal) {
		t.Fatalf("Get before delete mismatch for key=%s", deleteKey)
	}

	if err := repo.Delete(MAIN, deleteKey); err != nil {
		verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("Delete(%s)", deleteKey))
		t.Fatalf("Delete failed for key=%s: %v", deleteKey, err)
	}

	res, err = repo.Get(MAIN, deleteKey)
	if err != nil {
		verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("Get(after delete %s)", deleteKey))
	}
	if err == nil && len(res) != 0 {
		t.Errorf(
			"Get should have failed for deleted key=%s, but got error %v and result: %v",
			deleteKey,
			err,
			res,
		)
	}

	// Test 6: Multiple gets of the same key in MAIN
	multiKey := "multi_get"
	multiVal := []byte("multi_value")
	if err := repo.Set(MAIN, multiKey, multiVal); err != nil {
		verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("Set(multi %s)", multiKey))
		t.Fatalf("Set failed for key=%s: %v", multiKey, err)
	}

	for i := 0; i < 10; i++ {
		res, err = repo.Get(MAIN, multiKey)
		if err != nil {
			verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("Get(iteration %d %s)", i, multiKey))
			t.Fatalf("Get (iteration %d) failed for key=%s: %v", i, multiKey, err)
		}
		if !assertEqual(res, multiVal) {
			t.Fatalf("Get (iteration %d) mismatch for key=%s", i, multiKey)
		}
	}

	// Test 7: Get with empty key in MAIN (edge case)
	res, err = repo.Get(MAIN, "")
	if err != nil {
		verifyStoreErrorForRepo(t, repo, err, "Get(empty key)")
	}
	if err == nil {
		t.Logf("Get with empty key returned: err=%v, res=%v (this may be valid behavior)", err, res)
	}

	// Test 8: Get after SetNoCommit (to check if uncommitted changes are visible in MAIN)
	uncommittedKey := "uncommitted"
	uncommittedVal := []byte("uncommitted_value")
	if err := repo.SetNoCommit(MAIN, uncommittedKey, uncommittedVal); err != nil {
		verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("SetNoCommit(%s)", uncommittedKey))
		t.Fatalf("SetNoCommit failed for key=%s: %v", uncommittedKey, err)
	}

	// Try to get before commit
	res, err = repo.Get(MAIN, uncommittedKey)
	if err != nil {
		verifyStoreErrorForRepo(
			t,
			repo,
			err,
			fmt.Sprintf("Get(after SetNoCommit %s)", uncommittedKey),
		)
		t.Fatalf(
			"Get after SetNoCommit (before commit) returned error: %v (this may be expected)",
			err,
		)
	}
	if !assertEqual(res, uncommittedVal) {
		t.Logf(
			"Get after SetNoCommit (before commit) returned different value: expected %v, got %v",
			uncommittedVal,
			res,
		)
	}

	// Test 9: Get with special characters in key in MAIN
	specialKey := "special-!@#$%^&*()_+{}|:<>?"
	specialVal := []byte("special_value")
	if err := repo.Set(MAIN, specialKey, specialVal); err != nil {
		verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("Set(special %s)", specialKey))
		t.Fatalf("Set failed for special key=%s: %v", specialKey, err)
	}
	res, err = repo.Get(MAIN, specialKey)
	if err != nil {
		verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("Get(special %s)", specialKey))
		t.Fatalf("Get failed for special key=%s: %v", specialKey, err)
	}
	if !assertEqual(res, specialVal) {
		t.Fatalf(
			"Get mismatch for special key=%s: expected %v, got %v",
			specialKey,
			specialVal,
			res,
		)
	}

	// ---- Additional tests for isolation between RepoKinds ----

	// Define an alternate RepoKind different from MAIN
	otherKind := COMMIT_LOG // use LOG as an existing RepoKind other than MAIN

	// Set a key in otherKind, should not be visible in MAIN
	k := "uniquekey-aaa"
	v := []byte("specialvalue")
	// First, test that a Get from otherKind before any Set fails as expected.
	res, errOtherKindBeforeSet := repo.Get(otherKind, k)
	if errOtherKindBeforeSet != nil {
		verifyStoreErrorForRepo(
			t,
			repo,
			errOtherKindBeforeSet,
			fmt.Sprintf("Get(otherKind %s before Set)", k),
		)
	}
	if !assertEmptyRes(res, errOtherKindBeforeSet) {
		t.Errorf("Get from otherKind %v before Set should fail, but got no error and res %s",
			otherKind, res)
	}

	if err := repo.Set(otherKind, k, v); err == nil {
		// Only run these subtests if Set is not returning error (should only fail if no support for
		// this kind)
		got, err := repo.Get(otherKind, k)
		if err != nil {
			verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("Get(otherKind %s)", k))
			t.Errorf("Get failed for key=%s in otherKind: %v", k, err)
		} else if !assertEqual(got, v) {
			t.Errorf("Get mismatch for key=%s in otherKind: expected %v, got %v", k, v, got)
		}
		// Now check not visible in MAIN
		gotFromMain, errMain := repo.Get(MAIN, k)
		if errMain != nil {
			verifyStoreErrorForRepo(
				t,
				repo,
				errMain,
				fmt.Sprintf("Get(MAIN %s after otherKind Set)", k),
			)
		}
		if !assertEmptyRes(gotFromMain, errMain) {
			t.Errorf("Key set on otherKind was visible in MAIN, value: %v", gotFromMain)
		}
	} else {
		verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("Set(otherKind %s)", k))
		t.Errorf("Set failed for key=%s in otherKind: %v", k, err)
	}

	// Set a key in MAIN and ensure it is not visible in otherKind
	k2 := "uniquekey-bbb"
	v2 := []byte("othervalue")
	if err := repo.Set(MAIN, k2, v2); err != nil {
		verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("Set(MAIN %s)", k2))
		t.Errorf("Set failed on MAIN for key %s: %v", k2, err)
	}
	gotInMain, err := repo.Get(MAIN, k2)
	if err != nil {
		verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("Get(MAIN %s)", k2))
	}
	if err != nil || !assertEqual(gotInMain, v2) {
		t.Errorf("Get failed for key=%s in MAIN: %v", k2, err)
	}

	gotInOther, err := repo.Get(otherKind, k2)
	if err != nil {
		verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("Get(otherKind %s)", k2))
	}
	if !assertEmptyRes(gotInOther, err) {
		t.Errorf("Key set on MAIN is unexpectedly visible in otherKind, value: %v", gotInOther)
	}

	// Test 10: Insert and Get empty value
	emptyKey := "empty_value_key"
	emptyVal := []byte{}
	if err := repo.Set(MAIN, emptyKey, emptyVal); err != nil {
		verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("Set(empty value %s)", emptyKey))
		t.Fatalf("Set failed for empty value key=%s: %v", emptyKey, err)
	}
	res, err = repo.Get(MAIN, emptyKey)
	if err != nil {
		verifyStoreErrorForRepo(t, repo, err, fmt.Sprintf("Get(empty value %s)", emptyKey))
		t.Fatalf("Get failed for empty value key=%s: %v", emptyKey, err)
	}
	// Verify that the retrieved value is empty (length 0)
	if len(res) != 0 {
		t.Errorf(
			"Get returned non-empty value for empty value key=%s: expected length 0, got length %d, value: %v",
			emptyKey,
			len(res),
			res,
		)
	}
	// Also verify it's equal to empty byte slice
	if !assertEqual(res, emptyVal) {
		t.Errorf(
			"Get mismatch for empty value key=%s: expected %v, got %v",
			emptyKey,
			emptyVal,
			res,
		)
	}

	// Test 11: Get after repository is closed for MAIN
	repo.Close()
	_, err = repo.Get(MAIN, "key_after_close")
	verifyStoreErrorForRepo(t, repo, err, "get after close")
	storeErr := err.(*StoreError)
	if storeErr == nil || storeErr.Code() != ErrRepoClosedCode {
		t.Errorf("Get should fail after repo is closed, expected ErrRepoClosedCode, got: %v", err)
	}
}

func TestForestDBRepository_Get(t *testing.T) {
	dir := t.TempDir()
	repo := getOpenForestDBRepo(dir)
	defer repo.Close()
	utilRepoGet(t, repo)
}

func TestForestDBRepository_EmptyValue(t *testing.T) {
	dir := t.TempDir()
	repo := getOpenForestDBRepo(dir)
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

func TestForestDBRepository_Reopen(t *testing.T) {
	dir := t.TempDir()

	repo := getOpenForestDBRepo(dir)
	utilRepoSet(t, repo, repo.Set, t.Name())
	// Note: Fdb_Repository doesn't have GetItemsCount method, so we verify persistence differently
	// by checking that data is still accessible after reopening
	testKey := "foo"
	testVal := setTestCases[0].value
	repo.Close()

	// verify repo closed
	_, err := repo.Get(MAIN, "maybe_key")
	storeErr := verifyFdbStoreError(t, err, "Get after Close")
	if storeErr == nil || storeErr.Code() != ErrRepoClosedCode {
		t.Fatal("repo should have been closed but it is not")
	}

	repo = getOpenForestDBRepo(dir)
	// Verify that data persisted after reopening
	res, err := repo.Get(MAIN, testKey)
	if err != nil {
		t.Fatalf("Get failed after reopen for key=%s: %v", testKey, err)
	}
	if !reflect.DeepEqual(res, testVal) {
		t.Errorf(
			"Data mismatch after reopen for key=%s: expected %v, got %v",
			testKey,
			testVal,
			res,
		)
	}
	repo.Close()
}

func TestForestDBIter_CreateSnapshots(t *testing.T) {
	dir := t.TempDir()

	repo := getOpenForestDBRepo(dir).(*Fdb_Repository)
	defer repo.Close()
	err := repo.CreateSnapshot(MAIN, 1)
	if err != nil {
		t.Fatalf("failed to create snapshot for empty store with error - %v", err)
	}

	if len(repo.snapshots[MAIN]) == 0 {
		t.Fatalf("failed to find snapshot in snapshots map")
	}

	snapContainer := repo.snapshots[MAIN][0]
	if snapContainer == nil {
		t.Fatalf("failed to get snapshot container")
	}
	if snapContainer.snapshot == nil {
		t.Fatalf("got nil snapshot from snapshot container")
	}
	if snapContainer.txnid != 1 {
		t.Fatalf("got unexpected txnID from snapshot container expected 1 but got %d",
			snapContainer.txnid)
	}
	if snapContainer.count != 0 {
		t.Fatalf("got unexpected refCount from snapshot container expected 0 but got %d",
			snapContainer.count)
	}

	err = repo.CreateSnapshot(MAIN, 1)
	if err != nil {
		t.Errorf("failed to create multiple snapshots for same txnid")
	}

	// this will lead to pruneSnapshot closing the previously opened snapshots
	err = repo.CreateSnapshot(MAIN, 2)
	if err != nil {
		t.Fatalf("failed to create new snapshot with error - %v", err)
	}

	// verify that pruneSnapshot has deleted the older snapshot
	if len(repo.snapshots[MAIN]) != 1 {
		t.Fatalf("failed to create new snapshot, expected 1 snapshot but got %d",
			len(repo.snapshots[MAIN]))
	}

	snapContainer = repo.snapshots[MAIN][0]
	if snapContainer == nil {
		t.Fatalf("failed to get new snapshot container")
	}
	if snapContainer.snapshot == nil {
		t.Fatalf("got nil snapshot from new snapshot container")
	}
	if snapContainer.txnid != 2 {
		t.Fatalf("got unexpected txnID from new snapshot container expected 2 but got %d",
			snapContainer.txnid)
	}
	if snapContainer.count != 0 {
		t.Fatalf("got unexpected refCount from new snapshot container expected 0 but got %d",
			snapContainer.count)
	}
	snapContainer.count++

	err = repo.CreateSnapshot(MAIN, 2)
	if err != nil {
		t.Fatalf("failed to create new snapshot with error - %v", err)
	}

	if len(repo.snapshots[MAIN]) != 2 {
		t.Fatalf("failed to create new snapshot, expected 2 snapshots but got %d",
			len(repo.snapshots[MAIN]))
	}

	/*
		forestDb does not support creating snapshots on otherKind and it may panic
			nonExistsKind := RepoKind(999)
			err = repo.CreateSnapshot(nonExistsKind, 3)
			if err == nil {
				t.Fatalf("expected error for creating snapshot for non-existent store, got none")
			}
	*/
	repo.Close()

	err = repo.CreateSnapshot(MAIN, 1)
	storeErr := verifyFdbStoreError(t, err, "CreateSnapshot after Close")
	if storeErr == nil || storeErr.Code() != ErrRepoClosedCode {
		t.Fatalf("expected CreateSnapshot to fail with store closed error but got %v", err)
	}
}

func TestForestDBIter_ReleaseSnapshots(t *testing.T) {
	dir := t.TempDir()

	repo := getOpenForestDBRepo(dir).(*Fdb_Repository)
	defer repo.Close()

	// No snapshots to start with
	if len(repo.snapshots[MAIN]) != 0 {
		t.Fatalf("expected 0 snapshots at start, but got %d", len(repo.snapshots[MAIN]))
	}

	// Create first snapshot
	err := repo.CreateSnapshot(MAIN, 100)
	if err != nil {
		t.Fatalf("failed to create snapshot: %v", err)
	}
	repo.snapshots[MAIN][0].count++
	err = repo.CreateSnapshot(MAIN, 100)
	if err != nil {
		t.Fatalf("failed to create snapshot: %v", err)
	}

	repo.ReleaseSnapshot(MAIN, 100)
	for _, snapContainer := range repo.snapshots[MAIN] {
		if snapContainer.txnid == 100 && snapContainer.count != 0 {
			t.Errorf("expected refCount 0 after release, got %d", snapContainer.count)
		}
	}
}

func TestForestDBIter_AcquireSnapshot(t *testing.T) {
	dir := t.TempDir()

	repo := getOpenForestDBRepo(dir).(*Fdb_Repository)
	defer repo.Close()

	txnid, iter, err := repo.AcquireSnapshot(MAIN)
	if err != nil || txnid != 0 || iter != nil {
		t.Fatalf(
			"acquired snapshot when no snapshots exist should return nils but got: %v, txnid %d, iter %v",
			err,
			txnid,
			iter,
		)
	}

	utilRepoSet(t, repo, repo.Set, t.Name())

	err = repo.CreateSnapshot(MAIN, 1)
	if err != nil {
		t.Fatalf("failed to create snapshot: %v", err)
	}

	txnid, iter, err = repo.AcquireSnapshot(MAIN)
	if err != nil || txnid != 1 || iter == nil {
		t.Fatalf("failed to acquire snapshot: %v, txnid %d, iter %v", err, txnid, iter)
	}

	// test if we can close the snapshots if we have open iterator
	repo.ReleaseSnapshot(MAIN, txnid)
	repo.pruneSnapshotNoLock(MAIN)

	utilRepoDelete(t, repo, repo.SetNoCommit, repo.DeleteNoCommit, t.Name())

	repo.CreateSnapshot(MAIN, 2)

	key, val, err := iter.Next()
	for ; err == nil; key, val, err = iter.Next() {
		if len(val) < 1*kib {
			t.Logf("key %v, val %s, err %v", key, val, err)
		}
	}

	iter.Close()

}

func compareMultiIterators(t *testing.T,
	count int, compareFunc func(keyA string, valA []byte, keyB string, valB []byte) bool,
	iters ...IRepoIterator) {
	for i := 0; i < count; i++ {
		vals := make([][]byte, len(iters))
		keys := make([]string, len(iters))
		errs := make([]error, len(iters))
		for j := 0; j < len(iters); j++ {
			keys[j], vals[j], errs[j] = iters[j].Next()
		}

		for j := 0; j < len(iters); j++ {
			if errs[j] != nil && errs[j].(*StoreError).Code() != ErrIterFailCode {
				t.Errorf("failed to get for iter[%d] i(%d) data: err(%v)", j, i, errs[j])
			}
		}

		for j := 0; j < len(iters); j++ {
			for k := j + 1; k < len(iters); k++ {
				if !compareFunc(keys[j], vals[j], keys[k], vals[k]) {
					t.Errorf(
						"compare(keyA(%s), valA(%s), keyB(%s), valB(%s)) failed for iter %d and %d",
						keys[j],
						vals[j],
						keys[k],
						vals[k],
						j,
						k,
					)
				}
			}
		}
	}
}

func getIter(t *testing.T, repo IRepository, startKey, endKey string) IRepoIterator {
	iter, err := repo.NewIterator(MAIN, startKey, endKey)
	if err != nil || iter == nil {
		t.Fatalf("failed to create iterator: err(%v), iter(%v)", err, iter)
	}
	return iter
}

func assertNoDataFromIter(t *testing.T, iter IRepoIterator) {
	nextKey, nextVal, err := iter.Next()
	if len(nextKey) > 0 || len(nextVal) > 0 || err.(*StoreError).Code() != ErrIterFailCode {
		t.Fatalf("expected no data but got: key(%v), val(%v), err(%v)", nextKey, nextVal, err)
	}
}

func handleError(t *testing.T, err error, msg string) {
	if err != nil {
		t.Fatalf("%s: %v", msg, err)
	}
}

func TestForestDBIter_Iterations(t *testing.T) {
	key := func(i int) string {
		return fmt.Sprintf("key%d", i)
	}
	val := func(i int, prefix string) []byte {
		return []byte(fmt.Sprintf("%s%d", prefix, i))
	}

	dir := t.TempDir()

	repo := getOpenForestDBRepo(dir).(*Fdb_Repository)

	iterNoData, err := repo.NewIterator(MAIN, "", "")
	if err != nil || iterNoData == nil {
		t.Fatalf("failed to create iterator: err(%v), iter(%v)", err, iterNoData)
	}

	assertNoDataFromIter(t, iterNoData)
	iterNoData.Close()

	var dataLen = 10

	for i := 0; i < dataLen; i++ {
		err = repo.Set(MAIN, key(i), val(i, "val"))
		handleError(t, err, "failed to set data")
	}

	iterWithData := getIter(t, repo, "", "")

	for i := 0; i < dataLen; i++ {
		err = repo.Set(MAIN, key(i), val(i, "valv2"))
		handleError(t, err, "failed to set data")
	}

	iterWithUpdatedData := getIter(t, repo, "", "")
	iterWithUpdatedData2 := getIter(t, repo, "", "")
	iterWithEndKey := getIter(t, repo, "key0", "key5")
	iterWithStartKey := getIter(t, repo, "key3", "")
	iterWithStartAndEndKey := getIter(t, repo, "key3", "key5")
	iterWithSmallerStartKey := getIter(t, repo, "key", "")
	iterWithEndKeyLargerThanStartKey := getIter(t, repo, "key8", "key7")
	iterWithStartKeyGreaterThanLargestKey := getIter(t, repo, key(98), "")

	for i := 0; i < dataLen; i++ {
		err = repo.Delete(MAIN, key(i))
		handleError(t, err, "failed to delete data")
	}

	iterWithDeletedData := getIter(t, repo, "", "")

	assertNoDataFromIter(t, iterWithDeletedData)

	// verify that iter(set and updated) data key is matching and val is different
	compareMultiIterators(
		t,
		dataLen-1,
		func(keyA string, valA []byte, keyB string, valB []byte) bool {
			return keyA == keyB && !bytes.Equal(valA, valB)
		},
		iterWithData,
		iterWithUpdatedData,
	)

	compareMultiIterators(
		t,
		dataLen-1,
		func(keyA string, valA []byte, keyB string, valB []byte) bool {
			return keyA == keyB && bytes.Equal(valA, valB)
		},
		iterWithUpdatedData2,
		iterWithSmallerStartKey,
	)

	nextKey, nextVal, err := iterWithData.Next()
	if len(nextKey) == 0 || len(nextVal) == 0 ||
		(err != nil && err.(*StoreError).Code() != ErrIterFailCode) {
		t.Errorf("expected data and err but got: key(%s), val(%s), err(%v)", nextKey, nextVal, err)
	}

	iterWithData.Close()
	iterWithUpdatedData.Close()
	iterWithUpdatedData2.Close()
	iterWithDeletedData.Close()
	iterWithSmallerStartKey.Close()

	assertNoDataFromIter(t, iterWithUpdatedData)

	assertNoDataFromIter(t, iterWithEndKeyLargerThanStartKey)
	assertNoDataFromIter(t, iterWithStartKeyGreaterThanLargestKey)
	iterWithStartKeyGreaterThanLargestKey.Close()

	// verify that iter with end key
	for i := 0; i < 6; i++ {
		nextKey, nextVal, err = iterWithEndKey.Next()
		if (nextKey) != key(i) || len(nextVal) == 0 ||
			(err != nil && err.(*StoreError).Code() != ErrIterFailCode) {
			t.Fatalf(
				"expected data and err but got: key(%s), val(%s), err(%v)",
				nextKey,
				nextVal,
				err,
			)
		}
	}
	assertNoDataFromIter(t, iterWithEndKey)
	iterWithEndKey.Close()

	for i := 3; i < dataLen; i++ {
		nextKey, nextVal, err = iterWithStartKey.Next()
		if (nextKey) != key(i) || len(nextVal) == 0 ||
			(err != nil && err.(*StoreError).Code() != ErrIterFailCode) {
			t.Fatalf(
				"expected data and err but got: key(%s), val(%s), err(%v)",
				nextKey,
				nextVal,
				err,
			)
		}
	}
	assertNoDataFromIter(t, iterWithStartKey)
	iterWithStartKey.Close()

	for i := 3; i < 6; i++ {
		nextKey, nextVal, err = iterWithStartAndEndKey.Next()
		if (nextKey) != key(i) || len(nextVal) == 0 ||
			(err != nil && err.(*StoreError).Code() != ErrIterFailCode) {
			t.Fatalf(
				"expected data and err but got: key(%s), val(%s), err(%v)",
				nextKey,
				nextVal,
				err,
			)
		}
	}
	assertNoDataFromIter(t, iterWithStartAndEndKey)
	iterWithStartAndEndKey.Close()

	repo.Close()

	iter, err := repo.NewIterator(MAIN, "", "")
	storeErr := verifyFdbStoreError(t, err, "NewIterator after Close")
	if storeErr == nil || storeErr.Code() != ErrRepoClosedCode || iter != nil {
		t.Fatal("got valid iter after repo close")
	}
}
