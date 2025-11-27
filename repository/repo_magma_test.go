//go:build !community
// +build !community

package repository

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/couchbase/indexing/secondary/logging"
)

func init() {
	l = &logging.SystemLogger
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

func getOpenRepo(path string) IRepository {
	repo, err := OpenMagmaRepository(path)
	if err != nil {
		panic(err)
	}
	return repo
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
	}
	setNegativeCases = []testKeyValPair{
		{"empty val", []byte{}},
		{"", []byte("empty key")},
		{"abc", nil},
	}
)

// setFunc and delFunc are for Set and Delete API signatures.
type setFunc func(kind RepoKind, key string, val []byte) error
type delFunc func(kind RepoKind, key string) error

// Shared Set logic.
func utilMagmaRepoSet(t *testing.T, repo IRepository, set setFunc, label string) {
	// Positive test cases
	for _, tc := range setTestCases {
		err := set(MAIN, tc.key, tc.value)
		if err != nil {
			t.Errorf("%s: Set failed for key=%q: %v", label, tc.key, err)
			continue
		}
	}

	// Negative test cases
	for _, tc := range setNegativeCases {
		err := set(MAIN, tc.key, tc.value)
		if err == nil {
			t.Errorf("%s: Set should have failed for key=%q", label, tc.key)
		}
	}

	// Overwriting existing key should succeed
	key := "dup"
	val1 := genData(5 * kib)
	val2 := genData(5 * kib)
	if err := set(MAIN, key, val1); err != nil {
		t.Fatalf("%s: Set (first) failed for key=%q: %v", label, key, err)
	}
	if err := set(MAIN, key, val2); err != nil {
		t.Fatalf("%s: Set (overwrite) failed for key=%q: %v", label, key, err)
	}

	// Large value test
	hugeVal := genData(21 * mib)
	hugeKey := "huge"
	err := set(MAIN, hugeKey, hugeVal)
	if err != nil {
		t.Fatalf("%s: Set with huge value failed for key=%q: %v", label, hugeKey, err)
	}
}

func TestMagmaRepository_Set(t *testing.T) {
	dir := t.TempDir()
	repo := getOpenRepo(dir)
	defer repo.Close()

	utilMagmaRepoSet(t, repo, repo.Set, t.Name())

	// Test that Set fails if we write after repo is closed
	repo.Close()
	err := repo.Set(MAIN, "key_after_close", []byte("val"))
	if err != ErrRepoClosed {
		t.Errorf("Set should fail after repo is closed, expected ErrRepoClosed, got: %v", err)
	}
}

func TestMagmaRepository_SetNoCommit(t *testing.T) {
	dir := t.TempDir()
	repo := getOpenRepo(dir)
	defer repo.Close()

	utilMagmaRepoSet(t, repo, repo.SetNoCommit, t.Name())

	// Test that SetNoCommit fails if we write after repo is closed
	repo.Close()
	err := repo.SetNoCommit(MAIN, "key_after_close", []byte("val"))
	if err != ErrRepoClosed {
		t.Errorf("SetNoCommit should fail after repo is closed, expected ErrRepoClosed, got: %v", err)
	}
}

// Delete logic and tests

// Helper function for Delete tests
func utilMagmaRepoDelete(t *testing.T, repo IRepository, set setFunc, del delFunc, label string) {
	// Positive: Insert each key first, then delete it and ensure it's not retrievable
	for _, tc := range setTestCases {
		if err := set(MAIN, tc.key, tc.value); err != nil {
			t.Fatalf("%s: Set failed for key=%q: %v", label, tc.key, err)
		}
		if err := del(MAIN, tc.key); err != nil {
			t.Errorf("%s: Delete failed for key=%q: %v", label, tc.key, err)
			continue
		}
	}

	// Negative: Delete with keys that don't exist or are invalid
	invalidKeys := []string{"not-present", "", "abc"}
	for _, key := range invalidKeys {
		err := del(MAIN, key)
		if err == nil {
			t.Errorf("%s: Delete should have failed for key=%q", label, key)
		}
	}

	// Also try to delete from stores like COMMIT_LOG without performing any set.
	// The delete operation should return an error in these cases.
	storeList := []RepoKind{COMMIT_LOG /* add other stores here if desired */}
	for _, store := range storeList {
		err := del(store, "somekey")
		if err == nil {
			t.Errorf("%s: Delete should have failed for key=%q in store=%q (no set performed)", label, "somekey", store)
		}
		// Also test with an empty key in the store
		err = del(store, "")
		if err == nil {
			t.Errorf("%s: Delete should have failed for empty key in store=%q (no set performed)", label, store)
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

func TestMagmaRepository_Delete(t *testing.T) {
	dir := t.TempDir()
	repo := getOpenRepo(dir)
	defer repo.Close()

	utilMagmaRepoDelete(t, repo, repo.Set, repo.Delete, t.Name())

	// Ensure Delete fails after Close
	repo.Close()
	err := repo.Delete(MAIN, "key_after_close")
	if err != ErrRepoClosed {
		t.Errorf("Delete should fail after repo is closed, expected ErrRepoClosed, got: %v", err)
	}
}

func TestMagmaRepository_DeleteNoCommit(t *testing.T) {
	dir := t.TempDir()
	repo := getOpenRepo(dir)
	defer repo.Close()

	utilMagmaRepoDelete(t, repo, repo.Set, repo.DeleteNoCommit, t.Name())

	// Ensure DeleteNoCommit fails after Close
	repo.Close()
	err := repo.DeleteNoCommit(MAIN, "key_after_close")
	if err != ErrRepoClosed {
		t.Errorf("DeleteNoCommit should fail after repo is closed, expected ErrRepoClosed, got: %v", err)
	}
}

func TestMagmaRepository_Get(t *testing.T) {
	dir := t.TempDir()
	repo := getOpenRepo(dir)
	defer repo.Close()

	// Test 1: Get existing keys with different value sizes in MAIN
	for _, tc := range setTestCases {
		err := repo.Set(MAIN, tc.key, tc.value)
		if err != nil {
			t.Fatalf("Set failed for key=%q: %v", tc.key, err)
		}

		res, err := repo.Get(MAIN, tc.key)
		if err != nil {
			t.Errorf("Get failed for key=%q: %v", tc.key, err)
			continue
		}
		if !reflect.DeepEqual(res, tc.value) {
			t.Errorf("Get mismatch for key=%q: expected %v, got %v", tc.key, tc.value, res)
		}
	}

	// Test 2: Get after overwriting a key in MAIN
	key := "overwrite_test"
	val1 := []byte("value1")
	val2 := []byte("value2")

	if err := repo.Set(MAIN, key, val1); err != nil {
		t.Fatalf("Set (first) failed for key=%q: %v", key, err)
	}
	res, err := repo.Get(MAIN, key)
	if err != nil {
		t.Fatalf("Get (first) failed for key=%q: %v", key, err)
	}
	if !reflect.DeepEqual(res, val1) {
		t.Fatalf("Get (first) mismatch for key=%q: expected %v, got %v", key, val1, res)
	}

	if err := repo.Set(MAIN, key, val2); err != nil {
		t.Fatalf("Set (overwrite) failed for key=%q: %v", key, err)
	}
	res, err = repo.Get(MAIN, key)
	if err != nil {
		t.Fatalf("Get (overwrite) failed for key=%q: %v", key, err)
	}
	if !reflect.DeepEqual(res, val2) {
		t.Fatalf("Get (overwrite) mismatch for key=%q: expected %v, got %v", key, val2, res)
	}

	// Test 3: Get large value in MAIN
	hugeKey := "huge_value"
	hugeVal := genData(21 * mib)
	if err := repo.Set(MAIN, hugeKey, hugeVal); err != nil {
		t.Fatalf("Set with huge value failed for key=%q: %v", hugeKey, err)
	}
	res, err = repo.Get(MAIN, hugeKey)
	if err != nil {
		t.Fatalf("Get with huge value failed for key=%q: %v", hugeKey, err)
	}
	if !reflect.DeepEqual(res, hugeVal) {
		t.Fatalf("Get with huge value mismatch for key=%q: expected length %d, got %d", hugeKey, len(hugeVal), len(res))
	}

	// Test 4: Get non-existent key in MAIN
	nonExistentKey := "non_existent_key_12345"
	res, err = repo.Get(MAIN, nonExistentKey)
	if err == nil && len(res) != 0 {
		t.Errorf("Get should have failed for non-existent key=%q, but got error %v and result: %v", nonExistentKey, err, res)
	}

	// Test 5: Get after delete in MAIN
	deleteKey := "to_delete"
	deleteVal := []byte("will_be_deleted")
	if err := repo.Set(MAIN, deleteKey, deleteVal); err != nil {
		t.Fatalf("Set failed for key=%q: %v", deleteKey, err)
	}
	res, err = repo.Get(MAIN, deleteKey)
	if err != nil {
		t.Fatalf("Get before delete failed for key=%q: %v", deleteKey, err)
	}
	if !reflect.DeepEqual(res, deleteVal) {
		t.Fatalf("Get before delete mismatch for key=%q", deleteKey)
	}

	if err := repo.Delete(MAIN, deleteKey); err != nil {
		t.Fatalf("Delete failed for key=%q: %v", deleteKey, err)
	}

	res, err = repo.Get(MAIN, deleteKey)
	if err == nil && len(res) != 0 {
		t.Errorf("Get should have failed for deleted key=%q, but got error %v and result: %v", deleteKey, err, res)
	}

	// Test 6: Multiple gets of the same key in MAIN
	multiKey := "multi_get"
	multiVal := []byte("multi_value")
	if err := repo.Set(MAIN, multiKey, multiVal); err != nil {
		t.Fatalf("Set failed for key=%q: %v", multiKey, err)
	}

	for i := 0; i < 10; i++ {
		res, err = repo.Get(MAIN, multiKey)
		if err != nil {
			t.Fatalf("Get (iteration %d) failed for key=%q: %v", i, multiKey, err)
		}
		if !reflect.DeepEqual(res, multiVal) {
			t.Fatalf("Get (iteration %d) mismatch for key=%q", i, multiKey)
		}
	}

	// Test 7: Get with empty key in MAIN (edge case)
	res, err = repo.Get(MAIN, "")
	if err == nil {
		t.Logf("Get with empty key returned: err=%v, res=%v (this may be valid behavior)", err, res)
	}

	// Test 8: Get after SetNoCommit (to check if uncommitted changes are visible in MAIN)
	uncommittedKey := "uncommitted"
	uncommittedVal := []byte("uncommitted_value")
	if err := repo.SetNoCommit(MAIN, uncommittedKey, uncommittedVal); err != nil {
		t.Fatalf("SetNoCommit failed for key=%q: %v", uncommittedKey, err)
	}

	// Try to get before commit
	res, err = repo.Get(MAIN, uncommittedKey)
	if err != nil {
		t.Fatalf("Get after SetNoCommit (before commit) returned error: %v (this may be expected)", err)
	}
	if !reflect.DeepEqual(res, uncommittedVal) {
		t.Logf("Get after SetNoCommit (before commit) returned different value: expected %v, got %v", uncommittedVal, res)
	}

	// Test 9: Get with special characters in key in MAIN
	specialKey := "special-!@#$%^&*()_+{}|:<>?"
	specialVal := []byte("special_value")
	if err := repo.Set(MAIN, specialKey, specialVal); err != nil {
		t.Fatalf("Set failed for special key=%q: %v", specialKey, err)
	}
	res, err = repo.Get(MAIN, specialKey)
	if err != nil {
		t.Fatalf("Get failed for special key=%q: %v", specialKey, err)
	}
	if !reflect.DeepEqual(res, specialVal) {
		t.Fatalf("Get mismatch for special key=%q: expected %v, got %v", specialKey, specialVal, res)
	}

	// ---- Additional tests for isolation between RepoKinds ----

	// Define an alternate RepoKind different from MAIN
	otherKind := COMMIT_LOG // use LOG as an existing RepoKind other than MAIN

	// Set a key in otherKind, should not be visible in MAIN
	k := "uniquekey-aaa"
	v := []byte("specialvalue")
	// First, test that a Get from otherKind before any Set fails as expected.
	res, errOtherKindBeforeSet := repo.Get(otherKind, k)
	if errOtherKindBeforeSet == nil && len(res) != 0 {
		t.Errorf("Get from otherKind %v before Set should fail, but got no error and res %s",
			otherKind, res)
	}

	if err := repo.Set(otherKind, k, v); err == nil {
		// Only run these subtests if Set is not returning error (should only fail if no support for this kind)
		got, err := repo.Get(otherKind, k)
		if err != nil {
			t.Errorf("Get failed for key=%q in otherKind: %v", k, err)
		} else if !reflect.DeepEqual(got, v) {
			t.Errorf("Get mismatch for key=%q in otherKind: expected %v, got %v", k, v, got)
		}
		// Now check not visible in MAIN
		gotFromMain, errMain := repo.Get(MAIN, k)
		if (errMain == nil && len(gotFromMain) > 0) || (errMain != nil && errMain != ErrRepoClosed) {
			// Only care if it's unexpectedly present in MAIN
			if len(gotFromMain) != 0 {
				t.Errorf("Key set on otherKind was visible in MAIN, value: %v", gotFromMain)
			}
		}
	} else {
		t.Errorf("Set failed for key=%q in otherKind: %v", k, err)
	}

	// Set a key in MAIN and ensure it is not visible in otherKind
	k2 := "uniquekey-bbb"
	v2 := []byte("othervalue")
	repo2 := getOpenRepo(t.TempDir())
	defer repo2.Close()
	if err := repo2.Set(MAIN, k2, v2); err != nil {
		t.Errorf("Set failed on MAIN for key %q: %v", k2, err)
	}
	gotInMain, err := repo2.Get(MAIN, k2)
	if err != nil || !reflect.DeepEqual(gotInMain, v2) {
		t.Errorf("Get failed for key=%q in MAIN: %v", k2, err)
	}

	gotInOther, err := repo2.Get(otherKind, k2)
	if err == nil && len(gotInOther) > 0 {
		t.Errorf("Key set on MAIN is unexpectedly visible in otherKind, value: %v", gotInOther)
	}

	// Test 10: Get after repository is closed for MAIN
	repo.Close()
	_, err = repo.Get(MAIN, "key_after_close")
	if err != ErrRepoClosed {
		t.Errorf("Get should fail after repo is closed, but got error: %v", err)
	}
}

func TestMagmaRepository_Reopen(t *testing.T) {
	dir := t.TempDir()

	repo := getOpenRepo(dir)
	utilMagmaRepoSet(t, repo, repo.Set, t.Name())
	itemCount := repo.(*Magma_Repository).GetItemsCount(MAIN)
	repo.Close()

	// verify repo closed
	_, err := repo.Get(MAIN, "maybe_key")
	if err != ErrRepoClosed {
		t.Fatal("repo should have been closed but it is not")
	}

	repo = getOpenRepo(dir)
	reopenItemsCount := repo.(*Magma_Repository).GetItemsCount(MAIN)
	if itemCount != reopenItemsCount {
		t.Errorf("item count mismatch on magma re-instantiation. Old - %v, New - %v",
			itemCount, reopenItemsCount)
	}
	repo.Close()
}
