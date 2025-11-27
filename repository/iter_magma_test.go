//go:build !community
// +build !community

package repository

import (
	"bytes"
	"fmt"
	"testing"
)

func TestMagmaIter_CreateSnapshots(t *testing.T) {
	dir := t.TempDir()

	repo := getOpenRepo(dir).(*Magma_Repository)
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
	if snapContainer.snap == nil {
		t.Fatalf("got nil snapshot from snapshot container")
	}
	if snapContainer.txnID != 1 {
		t.Fatalf("got unexpected txnID from snapshot container expected 1 but got %d",
			snapContainer.txnID)
	}
	if snapContainer.refCount != 0 {
		t.Fatalf("got unexpected refCount from snapshot container expected 0 but got %d",
			snapContainer.refCount)
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
	if snapContainer.snap == nil {
		t.Fatalf("got nil snapshot from new snapshot container")
	}
	if snapContainer.txnID != 2 {
		t.Fatalf("got unexpected txnID from new snapshot container expected 2 but got %d",
			snapContainer.txnID)
	}
	if snapContainer.refCount != 0 {
		t.Fatalf("got unexpected refCount from new snapshot container expected 0 but got %d",
			snapContainer.refCount)
	}
	snapContainer.refCount++

	err = repo.CreateSnapshot(MAIN, 2)
	if err != nil {
		t.Fatalf("failed to create new snapshot with error - %v", err)
	}

	if len(repo.snapshots[MAIN]) != 2 {
		t.Fatalf("failed to create new snapshot, expected 2 snapshots but got %d",
			len(repo.snapshots[MAIN]))
	}

	nonExistsKind := RepoKind(999)
	err = repo.CreateSnapshot(nonExistsKind, 3)
	if err == nil {
		t.Fatalf("expected error for creating snapshot for non-existent store, got none")
	}
	repo.Close()

	err = repo.CreateSnapshot(MAIN, 1)
	if err != ErrRepoClosed {
		t.Fatalf("expected CreateSnapshot to fail with store closed error but got %v", err)
	}
}

func TestMagmaIter_ReleaseSnapshots(t *testing.T) {
	dir := t.TempDir()

	repo := getOpenRepo(dir).(*Magma_Repository)
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
	repo.snapshots[MAIN][0].refCount++
	err = repo.CreateSnapshot(MAIN, 100)
	if err != nil {
		t.Fatalf("failed to create snapshot: %v", err)
	}

	repo.ReleaseSnapshot(MAIN, 100)
	for _, snapContainer := range repo.snapshots[MAIN] {
		if snapContainer.txnID == 100 && snapContainer.refCount != 0 {
			t.Errorf("expected refCount 0 after release, got %d", snapContainer.refCount)
		}
	}
}

func TestMagmaIter_AcquireSnapshot(t *testing.T) {
	dir := t.TempDir()

	repo := getOpenRepo(dir).(*Magma_Repository)
	defer repo.Close()

	txnid, iter, err := repo.AcquireSnapshot(MAIN)
	if err != nil || txnid != 0 || iter != nil {
		t.Fatalf("acquired snapshot when no snapshots exist should return nils but got: %v, txnid %d, iter %v",
			err, txnid, iter)
	}

	utilMagmaRepoSet(t, repo, repo.Set, t.Name())

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
	repo.pruneSnapshotsNoLock(MAIN)

	utilMagmaRepoDelete(t, repo, repo.SetNoCommit, repo.DeleteNoCommit, t.Name())

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
			if errs[j] != nil && errs[j] != ErrIterFail {
				t.Errorf("failed to get for iter[%d] i(%d) data: err(%v)", j, i, errs[j])
			}
		}

		for j := 0; j < len(iters); j++ {
			for k := j + 1; k < len(iters); k++ {
				if !compareFunc(keys[j], vals[j], keys[k], vals[k]) {
					t.Errorf("compare(keyA(%s), valA(%s), keyB(%s), valB(%s)) failed for iter %d and %d",
						keys[j], vals[j], keys[k], vals[k], j, k)
				}
			}
		}
	}
}

func getIter(t *testing.T, repo *Magma_Repository, startKey, endKey string) IRepoIterator {
	iter, err := repo.NewIterator(MAIN, startKey, endKey)
	if err != nil || iter == nil {
		t.Fatalf("failed to create iterator: err(%v), iter(%v)", err, iter)
	}
	return iter
}

func assertNoDataFromIter(t *testing.T, iter IRepoIterator) {
	nextKey, nextVal, err := iter.Next()
	if len(nextKey) > 0 || len(nextVal) > 0 || err != ErrIterFail {
		t.Fatalf("expected no data but got: key(%v), val(%v), err(%v)", nextKey, nextVal, err)
	}
}

func handleError(t *testing.T, err error, msg string) {
	if err != nil {
		t.Fatalf("%s: %v", msg, err)
	}
}

func TestMagmaIter_Iterations(t *testing.T) {
	key := func(i int) string {
		return fmt.Sprintf("key%d", i)
	}
	val := func(i int, prefix string) []byte {
		return []byte(fmt.Sprintf("%s%d", prefix, i))
	}

	dir := t.TempDir()

	repo := getOpenRepo(dir).(*Magma_Repository)

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
	compareMultiIterators(t, dataLen-1, func(keyA string, valA []byte, keyB string, valB []byte) bool {
		return keyA == keyB && !bytes.Equal(valA, valB)
	}, iterWithData, iterWithUpdatedData)

	compareMultiIterators(t, dataLen-1, func(keyA string, valA []byte, keyB string, valB []byte) bool {
		return keyA == keyB && bytes.Equal(valA, valB)
	}, iterWithUpdatedData2, iterWithSmallerStartKey)

	nextKey, nextVal, err := iterWithData.Next()
	if len(nextKey) == 0 || len(nextVal) == 0 || err != ErrIterFail {
		t.Fatalf("expected data and err but got: key(%s), val(%s), err(%v)", nextKey, nextVal, err)
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
		if (nextKey) != key(i) || len(nextVal) == 0 || (err != nil && err != ErrIterFail) {
			t.Fatalf("expected data and err but got: key(%s), val(%s), err(%v)", nextKey, nextVal, err)
		}
	}
	assertNoDataFromIter(t, iterWithEndKey)
	iterWithEndKey.Close()

	for i := 3; i < dataLen; i++ {
		nextKey, nextVal, err = iterWithStartKey.Next()
		if (nextKey) != key(i) || len(nextVal) == 0 || (err != nil && err != ErrIterFail) {
			t.Fatalf("expected data and err but got: key(%s), val(%s), err(%v)", nextKey, nextVal, err)
		}
	}
	assertNoDataFromIter(t, iterWithStartKey)
	iterWithStartKey.Close()

	for i := 3; i < 6; i++ {
		nextKey, nextVal, err = iterWithStartAndEndKey.Next()
		if (nextKey) != key(i) || len(nextVal) == 0 || (err != nil && err != ErrIterFail) {
			t.Fatalf("expected data and err but got: key(%s), val(%s), err(%v)", nextKey, nextVal, err)
		}
	}
	assertNoDataFromIter(t, iterWithStartAndEndKey)
	iterWithStartAndEndKey.Close()

	repo.Close()

	iter, err := repo.NewIterator(MAIN, "", "")
	if err != ErrRepoClosed || iter != nil {
		t.Fatal("got valid iter after repo close")
	}
}
