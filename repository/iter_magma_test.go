//go:build !community
// +build !community

package repository

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	c "github.com/couchbase/gometa/common"
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
	storeErr := verifyMagmaStoreError(t, err, "CreateSnapshot after Close")
	if storeErr == nil || storeErr.Code() != ErrRepoClosedCode {
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
	repo.pruneSnapshotsNoLock(MAIN)

	utilRepoDelete(t, repo, repo.SetNoCommit, repo.DeleteNoCommit, t.Name())

	handleError(t, repo.CreateSnapshot(MAIN, 2), t.Name())

	key, val, err := iter.Next()
	for ; err == nil; key, val, err = iter.Next() {
		if len(val) < 1*kib {
			t.Logf("key %v, val %s, err %v", key, val, err)
		}
	}

	iter.Close()

}

/*
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
} */

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
	if len(nextKey) == 0 || len(nextVal) == 0 || err != nil {
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
	storeErr := verifyMagmaStoreError(t, err, "NewIterator after Close")
	if storeErr == nil || storeErr.Code() != ErrRepoClosedCode || iter != nil {
		t.Fatal("got valid iter after repo close")
	}
}

/////////////////////////////////////////////////////////////////////////////
// Snapshot contents and lifetime
/////////////////////////////////////////////////////////////////////////////
//
// CreateSnapshot no longer forces the store to persist before taking its
// snapshot. It is called once per committed write, so that flush dominated the
// metadata write path.
//
// The tests above cover the create/acquire/prune bookkeeping structurally
// (container fields, refcounts, pruning, error cases), and TestMagmaIter_Iterations
// covers iterator contents through NewIterator. The tests below cover what a
// snapshot obtained through CreateSnapshot/AcquireSnapshot actually contains, and
// that it keeps those contents while the store is written to, committed and
// compacted underneath it.
//
// Not coverable here: durability across the loss of the machine. The recovery
// tests in repo_magma_test.go kill the process, which covers a process restart
// only.

// drainIterator collects an iterator into a map so contents can be asserted
// independently of ordering.
func drainIterator(t *testing.T, iter IRepoIterator) map[string]string {
	t.Helper()

	out := make(map[string]string)
	if iter == nil {
		return out
	}

	for {
		key, content, err := iter.Next()
		if err != nil {
			// Next returns an error to signal end of iteration.
			break
		}
		out[key] = string(content)
	}
	return out
}

// snapshotContents acquires the latest snapshot, drains it, and releases it.
// Returns the snapshot's txnid alongside the contents.
func snapshotContents(t *testing.T, repo IRepository, kind RepoKind) (c.Txnid, map[string]string) {
	t.Helper()

	txnid, iter, err := repo.AcquireSnapshot(kind)
	if err != nil {
		t.Fatalf("AcquireSnapshot failed: %v", err)
	}
	if iter == nil {
		t.Fatalf("AcquireSnapshot returned a nil iterator")
	}
	defer func() {
		iter.Close()
		repo.ReleaseSnapshot(kind, txnid)
	}()

	return txnid, drainIterator(t, iter)
}

// TestMagmaIter_SnapshotSeesUnflushedWrites is the core case: a snapshot taken
// without a preceding flush must still observe everything written before it. If
// this fails, a watcher would sync incomplete metadata.
func TestMagmaIter_SnapshotSeesUnflushedWrites(t *testing.T) {
	dir := t.TempDir()
	repo := getOpenRepo(dir)
	defer repo.Close()

	want := map[string]string{
		"/idx/defn/1":     "defn-one",
		"/idx/defn/2":     "defn-two",
		"/idx/topology/a": "topo-a",
	}
	for k, v := range want {
		if err := repo.Set(MAIN, k, []byte(v)); err != nil {
			t.Fatalf("Set(%s) failed: %v", k, err)
		}
	}

	if err := repo.CreateSnapshot(MAIN, 10); err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}

	txnid, got := snapshotContents(t, repo, MAIN)
	if txnid != 10 {
		t.Errorf("expected snapshot txnid 10, got %d", txnid)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("snapshot missing or wrong value for %s: expected %q, got %q", k, v, got[k])
		}
	}
}

// TestMagmaIter_SnapshotIsStablePointInTime verifies a snapshot does not observe
// writes made after it was taken. A watcher streams thousands of keys; if the
// snapshot tracked live state instead of pinning a version, the scan would see a
// moving target.
func TestMagmaIter_SnapshotIsStablePointInTime(t *testing.T) {
	dir := t.TempDir()
	repo := getOpenRepo(dir)
	defer repo.Close()

	if err := repo.Set(MAIN, "/stable/before", []byte("v1")); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if err := repo.CreateSnapshot(MAIN, 1); err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}

	// Acquire first so the snapshot is pinned across the subsequent writes.
	txnid, iter, err := repo.AcquireSnapshot(MAIN)
	if err != nil {
		t.Fatalf("AcquireSnapshot failed: %v", err)
	}
	defer func() {
		iter.Close()
		repo.ReleaseSnapshot(MAIN, txnid)
	}()

	if err := repo.Set(MAIN, "/stable/after", []byte("v2")); err != nil {
		t.Fatalf("Set after snapshot failed: %v", err)
	}
	if err := repo.Set(MAIN, "/stable/before", []byte("v1-modified")); err != nil {
		t.Fatalf("overwrite after snapshot failed: %v", err)
	}

	got := drainIterator(t, iter)

	if _, found := got["/stable/after"]; found {
		t.Errorf("snapshot leaked a key written after it was taken: /stable/after")
	}
	if got["/stable/before"] != "v1" {
		t.Errorf("snapshot should hold the pre-snapshot value, expected %q got %q",
			"v1", got["/stable/before"])
	}

	// The live store must show the new state, confirming the writes landed.
	live, err := repo.Get(MAIN, "/stable/before")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(live) != "v1-modified" {
		t.Errorf("live read should see the overwrite, expected %q got %q",
			"v1-modified", string(live))
	}
}

// TestMagmaIter_SnapshotPruneSparesReferenced verifies a snapshot being consumed
// is not retired by a concurrent CreateSnapshot, and that the refcount is what
// decides it. This is what lets a watcher finish streaming while writes continue.
func TestMagmaIter_SnapshotPruneSparesReferenced(t *testing.T) {
	dir := t.TempDir()
	repo := getOpenRepo(dir).(*Magma_Repository)
	defer repo.Close()

	if err := repo.Set(MAIN, "/pinned/key", []byte("pinned")); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if err := repo.CreateSnapshot(MAIN, 1); err != nil {
		t.Fatalf("CreateSnapshot(1) failed: %v", err)
	}

	// Two consumers of the same snapshot, as two watchers syncing concurrently.
	txnidA, iterA, err := repo.AcquireSnapshot(MAIN)
	if err != nil {
		t.Fatalf("first AcquireSnapshot failed: %v", err)
	}
	txnidB, iterB, err := repo.AcquireSnapshot(MAIN)
	if err != nil {
		t.Fatalf("second AcquireSnapshot failed: %v", err)
	}
	if txnidA != 1 || txnidB != 1 {
		t.Fatalf("expected both acquires to return txnid 1, got %d and %d", txnidA, txnidB)
	}
	if repo.snapshots[MAIN][0].refCount != 2 {
		t.Fatalf("expected refCount 2 after two acquires, got %d",
			repo.snapshots[MAIN][0].refCount)
	}

	// A commit lands mid-scan. Both must survive: 1 is referenced, 2 is newest.
	if err := repo.CreateSnapshot(MAIN, 2); err != nil {
		t.Fatalf("CreateSnapshot(2) failed: %v", err)
	}
	if len(repo.snapshots[MAIN]) != 2 {
		t.Fatalf("expected the referenced snapshot to survive pruning, got %d snapshots",
			len(repo.snapshots[MAIN]))
	}

	// A pinned iterator must still be usable.
	if got := drainIterator(t, iterA); got["/pinned/key"] != "pinned" {
		t.Errorf("pinned snapshot lost its contents, got %q", got["/pinned/key"])
	}

	iterA.Close()
	repo.ReleaseSnapshot(MAIN, txnidA)
	if repo.snapshots[MAIN][0].refCount != 1 {
		t.Errorf("expected refCount 1 after one release, got %d",
			repo.snapshots[MAIN][0].refCount)
	}

	// Still referenced by B, so another commit must not retire it.
	if err := repo.CreateSnapshot(MAIN, 3); err != nil {
		t.Fatalf("CreateSnapshot(3) failed: %v", err)
	}
	if len(repo.snapshots[MAIN]) != 2 {
		t.Errorf("a still-referenced snapshot was pruned")
	}

	iterB.Close()
	repo.ReleaseSnapshot(MAIN, txnidB)
	if repo.snapshots[MAIN][0].refCount != 0 {
		t.Errorf("expected refCount 0 after both releases, got %d",
			repo.snapshots[MAIN][0].refCount)
	}

	// Fully released, so the next commit should reclaim it.
	if err := repo.CreateSnapshot(MAIN, 4); err != nil {
		t.Fatalf("CreateSnapshot(4) failed: %v", err)
	}
	if len(repo.snapshots[MAIN]) != 1 {
		t.Errorf("expected the fully-released snapshot to be pruned, got %d snapshots",
			len(repo.snapshots[MAIN]))
	}
}

// TestMagmaIter_SnapshotsAreIndependent covers several consumers each pinning a
// different generation while writes continue. Each held snapshot must stay at its
// own point in time and none may observe another's writes.
func TestMagmaIter_SnapshotsAreIndependent(t *testing.T) {
	dir := t.TempDir()
	repo := getOpenRepo(dir)
	defer repo.Close()

	type held struct {
		txnid c.Txnid
		iter  IRepoIterator
		gen   int
	}
	var pinned []held

	// Three generations, each snapshotted and pinned before the next write.
	for gen := 1; gen <= 3; gen++ {
		if err := repo.Set(MAIN, fmt.Sprintf("/gen/%d", gen), []byte(fmt.Sprintf("g%d", gen))); err != nil {
			t.Fatalf("Set for generation %d failed: %v", gen, err)
		}
		if err := repo.CreateSnapshot(MAIN, c.Txnid(gen)); err != nil {
			t.Fatalf("CreateSnapshot(%d) failed: %v", gen, err)
		}
		txnid, iter, err := repo.AcquireSnapshot(MAIN)
		if err != nil {
			t.Fatalf("AcquireSnapshot for generation %d failed: %v", gen, err)
		}
		if txnid != c.Txnid(gen) {
			t.Fatalf("expected to pin txnid %d, got %d", gen, txnid)
		}
		pinned = append(pinned, held{txnid: txnid, iter: iter, gen: gen})
	}

	mRepo := repo.(*Magma_Repository)
	if len(mRepo.snapshots[MAIN]) != 3 {
		t.Fatalf("expected 3 pinned snapshots, got %d", len(mRepo.snapshots[MAIN]))
	}

	// A fourth commit lands while all three are still held.
	if err := repo.Set(MAIN, "/gen/4", []byte("g4")); err != nil {
		t.Fatalf("Set for generation 4 failed: %v", err)
	}
	if err := repo.CreateSnapshot(MAIN, 4); err != nil {
		t.Fatalf("CreateSnapshot(4) failed: %v", err)
	}

	for _, h := range pinned {
		got := drainIterator(t, h.iter)
		if len(got) != h.gen {
			t.Errorf("snapshot at generation %d should hold %d keys, got %d: %v",
				h.gen, h.gen, len(got), got)
		}
		for g := 1; g <= h.gen; g++ {
			key := fmt.Sprintf("/gen/%d", g)
			if got[key] != fmt.Sprintf("g%d", g) {
				t.Errorf("snapshot at generation %d missing %s, got %q", h.gen, key, got[key])
			}
		}
		for g := h.gen + 1; g <= 4; g++ {
			if _, found := got[fmt.Sprintf("/gen/%d", g)]; found {
				t.Errorf("snapshot at generation %d leaked a later write /gen/%d", h.gen, g)
			}
		}
		h.iter.Close()
		repo.ReleaseSnapshot(MAIN, h.txnid)
	}
}

// TestMagmaIter_SnapshotSurvivesNewIteratorFlush covers the pairing of the two
// snapshot paths. NewIterator was left unchanged and still flushes the store
// before taking its own snapshot. Both paths run on every indexer start: the
// embedded server creates a snapshot on MAIN at bootstrap, then loadDefn and
// loadTopology call NewIterator on the same store. A flush driven by one must not
// disturb a snapshot held by the other.
func TestMagmaIter_SnapshotSurvivesNewIteratorFlush(t *testing.T) {
	dir := t.TempDir()
	repo := getOpenRepo(dir)
	defer repo.Close()

	before := map[string]string{
		"/mixed/defn/1": "defn-one",
		"/mixed/defn/2": "defn-two",
	}
	for k, v := range before {
		if err := repo.Set(MAIN, k, []byte(v)); err != nil {
			t.Fatalf("Set(%s) failed: %v", k, err)
		}
	}

	if err := repo.CreateSnapshot(MAIN, 1); err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}
	txnid, iter, err := repo.AcquireSnapshot(MAIN)
	if err != nil {
		t.Fatalf("AcquireSnapshot failed: %v", err)
	}

	// A write lands after the snapshot was pinned.
	if err := repo.Set(MAIN, "/mixed/after", []byte("after")); err != nil {
		t.Fatalf("Set after snapshot failed: %v", err)
	}

	// NewIterator flushes MAIN and takes its own snapshot.
	nIter, err := repo.NewIterator(MAIN, "", "")
	if err != nil {
		t.Fatalf("NewIterator failed: %v", err)
	}
	fresh := drainIterator(t, nIter)
	nIter.Close()

	if fresh["/mixed/after"] != "after" {
		t.Errorf("NewIterator missed a committed key, got %q", fresh["/mixed/after"])
	}

	// The pinned snapshot must be untouched: still readable, still at its own
	// point in time.
	got := drainIterator(t, iter)
	iter.Close()
	repo.ReleaseSnapshot(MAIN, txnid)

	for k, v := range before {
		if got[k] != v {
			t.Errorf("pinned snapshot lost %s across NewIterator's flush: expected %q, got %q",
				k, v, got[k])
		}
	}
	if _, found := got["/mixed/after"]; found {
		t.Errorf("pinned snapshot picked up a post-snapshot write after the flush")
	}

	// A snapshot created after the mixed sequence must still be correct.
	if err := repo.CreateSnapshot(MAIN, 2); err != nil {
		t.Fatalf("CreateSnapshot after NewIterator failed: %v", err)
	}
	if _, latest := snapshotContents(t, repo, MAIN); latest["/mixed/after"] != "after" {
		t.Errorf("snapshot taken after the mixed sequence is missing a key, got %q",
			latest["/mixed/after"])
	}
}

// TestMagmaIter_SnapshotSurvivesCommitAndCompaction forces the two store-level
// operations most likely to retire whatever a held snapshot is reading. Before the
// change CreateSnapshot flushed first, so a snapshot never had to outlive later
// store activity.
func TestMagmaIter_SnapshotSurvivesCommitAndCompaction(t *testing.T) {
	dir := t.TempDir()
	repo := getOpenRepo(dir)
	defer repo.Close()

	want := map[string]string{
		"/compact/a": "va",
		"/compact/b": "vb",
		"/compact/c": "vc",
	}
	// Rewrite so compaction has duplicate versions to collapse.
	for i := 0; i < 5; i++ {
		for k, v := range want {
			if err := repo.Set(MAIN, k, []byte(v)); err != nil {
				t.Fatalf("Set(%s) failed: %v", k, err)
			}
		}
	}

	if err := repo.CreateSnapshot(MAIN, 1); err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}
	txnid, iter, err := repo.AcquireSnapshot(MAIN)
	if err != nil {
		t.Fatalf("AcquireSnapshot failed: %v", err)
	}

	if err := repo.Commit(); err != nil {
		t.Fatalf("Commit failed while a snapshot was held: %v", err)
	}
	if err := repo.CompactStores(); err != nil {
		t.Fatalf("CompactStores failed while a snapshot was held: %v", err)
	}

	got := drainIterator(t, iter)
	iter.Close()
	repo.ReleaseSnapshot(MAIN, txnid)

	for k, v := range want {
		if got[k] != v {
			t.Errorf("snapshot lost %s across commit and compaction: expected %q, got %q",
				k, v, got[k])
		}
	}
	if len(got) != len(want) {
		t.Errorf("expected %d keys after commit and compaction, got %d: %v",
			len(want), len(got), got)
	}
}

// TestMagmaIter_SnapshotSurvivesWriteChurn writes several MB while a snapshot is
// held, well past the memory quota getOpenRepo configures, so the store is forced
// to persist repeatedly underneath it. Previously every commit flushed, so a
// snapshot never had to outlive that.
func TestMagmaIter_SnapshotSurvivesWriteChurn(t *testing.T) {
	dir := t.TempDir()
	repo := getOpenRepo(dir)
	defer repo.Close()

	want := map[string]string{
		"/churn/pinned/1": "pinned-one",
		"/churn/pinned/2": "pinned-two",
	}
	for k, v := range want {
		if err := repo.Set(MAIN, k, []byte(v)); err != nil {
			t.Fatalf("Set(%s) failed: %v", k, err)
		}
	}

	if err := repo.CreateSnapshot(MAIN, 1); err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}
	txnid, iter, err := repo.AcquireSnapshot(MAIN)
	if err != nil {
		t.Fatalf("AcquireSnapshot failed: %v", err)
	}

	blob := make([]byte, 8*kib)
	for i := range blob {
		blob[i] = byte('a' + i%26)
	}
	for i := 0; i < 512; i++ {
		if err := repo.Set(MAIN, fmt.Sprintf("/churn/data/%04d", i), blob); err != nil {
			t.Fatalf("churn Set(%d) failed: %v", i, err)
		}
	}

	got := drainIterator(t, iter)
	iter.Close()
	repo.ReleaseSnapshot(MAIN, txnid)

	for k, v := range want {
		if got[k] != v {
			t.Errorf("snapshot lost %s under write churn: expected %q, got %q", k, v, got[k])
		}
	}
	if len(got) != len(want) {
		t.Errorf("snapshot should still hold exactly %d keys, got %d", len(want), len(got))
	}

	// The churn must be readable live, confirming the writes actually landed.
	if val, err := repo.Get(MAIN, "/churn/data/0511"); err != nil {
		t.Errorf("Get on a churned key failed: %v", err)
	} else if len(val) != len(blob) {
		t.Errorf("churned value truncated: expected %d bytes, got %d", len(blob), len(val))
	}
}

// TestMagmaIter_SnapshotConcurrentAccess runs writers committing (so
// CreateSnapshot prunes) against readers holding and draining snapshots. That is
// the watcher-during-DDL shape: commits are serialized upstream, but a metadata
// provider syncing on another goroutine can be mid-scan throughout. A snapshot
// pruned while an iterator is reading it would be freed underneath that iterator,
// which the race detector cannot see, so the reader asserts on what it read.
func TestMagmaIter_SnapshotConcurrentAccess(t *testing.T) {
	dir := t.TempDir()
	repo := getOpenRepo(dir)
	defer repo.Close()

	const seedKeys = 50
	for i := 0; i < seedKeys; i++ {
		if err := repo.Set(MAIN, fmt.Sprintf("/conc/seed/%03d", i), []byte("seed")); err != nil {
			t.Fatalf("seed Set failed: %v", err)
		}
	}
	if err := repo.CreateSnapshot(MAIN, 1); err != nil {
		t.Fatalf("initial CreateSnapshot failed: %v", err)
	}

	var (
		txnCounter atomic.Uint64
		errMu      sync.Mutex
		errs       []string
		wg         sync.WaitGroup
	)
	txnCounter.Store(1)

	record := func(format string, args ...interface{}) {
		errMu.Lock()
		defer errMu.Unlock()
		if len(errs) < 20 {
			errs = append(errs, fmt.Sprintf(format, args...))
		}
	}

	const iterations = 200

	// Writers: Set then CreateSnapshot, as a committed write does.
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				key := fmt.Sprintf("/conc/w%d/%04d", id, i)
				if err := repo.Set(MAIN, key, []byte("v")); err != nil {
					record("writer %d: Set failed: %v", id, err)
					return
				}
				txnid := c.Txnid(txnCounter.Add(1))
				if err := repo.CreateSnapshot(MAIN, txnid); err != nil {
					record("writer %d: CreateSnapshot(%d) failed: %v", id, txnid, err)
					return
				}
			}
		}(w)
	}

	// Readers: acquire, drain fully, release -- the watcher sync pattern.
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				txnid, iter, err := repo.AcquireSnapshot(MAIN)
				if err != nil {
					record("reader %d: AcquireSnapshot failed: %v", id, err)
					return
				}
				if iter == nil {
					record("reader %d: AcquireSnapshot returned nil iterator", id)
					return
				}

				count := 0
				for {
					key, _, nerr := iter.Next()
					if nerr != nil {
						break
					}
					if key == "" {
						record("reader %d: empty key from snapshot %d", id, txnid)
					}
					count++
				}

				iter.Close()
				repo.ReleaseSnapshot(MAIN, txnid)

				// Every snapshot is at least the seed generation; one truncated
				// by a concurrent prune would come back short.
				if count < seedKeys {
					record("reader %d: snapshot %d held only %d keys, expected >= %d",
						id, txnid, count, seedKeys)
				}
			}
		}(r)
	}

	wg.Wait()

	errMu.Lock()
	defer errMu.Unlock()
	for _, e := range errs {
		t.Error(e)
	}

	// No reader holds anything now, so the next commit must collapse the list
	// back to one, i.e. concurrency did not leak refcounts.
	if err := repo.CreateSnapshot(MAIN, c.Txnid(txnCounter.Add(1))); err != nil {
		t.Fatalf("final CreateSnapshot failed: %v", err)
	}
	mRepo := repo.(*Magma_Repository)
	mRepo.Lock()
	live := len(mRepo.snapshots[MAIN])
	mRepo.Unlock()
	if live != 1 {
		t.Errorf("expected all snapshots to be reclaimable after the run, %d still pinned", live)
	}
}
