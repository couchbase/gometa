//go:build !community
// +build !community

package repository

import (
	"bytes"
	"io"
	"os"
	"os/exec"
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
	repo.Commit()
	itemCount := repo.(*Magma_Repository).getRepoStats(MAIN).ItemCount
	totalItemCount := repo.(*Magma_Repository).getRepoStats(MAIN).TotalItemCount
	repo.Close()

	// verify repo closed
	_, err := repo.Get(MAIN, "maybe_key")
	storeErr := verifyMagmaStoreError(t, err, "Get after Close")
	if storeErr == nil || storeErr.Code() != ErrRepoClosedCode {
		t.Fatal("repo should have been closed but it is not")
	}

	repo = getOpenRepo(dir)
	verifyMigrationMarkerExists(t, dir)
	reopenItemsCount := repo.(*Magma_Repository).getRepoStats(MAIN).ItemCount
	reopenTotalItemsCount := repo.(*Magma_Repository).getRepoStats(MAIN).TotalItemCount
	if itemCount != reopenItemsCount {
		t.Errorf("item count mismatch on magma re-instantiation. Old - %v, New - %v",
			itemCount, reopenItemsCount)
	}
	if totalItemCount != reopenTotalItemsCount {
		t.Errorf("total item count mismatch on magma re-instantiation. Old - %v, New - %v",
			totalItemCount, reopenTotalItemsCount)
	}
	repo.Close()
}

func TestMagmaRepository_SetNoCommitPersistsAfterReopen(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "metadata_repo") // use static paths here
	if os.Getenv(CHILD_PROC_TEST_ENV) != "1" {
		os.RemoveAll(dir)
		os.MkdirAll(dir, 0x0777)
	}

	key := "persist_no_commit_key"
	val := []byte("persist-no-commit-value")

	t.Run("ValSetter", runInChildProc(func(t *testing.T) {
		repo := getOpenRepo(dir)
		verifyMigrationMarkerExists(t, dir)

		if err := repo.SetNoCommit(MAIN, key, val); err != nil {
			verifyStoreErrorForRepo(t, repo, err, "SetNoCommit")
			t.Fatalf("SetNoCommit failed for key=%s: %v", key, err)
		}

		os.Exit(0)
	}))

	t.Run("ValGetter", runInChildProc(func(t *testing.T) {
		repo := getOpenRepo(dir)
		verifyMigrationMarkerExists(t, dir)
		defer repo.Close()

		got, err := repo.Get(MAIN, key)
		if err != nil {
			verifyStoreErrorForRepo(t, repo, err, "Get after reopen (SetNoCommit)")
			t.Fatalf("Get failed after reopen for key=%s: %v", key, err)
		}

		if !reflect.DeepEqual(got, val) {
			t.Fatalf("value mismatch after reopen for key=%s: expected %v, got %v",
				key, val, got)
		}
	}))
}

func TestMagmaRepository_DeletePersistsAfterReopen(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "metadata_repo")
	if os.Getenv(CHILD_PROC_TEST_ENV) != "1" {
		os.RemoveAll(dir)
		os.MkdirAll(dir, 0x0777)
	}

	key := "delete_no_commit_key"
	val := []byte("delete-no-commit-value")

	t.Run("ValOps", runInChildProc(func(t *testing.T) {
		defer func() {
			e := recover()
			log.Current.Infof("recovered panic - %v", e)
		}()
		repo := getOpenRepo(dir)
		verifyMigrationMarkerExists(t, dir)

		if err := repo.Set(MAIN, key, val); err != nil {
			verifyStoreErrorForRepo(t, repo, err, "Set before DeleteNoCommit")
			t.Fatalf("Set failed for key=%s: %v", key, err)
		}

		if err := repo.Delete(MAIN, key); err != nil {
			verifyStoreErrorForRepo(t, repo, err, "DeleteNoCommit")
			t.Fatalf("DeleteNoCommit failed for key=%s: %v", key, err)
		}

		// TODO: replace with testcode induced functions
		panic("induce panic")
	}))

	t.Run("ValTest", runInChildProc(func(t *testing.T) {
		repo := getOpenRepo(dir)
		verifyMigrationMarkerExists(t, dir)

		res, err := repo.Get(MAIN, key)
		storeErr := verifyMagmaStoreError(t, err, "Get after reopen (DeleteNoCommit)")
		if err == nil {
			t.Fatalf("expected ErrResultNotFoundCode after reopen for key=%s, got value: %v", key, res)
		}
		if storeErr == nil || storeErr.Code() != ErrResultNotFoundCode {
			t.Fatalf("expected ErrResultNotFoundCode after reopen for key=%s, got: %v", key, storeErr)
		}

		repo.Close()
	}))
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

const CHILD_PROC_TEST_ENV = "CHILD_EXEC"

type linePrefixWriter struct {
	dst         io.Writer
	prefix      []byte
	atLineStart bool
}

func newLinePrefixWriter(dst io.Writer, prefix string) *linePrefixWriter {
	return &linePrefixWriter{
		dst:         dst,
		prefix:      []byte(prefix),
		atLineStart: true,
	}
}

func (w *linePrefixWriter) Write(p []byte) (int, error) {
	written := 0

	for len(p) > 0 {
		if w.atLineStart {
			if _, err := w.dst.Write(w.prefix); err != nil {
				return written, err
			}
			w.atLineStart = false
		}

		idx := bytes.IndexByte(p, '\n')
		if idx == -1 {
			n, err := w.dst.Write(p)
			written += n
			return written, err
		}

		n, err := w.dst.Write(p[:idx+1])
		written += n
		if err != nil {
			return written, err
		}
		w.atLineStart = true
		p = p[idx+1:]
	}

	return written, nil
}

func runInChildProc(testFn func(subt *testing.T)) func(*testing.T) {
	return func(t *testing.T) {
		t.Helper()

		if os.Getenv(CHILD_PROC_TEST_ENV) == "1" {
			log.Current.Infof("[child] running in child process mode. executing test %v", t.Name())
			testFn(t)
		} else {
			// spin up separate process
			env := append(os.Environ(), "CHILD_EXEC=1")

			bin, err := os.Executable()
			handleError(t, err, "failed to get binary for current process")

			childProc := exec.Command(bin, "-test.v", "-test.run", t.Name())

			childProc.Env = env

			childProc.Stderr = newLinePrefixWriter(os.Stderr, "\t")
			childProc.Stdout = newLinePrefixWriter(os.Stdout, "\t")
			childProc.Stdin = nil

			handleError(t, childProc.Start(), "failed to start child process")

			log.Current.Infof("[parent] started child process to run test %v in PID- %v",
				t.Name(), childProc.Process.Pid)

			err = childProc.Wait()
			if err != nil && err.(*exec.ExitError) != nil {
				t.Errorf("child process non-0 exit - %v", err)
			}
		}
	}
}
