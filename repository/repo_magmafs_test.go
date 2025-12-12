//go:build !community
// +build !community

package repository

import (
	"log/slog"
	"os"
	"reflect"
	"testing"

	"github.com/NightWing1998/testfs"
)

type fsMocker struct {
	*testfs.MockFS
	mountPath string
}

func (fmk *fsMocker) Close() error {
	if err := fmk.MockFS.Close(); err != nil {
		return err
	}
	return os.RemoveAll(fmk.mountPath)
}

var gLogger *slog.Logger

func init() {
	gLogger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level:     slog.LevelDebug,
		AddSource: false,
	}))
}

func testGetMockFsPath(t *testing.T) *fsMocker {
	t.Helper()
	// TODO: remove sync pool. its useless
	path, _ := os.MkdirTemp(os.TempDir(), "mockfs*")
	mockFs := testfs.New()

	mockFs.SetLogger(gLogger.With("lib", "testfs"))
	err := mockFs.Mount(t.Context(), path)
	if err != nil {
		panic(err)
	}

	go func() {
		logger := gLogger.With("lib", "gometa")
		for event := range mockFs.Events() {
			logger.Info("mock-fs event",
				slog.Any("type", event.Type),
				slog.String("path", event.Path),
				slog.Any("err", event.Error))
		}
	}()

	return &fsMocker{
		mountPath: path,
		MockFS:    mockFs,
	}
}

//////////////////////////////////////////////////
// RUN BASIC FUNCTIONAL TESTS TO VERIFY MOCK-FS //
//////////////////////////////////////////////////

func TestMagmaRepository_Set_MkFs(t *testing.T) {
	mocker := testGetMockFsPath(t)
	defer func() {
		err := mocker.Close()
		if err != nil {
			t.Logf("file mocker closed failed with error - %v", err)
		}
	}()

	dir := mocker.mountPath
	repo := getOpenRepo(dir)
	defer repo.Close()

	utilRepoSet(t, repo, repo.Set, t.Name())
}

func TestMagmaRepository_SetNoCommit_MkFs(t *testing.T) {
	mocker := testGetMockFsPath(t)
	defer func() {
		err := mocker.Close()
		if err != nil {
			t.Logf("file mocker closed failed with error - %v", err)
		}
	}()

	dir := mocker.mountPath
	repo := getOpenRepo(dir)
	defer repo.Close()

	utilRepoSet(t, repo, repo.SetNoCommit, t.Name())

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

func TestMagmaRepository_Delete_MkFs(t *testing.T) {
	mocker := testGetMockFsPath(t)
	defer func() {
		err := mocker.Close()
		if err != nil {
			t.Logf("file mocker closed failed with error - %v", err)
		}
	}()

	dir := mocker.mountPath
	repo := getOpenRepo(dir)
	defer repo.Close()

	utilRepoDelete(t, repo, repo.Set, repo.Delete, t.Name())

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

func TestMagmaRepository_DeleteNoCommit_MkFs(t *testing.T) {
	mocker := testGetMockFsPath(t)
	defer func() {
		err := mocker.Close()
		if err != nil {
			t.Logf("file mocker closed failed with error - %v", err)
		}
	}()

	dir := mocker.mountPath
	repo := getOpenRepo(dir)
	defer repo.Close()

	utilRepoDelete(t, repo, repo.Set, repo.DeleteNoCommit, t.Name())

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

func TestMagmaRepository_Get_MkFs(t *testing.T) {
	mocker := testGetMockFsPath(t)
	defer func() {
		err := mocker.Close()
		if err != nil {
			t.Logf("file mocker closed failed with error - %v", err)
		}
	}()

	dir := mocker.mountPath
	repo := getOpenRepo(dir)
	defer repo.Close()

	utilRepoGet(t, repo)
}

func TestMagmaRepository_EmptyValue_MkFs(t *testing.T) {
	mocker := testGetMockFsPath(t)
	defer func() {
		err := mocker.Close()
		if err != nil {
			t.Logf("file mocker closed failed with error - %v", err)
		}
	}()

	dir := mocker.mountPath
	repo := getOpenRepo(dir)
	defer repo.Close()

	var (
		res []byte
		err error
	)

	emptyKey1 := "empty_value_key1"
	emptyVal1 := []byte{}
	if err = repo.Set(MAIN, emptyKey1, emptyVal1); err != nil {
		t.Fatalf("Set failed for empty value key=%s: %v", emptyKey1, err)
	}
	res, err = repo.Get(MAIN, emptyKey1)
	if err != nil {
		t.Fatalf("Get failed for empty value key=%s: %v", emptyKey1, err)
	}
	if len(res) != 0 {
		t.Errorf(
			"Get returned non-empty value for empty value key=%s: expected length 0, got length %d, value: %v",
			emptyKey1,
			len(res),
			res,
		)
	}
	if !reflect.DeepEqual(res, emptyVal1) {
		t.Errorf(
			"Get mismatch for empty value key=%s: expected %v, got %v",
			emptyKey1,
			emptyVal1,
			res,
		)
	}

	emptyKey2 := "empty_value_key2"
	var emptyVal2 []byte
	if err = repo.Set(MAIN, emptyKey2, emptyVal2); err != nil {
		t.Fatalf("Set failed for nil value key=%s: %v", emptyKey2, err)
	}
	res, err = repo.Get(MAIN, emptyKey2)
	if err != nil {
		t.Fatalf("Get failed for nil value key=%s: %v", emptyKey2, err)
	}
	if len(res) != 0 {
		t.Errorf(
			"Get returned non-empty value for nil value key=%s: expected length 0, got length %d, value: %v",
			emptyKey2,
			len(res),
			res,
		)
	}

	overwriteKey := "overwrite_with_empty"
	nonEmptyVal := []byte("non_empty_value")
	if err = repo.Set(MAIN, overwriteKey, nonEmptyVal); err != nil {
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
	if err = repo.Set(MAIN, overwriteKey, []byte{}); err != nil {
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

func TestMagmaRepository_Reopen_MkFs(t *testing.T) {
	mocker := testGetMockFsPath(t)
	defer func() {
		err := mocker.Close()
		if err != nil {
			t.Logf("file mocker closed failed with error - %v", err)
		}
	}()

	dir := mocker.mountPath

	repo := getOpenRepo(dir)
	utilRepoSet(t, repo, repo.Set, t.Name())
	repo.Commit()
	itemCount := repo.(*Magma_Repository).getRepoStats(MAIN).ItemCount
	totalItemCount := repo.(*Magma_Repository).getRepoStats(MAIN).TotalItemCount
	repo.Close()

	_, err := repo.Get(MAIN, "maybe_key")
	storeErr := verifyMagmaStoreError(t, err, "Get after Close")
	if storeErr == nil || storeErr.Code() != ErrRepoClosedCode {
		t.Fatal("repo should have been closed but it is not")
	}

	repo = getOpenRepo(dir)
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

///////////////////////////////////
// TEST MAGMA REPO WITH FLAKY FS //
///////////////////////////////////

func TestMagmaRepository_Set_MkFs_WriteErrors(t *testing.T) {
	mocker := testGetMockFsPath(t)
	defer func() {
		err := mocker.Close()
		if err != nil {
			t.Logf("file mocker closed failed with error - %v", err)
		}
	}()

	dir := mocker.mountPath
	repo := getOpenRepo(dir)
	defer repo.Close()

	mocker.InjectErrorOnEvent(testfs.EventWrite, testfs.ErrInvalid)

	err := repo.Set(MAIN, "failed_write", []byte("hello"))
	if err == nil {
		t.Error("Expected repo.Set to fail when fs writes give error but it worked")
	} else if sErr, ok := err.(*StoreError); !ok || sErr == nil {
		t.Errorf("expected to see store error but got %T", err)
	}

	mocker.ClearErrorOnEvent(testfs.EventWrite)
	utilRepoSet(t, repo, repo.Set, t.Name())
}

func TestMagmaRepository_Get_MkFs_ReadErrors(t *testing.T) {
	mocker := testGetMockFsPath(t)

	defer func() {
		err := mocker.Close()
		if err != nil {
			t.Logf("file mocker closed failed with error - %v", err)
		}
	}()

	dir := mocker.mountPath
	{
		// use lower quota so we perform disk reads
		repo := getMagmaRepo(RepoFactoryParams{
			Dir:                        dir,
			MemoryQuota:                1 * 1024 * 1024,
			CompactionMinFileSize:      0,
			CompactionThresholdPercent: 30,
			CompactionTimerDur:         60000,
			EnableWAL:                  true,
			StoreType:                  MagmaStoreType,
		})
		utilRepoSet(t, repo, repo.Set, t.Name())
		repo.Close()
	}

	{
		repo := getMagmaRepo(RepoFactoryParams{
			Dir:                        dir,
			MemoryQuota:                1 * 1024 * 1024,
			CompactionMinFileSize:      0,
			CompactionThresholdPercent: 30,
			CompactionTimerDur:         60000,
			EnableWAL:                  true,
			StoreType:                  MagmaStoreType,
		})
		errEvent := testfs.EventRead

		mocker.InjectErrorOnEvent(errEvent, testfs.ErrInvalid)

		tc := setTestCases[0]
		res, err := repo.Get(MAIN, tc.key)
		if !assertEmptyRes(res, err) {
			t.Errorf("%v expected read to fail but got %v,%v", t.Name(), string(res), err)
		}

		mocker.ClearErrorOnEvent(errEvent)
		res, err = repo.Get(MAIN, tc.key)
		if !assertEqual(res, tc.value) {
			t.Errorf("%v expected read does not match value got %s X %s,%v",
				t.Name(), res, tc.value, err)
		}

		repo.Close()
	}
}
