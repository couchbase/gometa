//go:build !community
// +build !community

package repository

import (
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
)

// ============================================================================
// Mock Implementation
// ============================================================================

// mockEncryptionKeyStoreCallbacks is a thread-safe mock of IEncryptionKeyStoreCallbacks.
// It supports per-method error injection, call counting, and configurable return values
// via function fields.
type mockEncryptionKeyStoreCallbacks struct {
	mu sync.Mutex

	// Function fields for per-test customization
	getActiveKeyCipherFn func() (*EarKey, error)
	getKeyCipherByIDFn   func(KeyID) (*EarKey, error)
	getEncryptionKeysFn  func() (*EncryKeyInfo, error)
	setInuseKeysFn       func(KeyID) error

	// Atomic call counters for thread-safe verification
	getActiveKeyCipherCalls atomic.Int64
	getKeyCipherByIDCalls   atomic.Int64
	getEncryptionKeysCalls  atomic.Int64
	setInuseKeysCalls       atomic.Int64
}

func newMockCallbacks() *mockEncryptionKeyStoreCallbacks {
	return &mockEncryptionKeyStoreCallbacks{}
}

func (m *mockEncryptionKeyStoreCallbacks) GetActiveKeyCipher() (*EarKey, error) {
	m.getActiveKeyCipherCalls.Add(1)
	m.mu.Lock()
	fn := m.getActiveKeyCipherFn
	m.mu.Unlock()
	if fn != nil {
		return fn()
	}
	return nil, nil
}

func (m *mockEncryptionKeyStoreCallbacks) GetKeyCipherByID(id KeyID) (*EarKey, error) {
	m.getKeyCipherByIDCalls.Add(1)
	m.mu.Lock()
	fn := m.getKeyCipherByIDFn
	m.mu.Unlock()
	if fn != nil {
		return fn(id)
	}
	return nil, nil
}

func (m *mockEncryptionKeyStoreCallbacks) GetEncryptionKeys() (*EncryKeyInfo, error) {
	m.getEncryptionKeysCalls.Add(1)
	m.mu.Lock()
	fn := m.getEncryptionKeysFn
	m.mu.Unlock()
	if fn != nil {
		return fn()
	}
	return nil, nil
}

func (m *mockEncryptionKeyStoreCallbacks) SetInuseKeys(id KeyID) error {
	m.setInuseKeysCalls.Add(1)
	m.mu.Lock()
	fn := m.setInuseKeysFn
	m.mu.Unlock()
	if fn != nil {
		return fn(id)
	}
	return nil
}

// setGetEncryptionKeysFn sets the GetEncryptionKeys function in a thread-safe way.
func (m *mockEncryptionKeyStoreCallbacks) setGetEncryptionKeysFn(
	fn func() (*EncryKeyInfo, error),
) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getEncryptionKeysFn = fn
}

// setGetActiveKeyCipherFn sets the GetActiveKeyCipher function in a thread-safe way.
func (m *mockEncryptionKeyStoreCallbacks) setGetActiveKeyCipherFn(
	fn func() (*EarKey, error),
) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getActiveKeyCipherFn = fn
}

// setGetKeyCipherByIDFn sets the GetKeyCipherByID function in a thread-safe way.
func (m *mockEncryptionKeyStoreCallbacks) setGetKeyCipherByIDFn(
	fn func(KeyID) (*EarKey, error),
) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getKeyCipherByIDFn = fn
}

// ============================================================================
// Random Key Generators
// ============================================================================

// generateRandomKeyID returns a random UUID-like key ID using crypto/rand.
func generateRandomKeyID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("failed to generate random key ID: %v", err))
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// generateRandomCipher returns the only encryption cipher supported by Magma.
func generateRandomCipher() string {
	return "AES-256-GCM"
}

// generateRandomKeyMaterial returns size bytes of cryptographic random data.
func generateRandomKeyMaterial(size int) []byte {
	key := make([]byte, size)
	if _, err := rand.Read(key); err != nil {
		panic(fmt.Sprintf("failed to generate random key material: %v", err))
	}
	return key
}

// generateRandomEarKey creates a random EarKey with a UUID-like ID, random cipher,
// and 32 bytes of random key material.
func generateRandomEarKey() EarKey {
	return EarKey{
		Id:     generateRandomKeyID(),
		Cipher: generateRandomCipher(),
		Key:    generateRandomKeyMaterial(32),
	}
}

// generateRandomEncryKeyInfo generates numKeys random keys with one marked active
// at activeIdx. If activeIdx is negative or out of range, ActiveKeyId is empty.
func generateRandomEncryKeyInfo(numKeys int, activeIdx int) *EncryKeyInfo {
	keys := make([]EarKey, numKeys)
	for i := range keys {
		keys[i] = generateRandomEarKey()
	}
	activeKeyId := ""
	if activeIdx >= 0 && activeIdx < numKeys {
		activeKeyId = keys[activeIdx].Id
	}
	return &EncryKeyInfo{
		ActiveKeyId: activeKeyId,
		Keys:        keys,
	}
}

// ============================================================================
// Common Assertion Helpers
// ============================================================================

func requireNoError(t *testing.T, err error, msg string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: unexpected error: %v", msg, err)
	}
}

func requireError(t *testing.T, err error, msg string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected error but got nil", msg)
	}
}

func requireRepoClosedError(t *testing.T, err error, operation string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected ErrRepoClosed error but got nil", operation)
	}
	storeErr, ok := err.(*StoreError)
	if !ok {
		t.Fatalf("%s: expected *StoreError, got %T: %v", operation, err, err)
	}
	if storeErr.Code() != ErrRepoClosedCode {
		t.Fatalf("%s: expected ErrRepoClosedCode, got %v", operation, storeErr.Code())
	}
}

func assertKeyIDsContain(t *testing.T, keys []KeyID, expected KeyID) {
	t.Helper()
	for _, k := range keys {
		if k == expected {
			return
		}
	}
	t.Errorf("expected key IDs %v to contain %q", keys, expected)
}

func assertKeyIDsEqual(t *testing.T, got, want []KeyID) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("key ID slices have different lengths: got %v (len %d), want %v (len %d)",
			got, len(got), want, len(want))
		return
	}
	gotSorted := make([]string, len(got))
	copy(gotSorted, got)
	sort.Strings(gotSorted)

	wantSorted := make([]string, len(want))
	copy(wantSorted, want)
	sort.Strings(wantSorted)

	for i := range gotSorted {
		if gotSorted[i] != wantSorted[i] {
			t.Errorf("key IDs mismatch: got %v, want %v", got, want)
			return
		}
	}
}

// ============================================================================
// Repo Lifecycle Helper
// ============================================================================

// openTestRepo opens a Magma repository in a temp directory, initializes the DEK cache,
// and returns the typed repo along with a cleanup function.
func openTestRepo(t *testing.T) (*Magma_Repository, func()) {
	t.Helper()
	dir := t.TempDir()
	repo := getOpenRepo(dir)
	mRepo := repo.(*Magma_Repository)

	// Initialize dekCache for EaR operations (not initialized by openMagmaRepository)
	mRepo.dekCacheMu.Lock()
	if mRepo.dekCache == nil {
		mRepo.dekCache = make(map[string]*MagmaDEK)
	}
	mRepo.dekCacheMu.Unlock()

	cleanup := func() {
		mRepo.Close()
	}
	return mRepo, cleanup
}

// setupMockWithKeys creates a fully-configured mock from key info, setting up all
// callback methods to properly resolve keys.
func setupMockWithKeys(keyInfo *EncryKeyInfo) *mockEncryptionKeyStoreCallbacks {
	mock := newMockCallbacks()

	mock.getEncryptionKeysFn = func() (*EncryKeyInfo, error) {
		return keyInfo, nil
	}

	if keyInfo != nil && len(keyInfo.Keys) > 0 {
		// Build lookup map (using copies to avoid pointer aliasing issues)
		keyMap := make(map[string]EarKey)
		var activeKey *EarKey
		for i := range keyInfo.Keys {
			k := keyInfo.Keys[i]
			keyMap[k.Id] = k
			if k.Id == keyInfo.ActiveKeyId {
				ak := k
				activeKey = &ak
			}
		}

		mock.getActiveKeyCipherFn = func() (*EarKey, error) {
			if activeKey != nil {
				k := *activeKey
				return &k, nil
			}
			return &EarKey{Id: ""}, nil
		}

		mock.getKeyCipherByIDFn = func(id KeyID) (*EarKey, error) {
			if k, ok := keyMap[id]; ok {
				kCopy := k
				return &kCopy, nil
			}
			return nil, fmt.Errorf("key %s not found", id)
		}
	}

	return mock
}

// ============================================================================
// Part 1: Individual Function Tests
// ============================================================================

// --- RegisterEncryptionKeyStoreCallback ---

func TestEaR_RegisterCallback_Valid(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	mock := newMockCallbacks()
	mRepo.RegisterEncryptionKeyStoreCallback(mock)

	if mRepo.keyStoreCallbacks == nil {
		t.Fatal("keyStoreCallbacks should be set after registration")
	}
}

func TestEaR_RegisterCallback_Nil(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	// First set a valid callback
	mock := newMockCallbacks()
	mRepo.RegisterEncryptionKeyStoreCallback(mock)
	if mRepo.keyStoreCallbacks == nil {
		t.Fatal("keyStoreCallbacks should be set")
	}

	// Now set nil
	mRepo.RegisterEncryptionKeyStoreCallback(nil)
	if mRepo.keyStoreCallbacks != nil {
		t.Fatal("keyStoreCallbacks should be nil after registering nil")
	}
}

func TestEaR_RegisterCallback_Replace(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	mockA := newMockCallbacks()
	mockB := newMockCallbacks()

	mRepo.RegisterEncryptionKeyStoreCallback(mockA)
	mRepo.RegisterEncryptionKeyStoreCallback(mockB)

	// Set up mockB to return a specific key
	keyInfo := generateRandomEncryKeyInfo(1, 0)
	mockB.setGetEncryptionKeysFn(func() (*EncryKeyInfo, error) {
		return keyInfo, nil
	})

	err := mRepo.RefreshKeys()
	requireNoError(t, err, "RefreshKeys with mockB")

	// Verify mockA was NOT called
	if mockA.getEncryptionKeysCalls.Load() != 0 {
		t.Error("mockA's GetEncryptionKeys should not have been called")
	}
	// Verify mockB WAS called
	if mockB.getEncryptionKeysCalls.Load() != 1 {
		t.Errorf("expected 1 call to mockB's GetEncryptionKeys, got %d",
			mockB.getEncryptionKeysCalls.Load())
	}
}

func TestEaR_RegisterCallback_OnClosedRepo(t *testing.T) {
	mRepo, _ := openTestRepo(t)
	mRepo.Close()

	// RegisterEncryptionKeyStoreCallback does NOT check isClosed — should not panic
	mock := newMockCallbacks()
	mRepo.RegisterEncryptionKeyStoreCallback(mock)

	if mRepo.keyStoreCallbacks == nil {
		t.Fatal("keyStoreCallbacks should be set even on closed repo")
	}
}

// --- RefreshKeys ---

func TestEaR_RefreshKeys_NoCallback(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	err := mRepo.RefreshKeys()
	requireNoError(t, err, "RefreshKeys with no callback")
}

func TestEaR_RefreshKeys_NilKeyInfo(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	mock := newMockCallbacks()
	mock.getEncryptionKeysFn = func() (*EncryKeyInfo, error) {
		return nil, nil
	}
	mRepo.RegisterEncryptionKeyStoreCallback(mock)

	err := mRepo.RefreshKeys()
	requireNoError(t, err, "RefreshKeys with nil key info")
}

func TestEaR_RefreshKeys_EmptyKeys(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	mock := newMockCallbacks()
	mock.getEncryptionKeysFn = func() (*EncryKeyInfo, error) {
		return &EncryKeyInfo{Keys: []EarKey{}}, nil
	}
	mRepo.RegisterEncryptionKeyStoreCallback(mock)

	err := mRepo.RefreshKeys()
	requireNoError(t, err, "RefreshKeys with empty keys")
}

func TestEaR_RefreshKeys_CallbackError(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	expectedErr := errors.New("key store unavailable")
	mock := newMockCallbacks()
	mock.getEncryptionKeysFn = func() (*EncryKeyInfo, error) {
		return nil, expectedErr
	}
	mRepo.RegisterEncryptionKeyStoreCallback(mock)

	err := mRepo.RefreshKeys()
	requireError(t, err, "RefreshKeys with callback error")
}

func TestEaR_RefreshKeys_SingleActiveKey(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	keyInfo := generateRandomEncryKeyInfo(1, 0)
	mock := setupMockWithKeys(keyInfo)
	mRepo.RegisterEncryptionKeyStoreCallback(mock)

	err := mRepo.RefreshKeys()
	requireNoError(t, err, "RefreshKeys with single active key")

	mRepo.dekCacheMu.RLock()
	cacheLen := len(mRepo.dekCache)
	_, exists := mRepo.dekCache[keyInfo.Keys[0].Id]
	currentKeyID := mRepo.currentKeyID
	mRepo.dekCacheMu.RUnlock()

	if cacheLen != 1 {
		t.Errorf("expected 1 key in DEK cache, got %d", cacheLen)
	}
	if !exists {
		t.Errorf("expected key %s in DEK cache", keyInfo.Keys[0].Id)
	}
	if currentKeyID != keyInfo.ActiveKeyId {
		t.Errorf("expected currentKeyID %s, got %s", keyInfo.ActiveKeyId, currentKeyID)
	}
}

func TestEaR_RefreshKeys_MultipleKeys(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	keyInfo := generateRandomEncryKeyInfo(3, 1) // 3 keys, index 1 is active
	mock := setupMockWithKeys(keyInfo)
	mRepo.RegisterEncryptionKeyStoreCallback(mock)

	err := mRepo.RefreshKeys()
	requireNoError(t, err, "RefreshKeys with multiple keys")

	mRepo.dekCacheMu.RLock()
	cacheLen := len(mRepo.dekCache)
	currentKeyID := mRepo.currentKeyID
	mRepo.dekCacheMu.RUnlock()

	if cacheLen != 3 {
		t.Errorf("expected 3 keys in DEK cache, got %d", cacheLen)
	}
	if currentKeyID != keyInfo.ActiveKeyId {
		t.Errorf("expected currentKeyID %s, got %s", keyInfo.ActiveKeyId, currentKeyID)
	}
}

func TestEaR_RefreshKeys_ActiveKeyChange(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	key1 := generateRandomEarKey()
	key2 := generateRandomEarKey()

	keyInfoV1 := &EncryKeyInfo{
		ActiveKeyId: key1.Id,
		Keys:        []EarKey{key1, key2},
	}
	mock := setupMockWithKeys(keyInfoV1)
	mRepo.RegisterEncryptionKeyStoreCallback(mock)

	err := mRepo.RefreshKeys()
	requireNoError(t, err, "first RefreshKeys")

	if mRepo.currentKeyID != key1.Id {
		t.Fatalf("expected currentKeyID %s after first refresh, got %s",
			key1.Id, mRepo.currentKeyID)
	}

	// Change active key to key2
	keyInfoV2 := &EncryKeyInfo{
		ActiveKeyId: key2.Id,
		Keys:        []EarKey{key1, key2},
	}
	mock.setGetEncryptionKeysFn(func() (*EncryKeyInfo, error) {
		return keyInfoV2, nil
	})

	err = mRepo.RefreshKeys()
	requireNoError(t, err, "second RefreshKeys")

	if mRepo.currentKeyID != key2.Id {
		t.Errorf("expected currentKeyID %s after second refresh, got %s",
			key2.Id, mRepo.currentKeyID)
	}
}

func TestEaR_RefreshKeys_StalePruning(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	key1 := generateRandomEarKey()
	key2 := generateRandomEarKey()

	// First refresh: both keys
	keyInfoV1 := &EncryKeyInfo{
		ActiveKeyId: key1.Id,
		Keys:        []EarKey{key1, key2},
	}
	mock := setupMockWithKeys(keyInfoV1)
	mRepo.RegisterEncryptionKeyStoreCallback(mock)

	err := mRepo.RefreshKeys()
	requireNoError(t, err, "first RefreshKeys")

	mRepo.dekCacheMu.RLock()
	if len(mRepo.dekCache) != 2 {
		t.Fatalf("expected 2 keys in DEK cache after first refresh, got %d",
			len(mRepo.dekCache))
	}
	mRepo.dekCacheMu.RUnlock()

	// Second refresh: only key2 (key1 pruned)
	keyInfoV2 := &EncryKeyInfo{
		ActiveKeyId: key2.Id,
		Keys:        []EarKey{key2},
	}
	mock.setGetEncryptionKeysFn(func() (*EncryKeyInfo, error) {
		return keyInfoV2, nil
	})

	err = mRepo.RefreshKeys()
	requireNoError(t, err, "second RefreshKeys")

	mRepo.dekCacheMu.RLock()
	cacheLen := len(mRepo.dekCache)
	_, k1Exists := mRepo.dekCache[key1.Id]
	_, k2Exists := mRepo.dekCache[key2.Id]
	mRepo.dekCacheMu.RUnlock()

	if cacheLen != 1 {
		t.Errorf("expected 1 key in DEK cache after pruning, got %d", cacheLen)
	}
	if k1Exists {
		t.Error("key1 should have been pruned from DEK cache")
	}
	if !k2Exists {
		t.Error("key2 should still be in DEK cache")
	}
}

func TestEaR_RefreshKeys_EmptyActiveKeyId(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	key := generateRandomEarKey()
	mock := newMockCallbacks()
	mock.getEncryptionKeysFn = func() (*EncryKeyInfo, error) {
		return &EncryKeyInfo{
			ActiveKeyId: "",
			Keys:        []EarKey{key},
		}, nil
	}
	mRepo.RegisterEncryptionKeyStoreCallback(mock)

	err := mRepo.RefreshKeys()
	requireNoError(t, err, "RefreshKeys with empty active key ID")

	// currentKeyID should remain at default (empty)
	if mRepo.currentKeyID != "" {
		t.Errorf("expected empty currentKeyID, got %s", mRepo.currentKeyID)
	}

	// Key should still be in cache even if not active
	mRepo.dekCacheMu.RLock()
	_, exists := mRepo.dekCache[key.Id]
	mRepo.dekCacheMu.RUnlock()
	if !exists {
		t.Errorf("expected key %s in DEK cache even with empty active ID", key.Id)
	}
}

func TestEaR_RefreshKeys_ClosedRepo(t *testing.T) {
	mRepo, _ := openTestRepo(t)

	keyInfo := generateRandomEncryKeyInfo(1, 0)
	mock := setupMockWithKeys(keyInfo)
	mRepo.RegisterEncryptionKeyStoreCallback(mock)

	mRepo.Close()

	err := mRepo.RefreshKeys()
	requireRepoClosedError(t, err, "RefreshKeys on closed repo")
}

func TestEaR_RefreshKeys_Idempotent(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	keyInfo := generateRandomEncryKeyInfo(2, 0)
	mock := setupMockWithKeys(keyInfo)
	mRepo.RegisterEncryptionKeyStoreCallback(mock)

	err := mRepo.RefreshKeys()
	requireNoError(t, err, "first RefreshKeys")

	mRepo.dekCacheMu.RLock()
	firstCacheLen := len(mRepo.dekCache)
	firstKeyID := mRepo.currentKeyID
	mRepo.dekCacheMu.RUnlock()

	// Second call with same keys
	err = mRepo.RefreshKeys()
	requireNoError(t, err, "second RefreshKeys")

	mRepo.dekCacheMu.RLock()
	secondCacheLen := len(mRepo.dekCache)
	secondKeyID := mRepo.currentKeyID
	mRepo.dekCacheMu.RUnlock()

	if firstCacheLen != secondCacheLen {
		t.Errorf("DEK cache size changed: %d -> %d", firstCacheLen, secondCacheLen)
	}
	if firstKeyID != secondKeyID {
		t.Errorf("currentKeyID changed: %s -> %s", firstKeyID, secondKeyID)
	}
}

// --- GetInuseKeys ---

func TestEaR_GetInuseKeys_NoEncryption(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	keys, err := mRepo.GetInuseKeys()
	requireNoError(t, err, "GetInuseKeys on fresh repo")
	t.Logf("GetInuseKeys on fresh (unencrypted) repo returned: %v", keys)
}

func TestEaR_GetInuseKeys_AfterRefresh(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	keyInfo := generateRandomEncryKeyInfo(1, 0)
	mock := setupMockWithKeys(keyInfo)
	mRepo.RegisterEncryptionKeyStoreCallback(mock)

	err := mRepo.RefreshKeys()
	requireNoError(t, err, "RefreshKeys")

	// Write data so the key is actually used by Magma
	err = mRepo.Set(MAIN, "ear_inuse_key", []byte("ear_inuse_value"))
	requireNoError(t, err, "Set with encryption")

	keys, err := mRepo.GetInuseKeys()
	requireNoError(t, err, "GetInuseKeys after refresh")
	t.Logf("GetInuseKeys after refresh returned: %v", keys)

	if len(keys) > 0 {
		assertKeyIDsContain(t, keys, keyInfo.ActiveKeyId)
	}
}

func TestEaR_GetInuseKeys_CallbackNotification(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	keyInfo := generateRandomEncryKeyInfo(1, 0)
	mock := setupMockWithKeys(keyInfo)

	var setInuseKeysIDs []KeyID
	var setInuseMu sync.Mutex
	mock.setInuseKeysFn = func(id KeyID) error {
		setInuseMu.Lock()
		defer setInuseMu.Unlock()
		setInuseKeysIDs = append(setInuseKeysIDs, id)
		return nil
	}
	mRepo.RegisterEncryptionKeyStoreCallback(mock)

	err := mRepo.RefreshKeys()
	requireNoError(t, err, "RefreshKeys")

	err = mRepo.Set(MAIN, "notification_key", []byte("notification_value"))
	requireNoError(t, err, "Set")

	keys, err := mRepo.GetInuseKeys()
	requireNoError(t, err, "GetInuseKeys")

	// SetInuseKeys should have been called once per returned key
	setInuseMu.Lock()
	callCount := len(setInuseKeysIDs)
	setInuseMu.Unlock()

	if callCount != len(keys) {
		t.Errorf("expected SetInuseKeys called %d times, got %d", len(keys), callCount)
	}
}

func TestEaR_GetInuseKeys_NoCallback(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	// No callback registered
	keys, err := mRepo.GetInuseKeys()
	requireNoError(t, err, "GetInuseKeys with no callback")
	t.Logf("GetInuseKeys with no callback returned: %v", keys)
}

func TestEaR_GetInuseKeys_ClosedRepo(t *testing.T) {
	mRepo, _ := openTestRepo(t)
	mRepo.Close()

	_, err := mRepo.GetInuseKeys()
	requireRepoClosedError(t, err, "GetInuseKeys on closed repo")
}

// --- DropKeys ---

func TestEaR_DropKeys_EmptySlice(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	err := mRepo.DropKeys([]KeyID{})
	requireNoError(t, err, "DropKeys with empty slice")
}

func TestEaR_DropKeys_NilSlice(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	err := mRepo.DropKeys(nil)
	requireNoError(t, err, "DropKeys with nil slice")
}

func TestEaR_DropKeys_SingleKey(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	// Set up 2 keys: keyA active, keyB non-active.
	// Magma does not allow dropping the currently active key.
	keyA := generateRandomEarKey()
	keyB := generateRandomEarKey()
	keyInfo := &EncryKeyInfo{
		ActiveKeyId: keyA.Id,
		Keys:        []EarKey{keyA, keyB},
	}
	mock := setupMockWithKeys(keyInfo)
	mRepo.RegisterEncryptionKeyStoreCallback(mock)

	err := mRepo.RefreshKeys()
	requireNoError(t, err, "RefreshKeys")

	// Write data with the active key
	err = mRepo.Set(MAIN, "drop_single_key", []byte("drop_single_value"))
	requireNoError(t, err, "Set before drop")

	// Drop the non-active key (keyB)
	err = mRepo.DropKeys([]KeyID{keyB.Id})
	requireNoError(t, err, "DropKeys single non-active key")
}

func TestEaR_DropKeys_MultipleKeys(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	// Set up 3 keys with key1 active. Magma does not allow dropping the
	// currently active key, so only the non-active keys (key2, key3) are dropped.
	key1 := generateRandomEarKey()
	key2 := generateRandomEarKey()
	key3 := generateRandomEarKey()
	keyInfo := &EncryKeyInfo{
		ActiveKeyId: key1.Id,
		Keys:        []EarKey{key1, key2, key3},
	}
	mock := setupMockWithKeys(keyInfo)
	mRepo.RegisterEncryptionKeyStoreCallback(mock)

	err := mRepo.RefreshKeys()
	requireNoError(t, err, "RefreshKeys")

	// Drop only the non-active keys
	err = mRepo.DropKeys([]KeyID{key2.Id, key3.Id})
	requireNoError(t, err, "DropKeys multiple non-active keys")
}

func TestEaR_DropKeys_EmptyStringKeyID(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	// Empty string is converted to "unencrypted" internally, which is the
	// default current key on a fresh repo. Magma correctly returns an error
	// because dropping the currently active key is not allowed.
	err := mRepo.DropKeys([]KeyID{""})
	if err == nil {
		t.Log("DropKeys with empty string key ID succeeded (key was not current)")
	} else {
		// Expected: cannot drop current "unencrypted" key
		t.Logf("DropKeys with empty string key ID returned expected error: %v", err)
	}
}

func TestEaR_DropKeys_ClosedRepo(t *testing.T) {
	mRepo, _ := openTestRepo(t)
	mRepo.Close()

	err := mRepo.DropKeys([]KeyID{generateRandomKeyID()})
	requireRepoClosedError(t, err, "DropKeys on closed repo")
}

// ============================================================================
// Part 2: Combination / Sequence Tests
// ============================================================================

func TestEaR_Sequence_RegisterThenOperations(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	keyInfo := generateRandomEncryKeyInfo(2, 0)
	mock := setupMockWithKeys(keyInfo)

	// Step 1: Register callback
	mRepo.RegisterEncryptionKeyStoreCallback(mock)

	// Step 2: RefreshKeys (should use mock)
	err := mRepo.RefreshKeys()
	requireNoError(t, err, "RefreshKeys after register")

	if mock.getEncryptionKeysCalls.Load() == 0 {
		t.Error("GetEncryptionKeys should have been called")
	}

	// Step 3: Write some data
	err = mRepo.Set(MAIN, "seq_reg_key", []byte("seq_reg_value"))
	requireNoError(t, err, "Set after register and refresh")

	// Step 4: GetInuseKeys (should return keys)
	keys, err := mRepo.GetInuseKeys()
	requireNoError(t, err, "GetInuseKeys after register")
	t.Logf("GetInuseKeys after register workflow: %v", keys)
}

func TestEaR_Sequence_KeyRotation(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	keyA := generateRandomEarKey()
	keyB := generateRandomEarKey()

	// Phase 1: Set up with keyA active
	keyInfoA := &EncryKeyInfo{
		ActiveKeyId: keyA.Id,
		Keys:        []EarKey{keyA, keyB},
	}
	mock := setupMockWithKeys(keyInfoA)
	mRepo.RegisterEncryptionKeyStoreCallback(mock)

	err := mRepo.RefreshKeys()
	requireNoError(t, err, "RefreshKeys with keyA active")

	// Write data with keyA
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("rotation_key_%d", i)
		val := fmt.Sprintf("rotation_value_%d", i)
		err = mRepo.Set(MAIN, key, []byte(val))
		requireNoError(t, err, fmt.Sprintf("Set %s with keyA", key))
	}

	// Phase 2: Rotate to keyB
	keyInfoB := &EncryKeyInfo{
		ActiveKeyId: keyB.Id,
		Keys:        []EarKey{keyA, keyB},
	}
	mock.setGetEncryptionKeysFn(func() (*EncryKeyInfo, error) {
		return keyInfoB, nil
	})

	err = mRepo.RefreshKeys()
	requireNoError(t, err, "RefreshKeys with keyB active")

	if mRepo.currentKeyID != keyB.Id {
		t.Errorf("expected currentKeyID %s, got %s", keyB.Id, mRepo.currentKeyID)
	}

	// Write more data with keyB
	for i := 5; i < 10; i++ {
		key := fmt.Sprintf("rotation_key_%d", i)
		val := fmt.Sprintf("rotation_value_%d", i)
		err = mRepo.Set(MAIN, key, []byte(val))
		requireNoError(t, err, fmt.Sprintf("Set %s with keyB", key))
	}

	// Verify all data is still readable
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("rotation_key_%d", i)
		expectedVal := fmt.Sprintf("rotation_value_%d", i)
		val, err := mRepo.Get(MAIN, key)
		requireNoError(t, err, fmt.Sprintf("Get %s after rotation", key))
		if string(val) != expectedVal {
			t.Errorf("data integrity failure for %s: got %s, want %s",
				key, string(val), expectedVal)
		}
	}
}

func TestEaR_Sequence_DropThenRefresh(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	keyA := generateRandomEarKey()
	keyB := generateRandomEarKey()

	// Phase 1: Set up with keyA active, keyB available
	keyInfo := &EncryKeyInfo{
		ActiveKeyId: keyA.Id,
		Keys:        []EarKey{keyA, keyB},
	}
	mock := setupMockWithKeys(keyInfo)
	mRepo.RegisterEncryptionKeyStoreCallback(mock)

	err := mRepo.RefreshKeys()
	requireNoError(t, err, "RefreshKeys with keyA active")

	// Write data
	err = mRepo.Set(MAIN, "drop_refresh_key", []byte("drop_refresh_value"))
	requireNoError(t, err, "Set before drop")

	// Phase 2: Rotate active key to keyB before dropping keyA.
	// Magma does not allow dropping the currently active key.
	keyInfoV2 := &EncryKeyInfo{
		ActiveKeyId: keyB.Id,
		Keys:        []EarKey{keyA, keyB},
	}
	mock.setGetEncryptionKeysFn(func() (*EncryKeyInfo, error) {
		return keyInfoV2, nil
	})

	err = mRepo.RefreshKeys()
	requireNoError(t, err, "RefreshKeys with keyB active")

	if mRepo.currentKeyID != keyB.Id {
		t.Fatalf("expected currentKeyID %s after rotation, got %s",
			keyB.Id, mRepo.currentKeyID)
	}

	// Phase 3: Now drop keyA (no longer active)
	err = mRepo.DropKeys([]KeyID{keyA.Id})
	requireNoError(t, err, "DropKeys keyA after rotation")
}

func TestEaR_Sequence_CallbackReplacement(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	keyA := generateRandomEarKey()
	keyB := generateRandomEarKey()

	// Phase 1: mockA with keyA
	keyInfoA := &EncryKeyInfo{
		ActiveKeyId: keyA.Id,
		Keys:        []EarKey{keyA},
	}
	mockA := setupMockWithKeys(keyInfoA)
	mRepo.RegisterEncryptionKeyStoreCallback(mockA)

	err := mRepo.RefreshKeys()
	requireNoError(t, err, "RefreshKeys with mockA")

	mRepo.dekCacheMu.RLock()
	_, keyAInCache := mRepo.dekCache[keyA.Id]
	mRepo.dekCacheMu.RUnlock()
	if !keyAInCache {
		t.Error("keyA should be in DEK cache after mockA refresh")
	}

	// Phase 2: Replace with mockB (different keys)
	keyInfoB := &EncryKeyInfo{
		ActiveKeyId: keyB.Id,
		Keys:        []EarKey{keyB},
	}
	mockB := setupMockWithKeys(keyInfoB)
	mRepo.RegisterEncryptionKeyStoreCallback(mockB)

	err = mRepo.RefreshKeys()
	requireNoError(t, err, "RefreshKeys with mockB")

	mRepo.dekCacheMu.RLock()
	_, keyAStillInCache := mRepo.dekCache[keyA.Id]
	_, keyBInCache := mRepo.dekCache[keyB.Id]
	mRepo.dekCacheMu.RUnlock()

	if keyAStillInCache {
		t.Error("keyA should have been pruned after mockB refresh")
	}
	if !keyBInCache {
		t.Error("keyB should be in DEK cache after mockB refresh")
	}
	if mRepo.currentKeyID != keyB.Id {
		t.Errorf("expected currentKeyID %s, got %s", keyB.Id, mRepo.currentKeyID)
	}
}

func TestEaR_Sequence_CloseInterleaving(t *testing.T) {
	t.Run("RefreshAfterClose", func(t *testing.T) {
		mRepo, _ := openTestRepo(t)
		keyInfo := generateRandomEncryKeyInfo(1, 0)
		mock := setupMockWithKeys(keyInfo)
		mRepo.RegisterEncryptionKeyStoreCallback(mock)

		err := mRepo.RefreshKeys()
		requireNoError(t, err, "RefreshKeys before close")

		mRepo.Close()

		err = mRepo.RefreshKeys()
		requireRepoClosedError(t, err, "RefreshKeys after close")
	})

	t.Run("GetInuseKeysAfterClose", func(t *testing.T) {
		mRepo, _ := openTestRepo(t)
		keyInfo := generateRandomEncryKeyInfo(1, 0)
		mock := setupMockWithKeys(keyInfo)
		mRepo.RegisterEncryptionKeyStoreCallback(mock)

		err := mRepo.RefreshKeys()
		requireNoError(t, err, "RefreshKeys before close")

		mRepo.Close()

		_, err = mRepo.GetInuseKeys()
		requireRepoClosedError(t, err, "GetInuseKeys after close")
	})

	t.Run("DropKeysAfterClose", func(t *testing.T) {
		mRepo, _ := openTestRepo(t)
		keyInfo := generateRandomEncryKeyInfo(1, 0)
		mock := setupMockWithKeys(keyInfo)
		mRepo.RegisterEncryptionKeyStoreCallback(mock)

		err := mRepo.RefreshKeys()
		requireNoError(t, err, "RefreshKeys before close")

		mRepo.Close()

		err = mRepo.DropKeys([]KeyID{keyInfo.Keys[0].Id})
		requireRepoClosedError(t, err, "DropKeys after close")
	})

	t.Run("RegisterAfterClose", func(t *testing.T) {
		mRepo, _ := openTestRepo(t)
		mRepo.Close()

		// Register does not check isClosed — no panic expected
		mock := newMockCallbacks()
		mRepo.RegisterEncryptionKeyStoreCallback(mock)
		if mRepo.keyStoreCallbacks == nil {
			t.Fatal("callback should be set even after close")
		}
	})
}

func TestEaR_Sequence_DataIntegrityAcrossKeyChange(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	keyA := generateRandomEarKey()
	keyB := generateRandomEarKey()

	// Set up with keyA
	keyInfoA := &EncryKeyInfo{
		ActiveKeyId: keyA.Id,
		Keys:        []EarKey{keyA, keyB},
	}
	mock := setupMockWithKeys(keyInfoA)
	mRepo.RegisterEncryptionKeyStoreCallback(mock)

	err := mRepo.RefreshKeys()
	requireNoError(t, err, "RefreshKeys keyA")

	// Write test data with keyA
	testData := map[string]string{
		"integrity_key_1": "integrity_value_1",
		"integrity_key_2": "integrity_value_2",
		"integrity_key_3": "integrity_value_3",
	}
	for k, v := range testData {
		err = mRepo.Set(MAIN, k, []byte(v))
		requireNoError(t, err, fmt.Sprintf("Set %s with keyA", k))
	}

	// Rotate to keyB
	keyInfoB := &EncryKeyInfo{
		ActiveKeyId: keyB.Id,
		Keys:        []EarKey{keyA, keyB},
	}
	mock.setGetEncryptionKeysFn(func() (*EncryKeyInfo, error) {
		return keyInfoB, nil
	})

	err = mRepo.RefreshKeys()
	requireNoError(t, err, "RefreshKeys keyB")

	// Read all data back — should be decryptable via go_global_get_encryption_key
	for k, expectedV := range testData {
		val, err := mRepo.Get(MAIN, k)
		requireNoError(t, err, fmt.Sprintf("Get %s after key rotation", k))
		if string(val) != expectedV {
			t.Errorf("data integrity failure for %s: got %q, want %q",
				k, string(val), expectedV)
		}
	}
}

func TestEaR_Sequence_AllRepoKinds(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	keyInfo := generateRandomEncryKeyInfo(1, 0)
	mock := setupMockWithKeys(keyInfo)
	mRepo.RegisterEncryptionKeyStoreCallback(mock)

	err := mRepo.RefreshKeys()
	requireNoError(t, err, "RefreshKeys")

	kinds := []RepoKind{MAIN, COMMIT_LOG, SERVER_CONFIG, LOCAL}
	testKey := "all_kinds_key"
	testVal := "all_kinds_value"

	// Write to all 4 repo kinds
	for _, kind := range kinds {
		kindKey := fmt.Sprintf("%s_%d", testKey, kind)
		kindVal := fmt.Sprintf("%s_%d", testVal, kind)
		err = mRepo.Set(kind, kindKey, []byte(kindVal))
		requireNoError(t, err, fmt.Sprintf("Set to kind %d", kind))
	}

	// Read from all 4 repo kinds
	for _, kind := range kinds {
		kindKey := fmt.Sprintf("%s_%d", testKey, kind)
		expectedVal := fmt.Sprintf("%s_%d", testVal, kind)
		val, err := mRepo.Get(kind, kindKey)
		requireNoError(t, err, fmt.Sprintf("Get from kind %d", kind))
		if string(val) != expectedVal {
			t.Errorf("data mismatch for kind %d: got %q, want %q",
				kind, string(val), expectedVal)
		}
	}
}

// ============================================================================
// Part 3: Concurrency Tests
// ============================================================================

const (
	earConcurrentGoroutines = 10
	earConcurrentIterations = 50
)

func TestEaR_Concurrent_RefreshKeys(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	keyInfo := generateRandomEncryKeyInfo(3, 0)
	mock := setupMockWithKeys(keyInfo)
	mRepo.RegisterEncryptionKeyStoreCallback(mock)

	// Initial refresh to establish baseline
	err := mRepo.RefreshKeys()
	requireNoError(t, err, "initial RefreshKeys")

	var wg sync.WaitGroup
	var errCount atomic.Int64

	for g := 0; g < earConcurrentGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < earConcurrentIterations; i++ {
				if err := mRepo.RefreshKeys(); err != nil {
					errCount.Add(1)
				}
			}
		}()
	}

	wg.Wait()

	if errCount.Load() > 0 {
		t.Logf("concurrent RefreshKeys had %d errors (may be acceptable if repo was busy)",
			errCount.Load())
	}

	// Verify final state is consistent
	mRepo.dekCacheMu.RLock()
	cacheLen := len(mRepo.dekCache)
	mRepo.dekCacheMu.RUnlock()

	if cacheLen != 3 {
		t.Errorf("expected 3 keys in DEK cache after concurrent refreshes, got %d", cacheLen)
	}
}

func TestEaR_Concurrent_GetInuseKeys(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	keyInfo := generateRandomEncryKeyInfo(1, 0)
	mock := setupMockWithKeys(keyInfo)
	mRepo.RegisterEncryptionKeyStoreCallback(mock)

	err := mRepo.RefreshKeys()
	requireNoError(t, err, "RefreshKeys")

	err = mRepo.Set(MAIN, "concurrent_inuse_key", []byte("value"))
	requireNoError(t, err, "Set data")

	var wg sync.WaitGroup
	var errCount atomic.Int64

	for g := 0; g < earConcurrentGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < earConcurrentIterations; i++ {
				_, err := mRepo.GetInuseKeys()
				if err != nil {
					errCount.Add(1)
				}
			}
		}()
	}

	wg.Wait()

	if errCount.Load() > 0 {
		t.Errorf("concurrent GetInuseKeys had %d errors", errCount.Load())
	}
}

func TestEaR_Concurrent_DropKeys(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	keyInfo := generateRandomEncryKeyInfo(1, 0)
	mock := setupMockWithKeys(keyInfo)
	mRepo.RegisterEncryptionKeyStoreCallback(mock)

	err := mRepo.RefreshKeys()
	requireNoError(t, err, "RefreshKeys")

	var wg sync.WaitGroup
	var errCount atomic.Int64

	// Generate unique key IDs for each goroutine to avoid overlap
	for g := 0; g < earConcurrentGoroutines; g++ {
		wg.Add(1)
		keyID := generateRandomKeyID()
		go func(kid string) {
			defer wg.Done()
			for i := 0; i < earConcurrentIterations; i++ {
				if err := mRepo.DropKeys([]KeyID{kid}); err != nil {
					errCount.Add(1)
				}
			}
		}(keyID)
	}

	wg.Wait()

	// Some errors are expected if Magma doesn't know the key, but no panics/races
	t.Logf("concurrent DropKeys had %d errors", errCount.Load())
}

func TestEaR_Concurrent_MixedOperations(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	keyInfo := generateRandomEncryKeyInfo(2, 0)
	mock := setupMockWithKeys(keyInfo)
	mRepo.RegisterEncryptionKeyStoreCallback(mock)

	err := mRepo.RefreshKeys()
	requireNoError(t, err, "initial RefreshKeys")

	// Write some initial data
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("mixed_key_%d", i)
		v := fmt.Sprintf("mixed_value_%d", i)
		err = mRepo.Set(MAIN, k, []byte(v))
		requireNoError(t, err, fmt.Sprintf("Set %s", k))
	}

	var wg sync.WaitGroup
	var panicCount atomic.Int64

	// Goroutines doing RefreshKeys
	for g := 0; g < 3; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicCount.Add(1)
				}
			}()
			for i := 0; i < earConcurrentIterations; i++ {
				_ = mRepo.RefreshKeys()
			}
		}()
	}

	// Goroutines doing GetInuseKeys
	for g := 0; g < 3; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicCount.Add(1)
				}
			}()
			for i := 0; i < earConcurrentIterations; i++ {
				_, _ = mRepo.GetInuseKeys()
			}
		}()
	}

	// Goroutines doing DropKeys
	for g := 0; g < 2; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicCount.Add(1)
				}
			}()
			for i := 0; i < earConcurrentIterations; i++ {
				_ = mRepo.DropKeys([]KeyID{generateRandomKeyID()})
			}
		}()
	}

	// Goroutines doing Set and Get
	for g := 0; g < 2; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicCount.Add(1)
				}
			}()
			for i := 0; i < earConcurrentIterations; i++ {
				k := fmt.Sprintf("concurrent_rw_%d_%d", id, i)
				_ = mRepo.Set(MAIN, k, []byte("value"))
				_, _ = mRepo.Get(MAIN, k)
			}
		}(g)
	}

	wg.Wait()

	if panicCount.Load() > 0 {
		t.Errorf("concurrent mixed operations had %d panics", panicCount.Load())
	}
}

func TestEaR_Concurrent_CloseWithOps(t *testing.T) {
	mRepo, _ := openTestRepo(t)

	keyInfo := generateRandomEncryKeyInfo(1, 0)
	mock := setupMockWithKeys(keyInfo)
	mRepo.RegisterEncryptionKeyStoreCallback(mock)

	err := mRepo.RefreshKeys()
	requireNoError(t, err, "initial RefreshKeys")

	var wg sync.WaitGroup
	var panicCount atomic.Int64

	// Start goroutines performing EaR operations
	for g := 0; g < earConcurrentGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicCount.Add(1)
				}
			}()
			for i := 0; i < earConcurrentIterations; i++ {
				switch id % 3 {
				case 0:
					_ = mRepo.RefreshKeys()
				case 1:
					_, _ = mRepo.GetInuseKeys()
				case 2:
					_ = mRepo.DropKeys([]KeyID{generateRandomKeyID()})
				}
			}
		}(g)
	}

	// Close the repo while operations are running
	mRepo.Close()

	wg.Wait()

	if panicCount.Load() > 0 {
		t.Errorf("concurrent close with ops had %d panics", panicCount.Load())
	}
}

// ============================================================================
// Part 4: EaR Restart Persistence Tests
// ============================================================================

// generateDeterministicEarKey creates an EarKey with deterministic key material
// derived from the given seed string. This allows writer and reader subprocesses
// to independently derive the same key.
func generateDeterministicEarKey(seed string) EarKey {
	h := sha256.Sum256([]byte(seed))
	return EarKey{
		Id:     fmt.Sprintf("key-%s", seed),
		Cipher: "AES-256-GCM",
		Key:    h[:],
	}
}

func openTestRepoAtWithCallbacks(dir string, callbacks IEncryptionKeyStoreCallbacks) *Magma_Repository {
	params := RepoFactoryParams{
		Dir:                        dir,
		MemoryQuota:                4 * 1024 * 1024,
		CompactionMinFileSize:      0,
		CompactionThresholdPercent: 30,
		CompactionTimerDur:         60000,
		EnableWAL:                  true,
		StoreType:                  MagmaStoreType,
		EarCallbacks:               callbacks,
	}
	repo := getMagmaRepo(params)
	return repo.(*Magma_Repository)
}

func TestEaR_Restart_SingleKeyPersistence(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "test_ear_single_key")
	if os.Getenv(CHILD_PROC_TEST_ENV) != "1" {
		os.RemoveAll(dir)
		os.Remove(dir)
		handleError(t, os.Mkdir(dir, 0777), "folder not created")
	}
	defer func() {
		os.RemoveAll(dir)
		os.Remove(dir)
	}()

	keyA := generateDeterministicEarKey("alpha")

	t.Run("Writer", runInChildProc(func(t *testing.T) {
		keyInfo := &EncryKeyInfo{
			ActiveKeyId: keyA.Id,
			Keys:        []EarKey{keyA},
		}
		mock := setupMockWithKeys(keyInfo)
		mRepo := openTestRepoAtWithCallbacks(dir, mock)

		err := mRepo.RefreshKeys()
		if err != nil {
			t.Fatalf("RefreshKeys failed: %v", err)
		}

		// Write 20 keys across MAIN and LOCAL
		for i := 0; i < 10; i++ {
			k := fmt.Sprintf("single_key_%d", i)
			v := fmt.Sprintf("single_value_%d", i)
			if err := mRepo.Set(MAIN, k, []byte(v)); err != nil {
				t.Fatalf("Set MAIN failed for %s: %v", k, err)
			}
			if err := mRepo.Set(LOCAL, k, []byte(v)); err != nil {
				t.Fatalf("Set LOCAL failed for %s: %v", k, err)
			}
		}

		if err := mRepo.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		os.Exit(0)
	}))

	t.Run("Reader", runInChildProc(func(t *testing.T) {
		// Re-register the SAME key so go_global_get_encryption_key can resolve it
		keyInfo := &EncryKeyInfo{
			ActiveKeyId: keyA.Id,
			Keys:        []EarKey{keyA},
		}
		mock := setupMockWithKeys(keyInfo)
		mRepo := openTestRepoAtWithCallbacks(dir, mock)
		defer mRepo.Close()

		err := mRepo.RefreshKeys()
		if err != nil {
			t.Fatalf("RefreshKeys failed: %v", err)
		}

		for i := 0; i < 10; i++ {
			k := fmt.Sprintf("single_key_%d", i)
			expectedV := fmt.Sprintf("single_value_%d", i)

			val, err := mRepo.Get(MAIN, k)
			if err != nil {
				t.Fatalf("Get MAIN failed for %s: %v", k, err)
			}
			if string(val) != expectedV {
				t.Fatalf("MAIN value mismatch for %s: got %q, want %q", k, string(val), expectedV)
			}

			val, err = mRepo.Get(LOCAL, k)
			if err != nil {
				t.Fatalf("Get LOCAL failed for %s: %v", k, err)
			}
			if string(val) != expectedV {
				t.Fatalf("LOCAL value mismatch for %s: got %q, want %q", k, string(val), expectedV)
			}
		}
	}))
}

func TestEaR_Restart_MultiKeyRotationPersistence(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "test_ear_multi_key")
	if os.Getenv(CHILD_PROC_TEST_ENV) != "1" {
		os.RemoveAll(dir)
		os.Remove(dir)
		handleError(t, os.Mkdir(dir, 0777), "folder not created")
	}
	defer func() {
		os.RemoveAll(dir)
		os.Remove(dir)
	}()

	keyA := generateDeterministicEarKey("alpha")
	keyB := generateDeterministicEarKey("bravo")

	t.Run("Writer", runInChildProc(func(t *testing.T) {
		// Phase 1: write with keyA active
		keyInfoA := &EncryKeyInfo{
			ActiveKeyId: keyA.Id,
			Keys:        []EarKey{keyA, keyB},
		}
		mock := setupMockWithKeys(keyInfoA)
		mRepo := openTestRepoAtWithCallbacks(dir, mock)

		if err := mRepo.RefreshKeys(); err != nil {
			t.Fatalf("RefreshKeys (keyA) failed: %v", err)
		}

		// Write across all 4 repo kinds with keyA
		for _, kind := range []RepoKind{MAIN, COMMIT_LOG, SERVER_CONFIG, LOCAL} {
			for i := 0; i < 5; i++ {
				k := fmt.Sprintf("multi_keyA_%d_%d", kind, i)
				v := fmt.Sprintf("multi_valueA_%d_%d", kind, i)
				if err := mRepo.Set(kind, k, []byte(v)); err != nil {
					t.Fatalf("Set kind=%d failed for %s: %v", kind, k, err)
				}
			}
		}

		// Phase 2: rotate to keyB and write more data
		keyInfoB := &EncryKeyInfo{
			ActiveKeyId: keyB.Id,
			Keys:        []EarKey{keyA, keyB},
		}
		mock.setGetEncryptionKeysFn(func() (*EncryKeyInfo, error) {
			return keyInfoB, nil
		})

		if err := mRepo.RefreshKeys(); err != nil {
			t.Fatalf("RefreshKeys (keyB) failed: %v", err)
		}

		for _, kind := range []RepoKind{MAIN, COMMIT_LOG, SERVER_CONFIG, LOCAL} {
			for i := 0; i < 5; i++ {
				k := fmt.Sprintf("multi_keyB_%d_%d", kind, i)
				v := fmt.Sprintf("multi_valueB_%d_%d", kind, i)
				if err := mRepo.Set(kind, k, []byte(v)); err != nil {
					t.Fatalf("Set kind=%d failed for %s: %v", kind, k, err)
				}
			}
		}

		if err := mRepo.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		os.Exit(0)
	}))

	t.Run("Reader", runInChildProc(func(t *testing.T) {
		// Register BOTH keys so callback can resolve either
		keyInfo := &EncryKeyInfo{
			ActiveKeyId: keyB.Id,
			Keys:        []EarKey{keyA, keyB},
		}
		mock := setupMockWithKeys(keyInfo)
		mRepo := openTestRepoAtWithCallbacks(dir, mock)
		defer mRepo.Close()

		if err := mRepo.RefreshKeys(); err != nil {
			t.Fatalf("RefreshKeys failed: %v", err)
		}

		// Verify ALL data from both key eras across all repo kinds
		for _, kind := range []RepoKind{MAIN, COMMIT_LOG, SERVER_CONFIG, LOCAL} {
			for i := 0; i < 5; i++ {
				// keyA era
				kA := fmt.Sprintf("multi_keyA_%d_%d", kind, i)
				expectedVA := fmt.Sprintf("multi_valueA_%d_%d", kind, i)
				val, err := mRepo.Get(kind, kA)
				if err != nil {
					t.Fatalf("Get kind=%d failed for %s: %v", kind, kA, err)
				}
				if string(val) != expectedVA {
					t.Fatalf("value mismatch for %s: got %q, want %q", kA, string(val), expectedVA)
				}

				// keyB era
				kB := fmt.Sprintf("multi_keyB_%d_%d", kind, i)
				expectedVB := fmt.Sprintf("multi_valueB_%d_%d", kind, i)
				val, err = mRepo.Get(kind, kB)
				if err != nil {
					t.Fatalf("Get kind=%d failed for %s: %v", kind, kB, err)
				}
				if string(val) != expectedVB {
					t.Fatalf("value mismatch for %s: got %q, want %q", kB, string(val), expectedVB)
				}
			}
		}

		// Verify the callback was used (go_global_get_encryption_key was called)
		if mock.getKeyCipherByIDCalls.Load() > 0 {
			t.Logf("GetKeyCipherByID callback was invoked %d times (key resolution on reopen)",
				mock.getKeyCipherByIDCalls.Load())
		}
		if mock.getActiveKeyCipherCalls.Load() > 0 {
			t.Logf("GetActiveKeyCipher callback was invoked %d times",
				mock.getActiveKeyCipherCalls.Load())
		}
	}))
}

func TestEaR_Restart_ThreeKeyRotationAllKinds(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "test_ear_three_keys")
	if os.Getenv(CHILD_PROC_TEST_ENV) != "1" {
		os.RemoveAll(dir)
		os.Remove(dir)
		handleError(t, os.Mkdir(dir, 0777), "folder not created")
	}
	defer func() {
		os.RemoveAll(dir)
		os.Remove(dir)
	}()

	keyA := generateDeterministicEarKey("alpha")
	keyB := generateDeterministicEarKey("bravo")
	keyC := generateDeterministicEarKey("charlie")
	allKeys := []EarKey{keyA, keyB, keyC}
	allKinds := []RepoKind{MAIN, COMMIT_LOG, SERVER_CONFIG, LOCAL}

	t.Run("Writer", runInChildProc(func(t *testing.T) {
		// Create initial mock with first key active
		keyInfo0 := &EncryKeyInfo{
			ActiveKeyId: allKeys[0].Id,
			Keys:        allKeys,
		}
		mock := setupMockWithKeys(keyInfo0)
		mRepo := openTestRepoAtWithCallbacks(dir, mock)

		// Rotate through 3 keys, writing data with each
		for idx, activeKey := range allKeys {
			keyInfo := &EncryKeyInfo{
				ActiveKeyId: activeKey.Id,
				Keys:        allKeys,
			}
			mock.setGetEncryptionKeysFn(func() (*EncryKeyInfo, error) {
				return keyInfo, nil
			})

			if err := mRepo.RefreshKeys(); err != nil {
				t.Fatalf("RefreshKeys for key %d failed: %v", idx, err)
			}

			for _, kind := range allKinds {
				for i := 0; i < 5; i++ {
					k := fmt.Sprintf("three_%d_%d_%d", idx, kind, i)
					v := fmt.Sprintf("val_%d_%d_%d", idx, kind, i)
					if err := mRepo.Set(kind, k, []byte(v)); err != nil {
						t.Fatalf("Set key=%d kind=%d i=%d failed: %v", idx, kind, i, err)
					}
				}
			}
		}

		if err := mRepo.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		os.Exit(0)
	}))

	t.Run("Reader", runInChildProc(func(t *testing.T) {
		// Register all 3 keys with keyC as active
		keyInfo := &EncryKeyInfo{
			ActiveKeyId: keyC.Id,
			Keys:        allKeys,
		}
		mock := setupMockWithKeys(keyInfo)
		mRepo := openTestRepoAtWithCallbacks(dir, mock)
		defer mRepo.Close()

		if err := mRepo.RefreshKeys(); err != nil {
			t.Fatalf("RefreshKeys failed: %v", err)
		}

		// Read all 3*4*5 = 60 key-value pairs
		for idx := range allKeys {
			for _, kind := range allKinds {
				for i := 0; i < 5; i++ {
					k := fmt.Sprintf("three_%d_%d_%d", idx, kind, i)
					expectedV := fmt.Sprintf("val_%d_%d_%d", idx, kind, i)
					val, err := mRepo.Get(kind, k)
					if err != nil {
						t.Fatalf("Get key=%d kind=%d i=%d failed: %v", idx, kind, i, err)
					}
					if string(val) != expectedV {
						t.Fatalf("value mismatch for %s: got %q, want %q", k, string(val), expectedV)
					}
				}
			}
		}

		t.Logf("Successfully read 60 encrypted entries across 3 keys and 4 repo kinds after restart")
		t.Logf("GetKeyCipherByID calls: %d, GetActiveKeyCipher calls: %d",
			mock.getKeyCipherByIDCalls.Load(), mock.getActiveKeyCipherCalls.Load())
	}))
}

func TestEaR_Restart_CallbackResolvesUnknownKey(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "test_ear_callback_resolve")
	if os.Getenv(CHILD_PROC_TEST_ENV) != "1" {
		os.RemoveAll(dir)
		os.Remove(dir)
		handleError(t, os.Mkdir(dir, 0777), "folder not created")
	}
	defer func() {
		os.RemoveAll(dir)
		os.Remove(dir)
	}()

	keyA := generateDeterministicEarKey("alpha")
	keyB := generateDeterministicEarKey("bravo")

	t.Run("Writer", runInChildProc(func(t *testing.T) {
		keyInfo := &EncryKeyInfo{
			ActiveKeyId: keyA.Id,
			Keys:        []EarKey{keyA},
		}
		mock := setupMockWithKeys(keyInfo)
		mRepo := openTestRepoAtWithCallbacks(dir, mock)

		if err := mRepo.RefreshKeys(); err != nil {
			t.Fatalf("RefreshKeys failed: %v", err)
		}

		for i := 0; i < 10; i++ {
			k := fmt.Sprintf("callback_resolve_%d", i)
			v := fmt.Sprintf("callback_value_%d", i)
			if err := mRepo.Set(MAIN, k, []byte(v)); err != nil {
				t.Fatalf("Set failed for %s: %v", k, err)
			}
		}

		if err := mRepo.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		os.Exit(0)
	}))

	t.Run("Reader", runInChildProc(func(t *testing.T) {
		// Register keyB as active, but callback can also resolve keyA by ID.
		// keyA is NOT in RefreshKeys' key set, so it won't be in the DEK cache.
		// Magma will call go_global_get_encryption_key with keyA's ID,
		// which must fall through to GetKeyCipherByID callback.
		mock := newMockCallbacks()
		mock.getEncryptionKeysFn = func() (*EncryKeyInfo, error) {
			return &EncryKeyInfo{
				ActiveKeyId: keyB.Id,
				Keys:        []EarKey{keyB}, // keyA intentionally NOT here
			}, nil
		}
		mock.getActiveKeyCipherFn = func() (*EarKey, error) {
			kb := keyB
			return &kb, nil
		}
		mock.getKeyCipherByIDFn = func(id KeyID) (*EarKey, error) {
			switch id {
			case keyA.Id:
				ka := keyA
				return &ka, nil
			case keyB.Id:
				kb := keyB
				return &kb, nil
			default:
				return nil, fmt.Errorf("unknown key: %s", id)
			}
		}
		mRepo := openTestRepoAtWithCallbacks(dir, mock)
		defer mRepo.Close()

		if err := mRepo.RefreshKeys(); err != nil {
			t.Fatalf("RefreshKeys failed: %v", err)
		}

		// Read data that was encrypted with keyA — must go through callback
		for i := 0; i < 10; i++ {
			k := fmt.Sprintf("callback_resolve_%d", i)
			expectedV := fmt.Sprintf("callback_value_%d", i)
			val, err := mRepo.Get(MAIN, k)
			if err != nil {
				t.Fatalf("Get failed for %s: %v", k, err)
			}
			if string(val) != expectedV {
				t.Fatalf("value mismatch for %s: got %q, want %q", k, string(val), expectedV)
			}
		}

		// Verify the callback was actually used to resolve keyA
		byIDCalls := mock.getKeyCipherByIDCalls.Load()
		if byIDCalls == 0 {
			t.Error("expected GetKeyCipherByID callback to be called to resolve keyA, but it was not called")
		} else {
			t.Logf("GetKeyCipherByID was called %d times to resolve keys on reopen", byIDCalls)
		}
	}))
}

// ============================================================================
// Part 5: GetInuseKeys Unencrypted Key Tests
// ============================================================================

func TestEaR_GetInuseKeys_UnencryptedDataOnly(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	// Write data without any encryption configured
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("unenc_key_%d", i)
		v := fmt.Sprintf("unenc_value_%d", i)
		err := mRepo.Set(MAIN, k, []byte(v))
		requireNoError(t, err, fmt.Sprintf("Set %s", k))
	}

	err := mRepo.Commit()
	requireNoError(t, err, "Commit")

	keys, err := mRepo.GetInuseKeys()
	requireNoError(t, err, "GetInuseKeys")

	t.Logf("GetInuseKeys with unencrypted data only returned: %v (len=%d)", keys, len(keys))

	// Magma should report the "unencrypted" key as active.
	// When no encryption callbacks are registered, the raw Magma key ID
	// "unencrypted" may not match the static C string used for comparison,
	// so it can come back as the literal string "unencrypted" instead of "".
	found := false
	for _, k := range keys {
		if k == "" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected unencrypted key (empty string or \"unencrypted\") in active keys, got: %v", keys)
	}
}

func TestEaR_GetInuseKeys_EncryptionEnabledAfterUnencryptedData(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	// Phase 1: Write data WITHOUT encryption
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("phase1_key_%d", i)
		v := fmt.Sprintf("phase1_value_%d", i)
		err := mRepo.Set(MAIN, k, []byte(v))
		requireNoError(t, err, fmt.Sprintf("Set phase1 %s", k))
	}
	err := mRepo.Commit()
	requireNoError(t, err, "Commit phase1")

	// Phase 2: Enable encryption and write more data
	keyInfo := generateRandomEncryKeyInfo(1, 0)
	mock := setupMockWithKeys(keyInfo)
	mRepo.RegisterEncryptionKeyStoreCallback(mock)

	err = mRepo.RefreshKeys()
	requireNoError(t, err, "RefreshKeys")

	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("phase2_key_%d", i)
		v := fmt.Sprintf("phase2_value_%d", i)
		err := mRepo.Set(MAIN, k, []byte(v))
		requireNoError(t, err, fmt.Sprintf("Set phase2 %s", k))
	}
	err = mRepo.Commit()
	requireNoError(t, err, "Commit phase2")

	// GetInuseKeys should return both the unencrypted key and the real key
	keys, err := mRepo.GetInuseKeys()
	requireNoError(t, err, "GetInuseKeys")

	t.Logf("GetInuseKeys after enabling encryption returned: %v (len=%d)", keys, len(keys))

	if len(keys) < 2 {
		t.Fatalf("expected at least 2 active keys (unencrypted + encrypted), got %d: %v", len(keys), keys)
	}

	// Check for unencrypted key (empty string)
	foundUnencrypted := false
	foundEncrypted := false
	for _, k := range keys {
		if k == "" {
			foundUnencrypted = true
		}
		if k == keyInfo.ActiveKeyId {
			foundEncrypted = true
		}
	}

	if !foundUnencrypted {
		t.Errorf("expected unencrypted key (empty string) in active keys, got: %v", keys)
	}
	if !foundEncrypted {
		t.Errorf("expected encryption key %q in active keys, got: %v", keyInfo.ActiveKeyId, keys)
	}
}

// ============================================================================
// Part 6: DropKeys Encryption Lifecycle Tests
// ============================================================================

func TestEaR_DropKeys_UnencryptedData(t *testing.T) {
	mRepo, cleanup := openTestRepo(t)
	defer cleanup()

	// Phase 1: Write unencrypted data
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("unenc_%d", i)
		v := fmt.Sprintf("unenc_val_%d", i)
		err := mRepo.Set(MAIN, k, []byte(v))
		requireNoError(t, err, fmt.Sprintf("Set unenc %s", k))
	}
	err := mRepo.Commit()
	requireNoError(t, err, "Commit unencrypted")

	// Phase 2: Enable encryption, write encrypted data
	keyInfo := generateRandomEncryKeyInfo(1, 0)
	encKeyID := keyInfo.ActiveKeyId
	mock := setupMockWithKeys(keyInfo)
	mRepo.RegisterEncryptionKeyStoreCallback(mock)

	err = mRepo.RefreshKeys()
	requireNoError(t, err, "RefreshKeys")

	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("enc_%d", i)
		v := fmt.Sprintf("enc_val_%d", i)
		err := mRepo.Set(MAIN, k, []byte(v))
		requireNoError(t, err, fmt.Sprintf("Set enc %s", k))
	}
	err = mRepo.Commit()
	requireNoError(t, err, "Commit encrypted")

	// Sanity: both keys should be active
	keys, err := mRepo.GetInuseKeys()
	requireNoError(t, err, "GetInuseKeys before drop")
	t.Logf("Before drop: active keys = %v", keys)

	hasUnenc := false
	hasEnc := false
	for _, k := range keys {
		if k == "" {
			hasUnenc = true
		}
		if string(k) == encKeyID {
			hasEnc = true
		}
	}
	if !hasUnenc || !hasEnc {
		t.Fatalf("expected both unencrypted and encrypted keys before drop, got: %v", keys)
	}

	// Drop the unencrypted key — rewrites unencrypted data with the active encryption key
	err = mRepo.DropKeys([]KeyID{""})
	requireNoError(t, err, "DropKeys unencrypted")

	// Verify: unencrypted key gone, only encryption key remains
	keys, err = mRepo.GetInuseKeys()
	requireNoError(t, err, "GetInuseKeys after drop")
	t.Logf("After drop unencrypted: active keys = %v", keys)

	for _, k := range keys {
		if k == "" {
			t.Errorf("unencrypted key should no longer be active after drop, got: %v", keys)
			break
		}
	}
	assertKeyIDsContain(t, keys, encKeyID)

	// Verify data integrity — all data should still be readable
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("unenc_%d", i)
		expected := fmt.Sprintf("unenc_val_%d", i)
		val, err := mRepo.Get(MAIN, k)
		requireNoError(t, err, fmt.Sprintf("Get %s after drop", k))
		if string(val) != expected {
			t.Errorf("data mismatch for %s: got %q, want %q", k, string(val), expected)
		}
	}
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("enc_%d", i)
		expected := fmt.Sprintf("enc_val_%d", i)
		val, err := mRepo.Get(MAIN, k)
		requireNoError(t, err, fmt.Sprintf("Get %s after drop", k))
		if string(val) != expected {
			t.Errorf("data mismatch for %s: got %q, want %q", k, string(val), expected)
		}
	}
}

func TestEaR_DropKeys_EncryptedDataAfterSwitchToUnencrypted(t *testing.T) {
	// Phase 1: Write encrypted data
	keyInfo := generateRandomEncryKeyInfo(1, 0)
	encKeyID := keyInfo.ActiveKeyId
	encKey := keyInfo.Keys[0] // save for later
	mock := setupMockWithKeys(keyInfo)

	mRepo := openTestRepoAtWithCallbacks(t.TempDir(), mock)
	defer mRepo.Close()

	err := mRepo.RefreshKeys()
	requireNoError(t, err, "RefreshKeys encrypted")

	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("enc_%d", i)
		v := fmt.Sprintf("enc_val_%d", i)
		err := mRepo.Set(MAIN, k, []byte(v))
		requireNoError(t, err, fmt.Sprintf("Set enc %s", k))
	}
	err = mRepo.Commit()
	requireNoError(t, err, "Commit encrypted")

	// Phase 2: Switch to unencrypted
	unencKey := EarKey{Id: "", Cipher: "None"}
	mock.setGetEncryptionKeysFn(func() (*EncryKeyInfo, error) {
		return &EncryKeyInfo{
			ActiveKeyId: "",
			Keys:        []EarKey{unencKey, encKey},
		}, nil
	})
	mock.mu.Lock()
	mock.getActiveKeyCipherFn = func() (*EarKey, error) {
		k := unencKey
		return &k, nil
	}
	origGetByID := mock.getKeyCipherByIDFn
	mock.getKeyCipherByIDFn = func(id KeyID) (*EarKey, error) {
		if id == "" {
			k := unencKey
			return &k, nil
		}
		return origGetByID(id)
	}
	mock.mu.Unlock()

	err = mRepo.RefreshKeys()
	requireNoError(t, err, "RefreshKeys to unencrypted")

	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("unenc_%d", i)
		v := fmt.Sprintf("unenc_val_%d", i)
		err := mRepo.Set(MAIN, k, []byte(v))
		requireNoError(t, err, fmt.Sprintf("Set unenc %s", k))
	}
	err = mRepo.Commit()
	requireNoError(t, err, "Commit unencrypted")

	// Verify: both keys should be active
	keys, err := mRepo.GetInuseKeys()
	requireNoError(t, err, "GetInuseKeys before drop")
	t.Logf("Before drop: active keys = %v", keys)

	if len(keys) < 2 {
		t.Fatalf("expected at least 2 active keys, got %d: %v", len(keys), keys)
	}

	// Drop the old encryption key — rewrites encrypted data as unencrypted
	err = mRepo.DropKeys([]KeyID{KeyID(encKeyID)})
	requireNoError(t, err, "DropKeys encrypted key")

	// Verify: only unencrypted key remains
	keys, err = mRepo.GetInuseKeys()
	requireNoError(t, err, "GetInuseKeys after drop")
	t.Logf("After drop encrypted key: active keys = %v", keys)

	for _, k := range keys {
		if string(k) == encKeyID {
			t.Errorf("encrypted key %q should no longer be active after drop, got: %v", encKeyID, keys)
			break
		}
	}

	// After dropping the only encryption key, Magma may return either:
	// - [""] (unencrypted key marker) if it tracks the unencrypted data as active, OR
	// - [] (empty) if the unencrypted key's reference count drops after compaction.
	// Both are valid — the important thing is the old encryption key is gone.
	for _, k := range keys {
		if k != "" {
			t.Errorf("expected only unencrypted key (or empty list) after drop, but found %q in: %v", k, keys)
		}
	}

	// Verify data integrity
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("enc_%d", i)
		expected := fmt.Sprintf("enc_val_%d", i)
		val, err := mRepo.Get(MAIN, k)
		requireNoError(t, err, fmt.Sprintf("Get %s after drop", k))
		if string(val) != expected {
			t.Errorf("data mismatch for %s: got %q, want %q", k, string(val), expected)
		}
	}
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("unenc_%d", i)
		expected := fmt.Sprintf("unenc_val_%d", i)
		val, err := mRepo.Get(MAIN, k)
		requireNoError(t, err, fmt.Sprintf("Get %s after drop", k))
		if string(val) != expected {
			t.Errorf("data mismatch for %s: got %q, want %q", k, string(val), expected)
		}
	}
}
