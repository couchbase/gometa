//go:build !community
// +build !community

package repository

/*
#cgo LDFLAGS: -lmagma_shared -ljemalloc
#include <stdlib.h>
#include <string.h>
#include <libmagma/magma_capi.h>
// #include <libmagma/magma_data_recovery.h>

extern DataEncryptionKey* go_global_get_encryption_key(SizedBuf id, void* ctx);

extern void* je_malloc(size_t size);
extern void je_free(void* ptr);

// Static unencrypted DEK - must persist for program lifetime
static DataEncryptionKey unencryptedDEK = {0};

static void initUnencryptedDEK(void) {
	unencryptedDEK.id = "unencrypted";
	unencryptedDEK.cipher = "None";
	unencryptedDEK.key.data = NULL;
	unencryptedDEK.key.len = 0;
}

static DataEncryptionKey* getUnencryptedDEK(void) {
	return &unencryptedDEK;
}

// Allocate a null-terminated C string using jemalloc
static char* je_strdup(const char* src) {
	if (!src) return NULL;
	size_t len = strlen(src) + 1;
	char* dst = (char*)je_malloc(len);
	if (dst) memcpy(dst, src, len);
	return dst;
}

// Allocate a byte buffer using jemalloc
static char* je_memdup(const char* src, size_t len) {
	if (!src || len == 0) return NULL;
	char* dst = (char*)je_malloc(len);
	if (dst) memcpy(dst, src, len);
	return dst;
}
*/
import "C"

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	c "github.com/couchbase/gometa/common"
	"github.com/couchbase/gometa/log"
)

// type aliases for ease of use
type (
	uint8_t           = C.uint8_t
	uint16_t          = C.uint16_t
	uint32_t          = C.uint32_t
	uint64_t          = C.uint64_t
	size_t            = C.size_t
	MagmaShard        = C.MagmaKVStore
	MagmaShardConfig  = C.MagmaKVStoreConfig
	MagmaStringBuf    = C.SizedBuf
	MagmaKeyIterator  = C.MagmaKeyIterator
	MagmaStoreID      = uint16_t
	MagmaOp           = C.int
	MagmaRecord       = C.CRecord
	MagmaStoreStats   = C.MagmaPerKVStoreStats
	MagmaMemAllocator = C.MagmaWorkContext
	MagmaOpStatus     = C.CStatus
	MagmaSnapshot     = C.MagmaSnapHandle
	MagmaStatusCode   = C.int
	MagmaStats        = C.MagmaKVStoreStats
	MagmaDEK          = C.DataEncryptionKey
)

func init() {
	// Initialize the static C unencrypted DEK
	C.initUnencryptedDEK()
}

func defaultMagmaCfg() *MagmaShardConfig {
	numFlushers := uint64_t(1)
	cfg := C.MKV_DefaultConfig()
	cfg.EnableWAL = 1
	cfg.NumFlushers = numFlushers
	cfg.NumCompactors = numFlushers * 4
	cfg.StatsSamplePeriodSecs = 300
	cfg.MemoryQuota = 4 * 1024 * 1024
	cfg.EnableAutoCheckpointing = 1
	cfg.EnablePerKVStoreEncryptionManagement = 0
	cfg.GetEncryptionKeyCallback = (C.GetEncryptionKeyCb)(C.go_global_get_encryption_key)
	// GetEncryptionKeyCallbackCtx will be set after repo struct is created
	return &cfg
}

const (
	NaMagmaStoreID           MagmaStoreID = iota // NaMagmaStoreID -> 0; default store ID
	MainMagmaStoreID                             // MainMagmaStoreID -> 1; for repo kind MAIN
	CommitLogMagmaStoreID                        // CommitLogMagmaStoreID -> 2; for repo kind COMMIT_LOG
	ServerConfigMagmaStoreID                     // ServerConfigMagmaStoreID -> 3; for repo kind SERVER_CONFIG
	LocalMagmaStoreID                            // LocalMagmaStoreID -> 4; for repo kind LOCAL
)

func storeIDString(storeID MagmaStoreID) string {
	switch storeID {
	case MainMagmaStoreID:
		return "Main"
	case CommitLogMagmaStoreID:
		return "CommitLog"
	case ServerConfigMagmaStoreID:
		return "ServerConfig"
	case LocalMagmaStoreID:
		return "Local"
	}
	return "NA"
}

var gRepoKindToMagmaStoreIDMap = map[RepoKind]MagmaStoreID{
	MAIN:          MainMagmaStoreID,
	COMMIT_LOG:    CommitLogMagmaStoreID,
	SERVER_CONFIG: ServerConfigMagmaStoreID,
	LOCAL:         LocalMagmaStoreID,
}

var gMagmaStoreIDToRepoKindMap = map[MagmaStoreID]RepoKind{
	MainMagmaStoreID:         MAIN,
	CommitLogMagmaStoreID:    COMMIT_LOG,
	ServerConfigMagmaStoreID: SERVER_CONFIG,
	LocalMagmaStoreID:        LOCAL,
}

func repoKindToMagmaStoreID(kind RepoKind) MagmaStoreID {
	if storeID, exists := gRepoKindToMagmaStoreIDMap[kind]; exists {
		return storeID
	}
	return NaMagmaStoreID
}

func magmaStoreIDToRepoKind(storeID MagmaStoreID) RepoKind {
	if kind, exists := gMagmaStoreIDToRepoKindMap[storeID]; exists {
		return kind
	}
	return LOCAL
}

// Magma Op codes
const (
	MagmaOpInsert MagmaOp = iota
	MagmaOpUpsert
	MagmaOpDelete
	MagmaOpLocalUpdate
	MagmaOpLocalDelete
	MagmaOpDiscard
)

// Magma Error codes. translated from `magma/include/libmagma/status.h`
const (
	// No error status code
	MagmaStatusOk MagmaStatusCode = iota
	// This is a special case of code OK that indicates there were no errors
	// in processing the doc lookup request and
	// the doc was not found.
	MagmaStatusOkNotFound
	// Internal error indicates the operation failed due to violation of
	// some internal constraint or some unexpected state or encountering an
	// unexpected failure condition (for eg compression failure). These are
	// not recoverable.
	MagmaStatusInternal
	// Invalid indicates the operation or the input argument to a method is
	// not valid/supported. It is analogous to std::invalid_argument. These
	// are due to programmatic errors/improper usage of APIs by the user,
	// for example trying to open an already opened Magma instance.
	MagmaStatusInvalid
	// InvalidKVStore is a sub code under the Invalid code. It is used to
	// indicate an operation failed because the specified KVStore does not
	// exist. It is a recoverable error.
	MagmaStatusInvalidKVStore
	// Corruption means the on disk state of Magma is not what is expected.
	// Some examples of corruption: checksum mismatches, decompression
	// failures, etc. This is an unrecoverable error.
	MagmaStatusCorruption
	// NotFound indicates the requested resource does not exist. Whether
	// recovery is possible or not depends upon the context and what the
	// resource is. For example, when answering GetDocs, if a sstable is not
	// found, it means data loss and recovery is not possible.
	MagmaStatusNotFound
	// IOError is a generic error returned when a syscall related to file
	// operations fails. Recovery may be possible by addressing the cause.
	// TransientIO, DiskFull, NoAccess are special codes under the IOError.
	MagmaStatusIOError
	// ReadOnly means Magma is in a read only mode and a data modification
	// operation (file delete/create/write/truncate etc) was attempted that
	// is not allowed. This is a recoverable error. Recovery requires
	// reopening Magma is read-write mode.
	MagmaStatusReadOnly
	// TransientIO maps to an error from the filesystem. This is a
	// recoverable error. To recover, the underlying cause needs to be
	// addressed. For example, if the error is crossing the limit of number
	// of open files, increasing the limit will resolve the error.
	MagmaStatusTransientIO
	// DiskFull maps to an error from the filesystem. This is a recoverable
	// error and can be recovered from if some disk space is freed up.
	MagmaStatusDiskFull
	// Cancelled indicates an operation (for eg a compaction) was cancelled.
	// It is safe to retry the operation.
	MagmaStatusCancelled
	// RetryLater indicates the operation cannot proceed right now due to
	// some reason and should be retried later.
	MagmaStatusRetryLater
	// CheckpointNotFound indicates that a checkpoint was not found on disk.
	// It is a recoverable error. Resolving it depends on the context.
	MagmaStatusCheckpointNotFound
	// NoAccess maps to an error from the filesystem. It indicates the
	// performed syscall is not allowed based on current permissions set on
	// the file. This is recoverable if the file permissions are revised.
	MagmaStatusNoAccess
	// EncryptionKeyNotFound indicates that a required encryption key was
	// not found. It is a recoverable error once the required key is
	// provided.
	MagmaStatusEncryptionKeyNotFound
)

func magmaStatusString(code MagmaStatusCode) string {
	switch code {
	case MagmaStatusOk:
		return "StatusOk"
	case MagmaStatusOkNotFound:
		return "StatusOkNotFound"
	case MagmaStatusInternal:
		return "StatusInternal"
	case MagmaStatusInvalid:
		return "StatusInvalid"
	case MagmaStatusInvalidKVStore:
		return "StatusInvalidKVStore"
	case MagmaStatusCorruption:
		return "StatusCorruption"
	case MagmaStatusNotFound:
		return "StatusNotFound"
	case MagmaStatusIOError:
		return "StatusIOError"
	case MagmaStatusReadOnly:
		return "StatusReadOnly"
	case MagmaStatusTransientIO:
		return "StatusTransientIO"
	case MagmaStatusDiskFull:
		return "StatusDiskFull"
	case MagmaStatusCancelled:
		return "StatusCancelled"
	case MagmaStatusRetryLater:
		return "StatusRetryLater"
	case MagmaStatusCheckpointNotFound:
		return "StatusCheckpointNotFound"
	case MagmaStatusNoAccess:
		return "StatusNoAccess"
	case MagmaStatusEncryptionKeyNotFound:
		return "StatusEncryptionKeyNotFound"
	default:
		return fmt.Sprintf("StatusUnknown(%d)", code)
	}
}

func translateMagmaErrToStoreErr(status MagmaOpStatus) error {
	if status.Code == MagmaStatusOk {
		return nil
	}
	errCode := ErrInternalError
	switch status.Code {
	case MagmaStatusOkNotFound:
		errCode = ErrResultNotFoundCode
		// TODO: extend generic error codes and expand them here too
	}
	return &StoreError{
		sType: MagmaStoreType,
		errMsg: fmt.Sprintf("(%d-%s)%v",
			status.Code, magmaStatusString(status.Code),
			C.GoStringN(
				status.ErrMsg.data,       // string *C.Char
				C.int(status.ErrMsg.len), // len C.int
			),
		),
		storeCode: errCode,
		rawErr:    status.Code,
	}
}

var (
	magmaErrRepoClosed = &StoreError{
		sType:     MagmaStoreType,
		errMsg:    ErrRepoClosedCode.Error(),
		storeCode: ErrRepoClosedCode,
	}
	magmaErrIterFail = &StoreError{
		sType:     MagmaStoreType,
		errMsg:    "MAGMA_ITERATOR_FAIL",
		storeCode: ErrIterFailCode,
	}
	magmaErrNotImplemented = &StoreError{
		sType:     MagmaStoreType,
		errMsg:    "Method not implemented",
		storeCode: ErrInternalError,
	}
)

// Magma_Repository is the global interface to magma backed store
type Magma_Repository struct {
	sync.Mutex
	mInst             *MagmaShard
	storeRev          map[RepoKind]uint32_t
	storeSeqNum       map[RepoKind]*atomic.Uint64
	snapshots         map[RepoKind][]*magmaSnapContainer
	memAllocMutex     sync.Mutex // memAllocMutex is to serialize access to memAllocator.
	memAllocator      *MagmaMemAllocator
	isClosed          bool
	keyStoreCallbacks IEncryptionKeyStoreCallbacks
	dekCache          map[string]*MagmaDEK // keyID -> C-allocated DEK
	currentDEK        *MagmaDEK            // current active key
	currentKeyID      string               // track active key ID for change detection
	dekCacheMu        sync.RWMutex
	pinner            runtime.Pinner
}

const numRecsForMemAlloc = 1

func OpenMagmaRepositoryAndUpgrade(params RepoFactoryParams) (IRepository, error) {
	if params.StoreType != MagmaStoreType {
		return nil, &StoreError{
			sType:     MagmaStoreType,
			storeCode: ErrNotSupported,
			errMsg:    fmt.Sprintf("cannot open store type - %v", params.StoreType),
		}
	}

	magmaPath := filepath.Join(params.Dir, c.MAGMA_SUB_DIR)
	checkpointFilePath := filepath.Join(magmaPath, c.MAGMA_MIGRATION_MARKER)

	////////// if no `migration` file exists, cleanup stale metadata dir contents
	var doesMigrationMarkerExist = true
	checkpointFile, err := os.Open(checkpointFilePath)
	if err != nil && !os.IsNotExist(err) {
		log.Current.Errorf("OpenMagmaRepositoryAndUpgrade:: couldn't open migration marker %v due to err - %v",
			checkpointFilePath, err)
		return nil, &StoreError{
			sType:     MagmaStoreType,
			storeCode: ErrInternalError,
			errMsg:    err.Error(),
		}
	} else if os.IsNotExist(err) {
		doesMigrationMarkerExist = false
		// CORRUPTION HANDLE TODO: instead of dir removal, increment the KV store revisions or
		// backup the old files in a separate directory
		err = os.RemoveAll(magmaPath)
		if err != nil {
			log.Current.Errorf("OpenMagmaRepositoryAndUpgrade:: couldn't remove stale metadata files %v due to err - %v",
				magmaPath, err)
			return nil, &StoreError{
				sType:     MagmaStoreType,
				storeCode: ErrInternalError,
				errMsg:    err.Error(),
			}
		} else {
			log.Current.Infof("OpenMagmaRepositoryAndUpgrade:: removed metadata dir %v as no marker exists",
				magmaPath)
		}
	} else {
		checkpointFile.Close()
	}

	////////// derive magma path from base storage dir (@2i => @2i/metadata_repo_v2)
	repo, err := openMagmaRepository(
		magmaPath,
		params.MemoryQuota, params.CompactionMinFileSize,
		params.CompactionThresholdPercent,
		params.EnableWAL,
		params.EarCallbacks,
	)

	if err != nil {
		log.Current.Errorf("OpenMagmaRepositoryAndUpgrade:: Failed to open magma stores with error %v",
			err)
		return nil, err
	}

	// CORRUPTION HANDLE TODO: handle StatusNotFound, StatusCorruption for magma store file
	// corruption

	////////// check if forestDb files exist or not
	var foundFdbFile = false
	files, err := os.ReadDir(params.Dir)
	if err != nil {
		log.Current.Errorf("OpenMagmaRepositoryAndUpgrade:: Failed to read metadata dir %v with err - %v",
			params.Dir, err)
		return nil, &StoreError{
			sType:     MagmaStoreType,
			storeCode: ErrInternalError,
			errMsg:    err.Error(),
		}
	}
	for _, file := range files {
		if strings.Contains(file.Name(), c.FDB_REPOSITORY_NAME) {
			foundFdbFile = true
			break
		}
	}

	////////// open forestDb for migration
	if foundFdbFile && !doesMigrationMarkerExist {

		if params.TestSleepDur > 0 {
			sleepDur := min(time.Duration(params.TestSleepDur)*time.Second, 5*time.Minute)

			log.Current.Warnf("OpenMagmaRepositoryAndUpgrade:: test induced sleep for %v",
				sleepDur.Seconds())

			time.Sleep(sleepDur)
		}

		log.Current.Infof("OpenMagmaRepositoryAndUpgrade:: Starting metadata migration from fDb to magma...")
		var migrationStart = time.Now()

		fdbRepo, err := OpenFDbRepositoryWithParams(RepoFactoryParams{
			Dir:         params.Dir,
			MemoryQuota: params.MemoryQuota,
			StoreType:   FDbStoreType,

			// since CompactionTimerDur is not relevant for magma stores, it can empty. use a max of
			// 1000s timer duration for compaction. FDb stores will not open on 0s timer duration
			CompactionTimerDur: max(params.CompactionTimerDur, 1000),

			CompactionMinFileSize:      params.CompactionMinFileSize,
			CompactionThresholdPercent: params.CompactionThresholdPercent,
		})
		if err != nil {
			log.Current.Errorf("OpenMagmaRepositoryAndUpgrade:: Failed to read fdb metadata with err - %v",
				err)
			return nil, err
		}
		defer fdbRepo.Close()

		migrateStore := func(kind RepoKind) error {
			storeMirgrationStart := time.Now()
			iter, err := fdbRepo.NewIterator(kind, "", "")
			if err != nil {
				log.Current.Errorf("OpenMagmaRepositoryAndUpgrade:: Failed to start iterator for %v store with err - %v",
					kind, err)
				return err
			}
			defer iter.Close()

			count := 0
			item, val, iterErr := iter.Next()
			for iterErr == nil {
				count++
				// use SetNoCommit and avoid too many flushes
				setErr := repo.SetNoCommit(kind, item, val)
				if setErr != nil {
					log.Current.Errorf("OpenMagmaRepositoryAndUpgrade:: Failed to set key %v for %v store with err - %v",
						item, kind, setErr)
					return setErr
				}

				item, val, iterErr = iter.Next()
			}
			if !errors.Is(iterErr, &StoreError{storeCode: ErrIterFailCode}) {
				log.Current.Errorf("OpenMagmaRepositoryAndUpgrade:: iterator error %v on fdb store %v during migration",
					err, kind)
				return err
			}

			log.Current.Infof("OpenMagmaRepositoryAndUpgrade:: store %v migrated successfully in %v (total items %v)",
				kind, time.Since(storeMirgrationStart), count)
			return nil
		}

		var errCh = make(chan error, 4)
		var wg = sync.WaitGroup{}
		for _, kind := range []RepoKind{MAIN, SERVER_CONFIG, COMMIT_LOG, LOCAL} {
			wg.Add(1)

			// do migration in parallel. usually MAIN and COMMIT_LOG will be heavy stores so we can
			// can save time in iterator loads
			go func(kind RepoKind) {
				defer wg.Done()
				if err := migrateStore(kind); err != nil {
					errCh <- err
				}
			}(kind)
		}

		wg.Wait()
		close(errCh)

		if len(errCh) > 0 {
			for err := range errCh {
				return nil, err
			}
		}

		err = repo.Commit()
		if err != nil {
			log.Current.Errorf("OpenMagmaRepositoryAndUpgrade:: failed to sync magma stores to disk with error - %v",
				err)
			return nil, err
		}

		////////// MIGRATION verification
		log.Current.Infof("OpenMagmaRepositoryAndUpgrade:: verifying migration...")
		err = verifyMigration(fdbRepo, repo)
		if err != nil {
			log.Current.Errorf("OpenMagmaRepositoryAndUpgrade:: migration verification failed: %v",
				err)
			return nil, err
		}

		log.Current.Infof("OpenMagmaRepositoryAndUpgrade:: metadata migration successful. time spent %v. will save migration marker file",
			time.Since(migrationStart))
	} else if !foundFdbFile {
		log.Current.Infof("OpenMagmaRepositoryAndUpgrade:: no old metadata files found. no migration is performed")
	}

	////////// save marker file to confirm migration is completed
	// do this even if no migration is performed to ensure we don't treat valid metadata files as
	// stale files, which get deleted to ensure consistency, after an indexer restarts.
	if !doesMigrationMarkerExist {
		now := time.Now().String()
		// WriteFile is not atomic. it can lead to a partial state on disk. for our use case, just
		// a presence of the file eeans succesful migration hence it is fine

		err = os.WriteFile(checkpointFilePath, []byte(now), 0x0777)
		if err != nil {
			log.Current.Errorf("OpenMagmaRepositoryAndUpgrade:: couldn't write migration marker to disk. err - %v",
				err)
			return nil, &StoreError{
				sType:     MagmaStoreType,
				errMsg:    err.Error(),
				storeCode: ErrInternalError,
			}
		}
		log.Current.Infof("OpenMagmaRepositoryAndUpgrade:: save metadata migration file successfully")
	} else if foundFdbFile {
		err = DestroyRepositoryWithParams(RepoFactoryParams{
			Dir:         params.Dir,
			MemoryQuota: params.MemoryQuota,
			StoreType:   FDbStoreType,

			// since CompactionTimerDur is not relevant for magma stores, it can empty. use a max of
			// 1000s timer duration for compaction. FDb stores will not open on 0s timer duration
			CompactionTimerDur: max(params.CompactionTimerDur, 1000),

			CompactionMinFileSize:      params.CompactionMinFileSize,
			CompactionThresholdPercent: params.CompactionThresholdPercent,
		})
		if err != nil {
			return nil, err
		}
	}

	return repo, err

}

func verifyMigration(fdbRepo, magmaRepo IRepository) error {
	verificationStartTime := time.Now()

	var storesToVerify = []RepoKind{MAIN, SERVER_CONFIG, COMMIT_LOG, LOCAL}
	var overallErrors []string

	for _, kind := range storesToVerify {
		storeVerificationStart := time.Now()

		var (
			fdbItemStoreCount   = fdbRepo.GetItemsCount(kind)
			magmaItemStoreCount = magmaRepo.GetItemsCount(kind)
		)

		if fdbItemStoreCount != magmaItemStoreCount {
			errMsg := fmt.Sprintf("item count mismatch for %v store - fdb: %d, magma: %d",
				kind, fdbItemStoreCount, magmaItemStoreCount)
			log.Current.Errorf("OpenMagmaRepositoryAndUpgrade::verifyMigration: %v", errMsg)
			overallErrors = append(overallErrors, errMsg)
			continue
		}

		if fdbItemStoreCount == 0 {
			log.Current.Infof("verifyMigration: store %v has 0 items, skipping key verification",
				kind)
			continue
		}

		var (
			missingKeys     []string
			valueMismatches []string
			keysVerified    uint64
		)

		switch kind {
		case MAIN:
			sampleSize := max((fdbItemStoreCount*10)/100, 100)
			log.Current.Infof("verifyMigration: sampling %d keys from MAIN store",
				sampleSize)

			keysVerified, missingKeys, valueMismatches = sampleAndVerifyKeys(
				fdbRepo, magmaRepo,
				kind,
				sampleSize, fdbItemStoreCount,
			)
		case SERVER_CONFIG:
			log.Current.Infof("verifyMigration: verifying ALL keys in SERVER_CONFIG store")

			keysVerified, missingKeys, valueMismatches = sampleAndVerifyKeys(
				fdbRepo, magmaRepo,
				kind,
				fdbItemStoreCount, fdbItemStoreCount,
			)
		default:
			log.Current.Infof("verifyMigration: store %v has %d items,"+
				" skipping key verification (only MAIN and SERVER_CONFIG verified)",
				kind, fdbItemStoreCount)
		}

		if len(missingKeys) > 0 {
			errMsg := fmt.Sprintf("verifyMigration: found %d missing keys in %v store",
				len(missingKeys), kind)
			log.Current.Errorf("%s. Missing keys: %v", errMsg, missingKeys)
			overallErrors = append(overallErrors, errMsg)
		}

		if len(valueMismatches) > 0 {
			errMsg := fmt.Sprintf("verifyMigration: found %d value mismatches in %v store",
				len(valueMismatches), kind)
			log.Current.Errorf("%s. Mismatched keys: %v", errMsg, valueMismatches)
			overallErrors = append(overallErrors, errMsg)
		}

		log.Current.Infof(
			"verifyMigration: store %v verification completed in %v - verified %d keys",
			kind,
			time.Since(storeVerificationStart),
			keysVerified,
		)
	}

	if len(overallErrors) > 0 {
		err := &StoreError{
			sType:     MagmaStoreType,
			storeCode: ErrMigrationVerificationFailure,
			errMsg: fmt.Sprintf("migration verification failed with %d errors: %v",
				len(overallErrors), strings.Join(overallErrors, "; ")),
		}
		log.Current.Errorf("verifyMigration: completed with errors in %v - %v",
			time.Since(verificationStartTime), err.Error())
		return err
	}

	log.Current.Infof("verifyMigration: all verification passed successfully in %v",
		time.Since(verificationStartTime))
	return nil
}

func sampleAndVerifyKeys(
	fdbRepo, magmaRepo IRepository,
	kind RepoKind,
	sampleSize, totalItems uint64,
) (uint64, []string, []string) {

	keys := collectAllKeys(fdbRepo, kind)
	if len(keys) == 0 {
		return 0, nil, nil
	}

	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	keysToVerify := keys
	if int(sampleSize) < len(keys) {
		keysToVerify = keys[:sampleSize]
	}

	var missingKeys []string
	var valueMismatches []string
	var verifiedCount uint64

	for _, key := range keysToVerify {
		fdbVal, fdbErr := fdbRepo.Get(kind, key)
		if fdbErr != nil {
			if storeErr, ok := fdbErr.(*StoreError); ok &&
				storeErr.Code() == ErrResultNotFoundCode {

				missingKeys = append(missingKeys, key)
				continue
			}
			log.Current.Errorf("sampleAndVerifyKeys: failed to read key %v from fdb %v store: %v",
				key, kind, fdbErr)
			continue
		}

		magmaVal, magmaErr := magmaRepo.Get(kind, key)
		if magmaErr != nil {
			if storeErr, ok := magmaErr.(*StoreError); ok &&
				storeErr.Code() == ErrResultNotFoundCode {

				missingKeys = append(missingKeys, key)
				continue
			}
			log.Current.Errorf("sampleAndVerifyKeys: failed to read key %v from magma %v store: %v",
				key, kind, magmaErr)
			continue
		}

		if !reflect.DeepEqual(fdbVal, magmaVal) {
			valueMismatches = append(valueMismatches, key)
		}
		verifiedCount++
	}

	return verifiedCount, missingKeys, valueMismatches
}

func collectAllKeys(repo IRepository, kind RepoKind) []string {
	iter, err := repo.NewIterator(kind, "", "")
	if err != nil {
		log.Current.Errorf("collectAllKeys: failed to create iterator for %v store: %v",
			kind, err)
		return nil
	}
	defer iter.Close()

	keys := make([]string, 0)

	for {
		key, _, err := iter.Next()
		if err != nil {
			if storeErr, ok := err.(*StoreError); ok &&
				storeErr.Code() == ErrIterFailCode {

				break
			}
			log.Current.Errorf("collectAllKeys: iterator error for %v store: %v", kind, err)
			break
		}
		keys = append(keys, key)
	}

	return keys
}

func openMagmaRepository(
	path string,
	memoryQuota, minTreeSize uint64,
	compactionThreshold uint8,
	enableWAL bool,
	earCallbacks IEncryptionKeyStoreCallbacks,
) (IRepository, error) {

	m_repo := &Magma_Repository{
		storeRev:     make(map[RepoKind]uint32_t, 4),
		snapshots:    make(map[RepoKind][]*magmaSnapContainer, 4),
		storeSeqNum:  make(map[RepoKind]*atomic.Uint64, 4),
		memAllocator: C.MKV_CreateWorkContext(numRecsForMemAlloc /*numRecs C.size_t*/),
	}

	if earCallbacks != nil {
		m_repo.keyStoreCallbacks = earCallbacks
		m_repo.dekCache = make(map[string]*MagmaDEK)
	}

	cfg := defaultMagmaCfg()
	m_repo.pinner.Pin(m_repo)
	cfg.GetEncryptionKeyCallbackCtx = unsafe.Pointer(m_repo)

	if enableWAL {
		cfg.EnableWAL = 1
	} else {
		cfg.EnableWAL = 0
	}

	lsdFragRatio := C.double(compactionThreshold) / 100
	cfg.LSDFragmentationRatio = lsdFragRatio

	cfg.MemoryQuota = size_t(memoryQuota)
	cfg.LSMMinCompactSize = size_t(minTreeSize)

	c_path := C.CString(path)
	defer C.free(unsafe.Pointer(c_path))

	log.Current.Debugf("OpenMagmaRepository: using path %s, config %+v", path, cfg)

	mInst := C.NewMagmaKVStore(
		c_path, // path *C.char
		cfg,    // config *C.MagmaKVStoreConfig
	)
	cstatus := C.MKV_Open(mInst /*inst *C.MagmaKVStore*/)
	if err := translateMagmaErrToStoreErr(cstatus); err != nil {
		log.Current.Errorf("OpenMagmaRepository: magma open failed with err - %v", err)
		return nil, err
	}

	m_repo.mInst = mInst

	for _, storeId := range []MagmaStoreID{
		MainMagmaStoreID, CommitLogMagmaStoreID, ServerConfigMagmaStoreID, LocalMagmaStoreID,
	} {
		repoKind := magmaStoreIDToRepoKind(storeId)
		m_repo.storeSeqNum[repoKind] = &atomic.Uint64{}
		if C.MKV_KVStoreExists(mInst /* *C.MagmaKVStore*/, storeId /*C.uint16_t*/) == 1 {
			var rev uint32_t
			cstatus := C.MKV_GetKVStoreRevision(
				mInst,   // *C.MagmaKVStore
				storeId, // C.uint16_t
				&rev,    // *C.uint32_t
			)
			if err := translateMagmaErrToStoreErr(cstatus); err != nil {
				log.Current.Errorf("OpenMagmaRepository: failed to read KV store revision for %v with error - %v",
					storeIDString(storeId), err)
				return nil, err
			}
			m_repo.storeRev[repoKind] = rev

			var seqNum uint64_t
			cstatus = C.MKV_GetMaxSeqNo(
				mInst,   // *C.MagmaKVStore
				storeId, // C.uint16_t
				&seqNum, // *C.uint64_t
			)
			if err := translateMagmaErrToStoreErr(cstatus); err != nil {
				log.Current.Errorf("OpenMagmaRepository failed to read Max Seq Num for %v with error - %v",
					storeIDString(storeId), err)
				return nil, err
			}
			m_repo.storeSeqNum[repoKind].Store(uint64(seqNum))

			log.Current.Infof("OpenMagmaRepository: found store %v with rev %v highSeqNo %v",
				storeIDString(storeId), rev, seqNum)
		} else {
			m_repo.storeRev[repoKind] = 1
			cstatus = C.MKV_CreateKVStore(
				mInst,                     // *C.MagmaKVStore
				storeId,                   // C.uint16_t
				m_repo.storeRev[repoKind], // C.uint32_t
			)
			if err := translateMagmaErrToStoreErr(cstatus); err != nil {
				log.Current.Errorf("OpenMagmaRepository: store %v creation fails with error - %v",
					storeIDString(storeId), err)
				return nil, err
			}
			log.Current.Infof("OpenMagmaRepository: created kv store %v using rev 1",
				storeIDString(storeId))
		}
	}

	return m_repo, nil
}

func (m_repo *Magma_Repository) Close() {
	m_repo.Lock()
	defer m_repo.Unlock()
	if !m_repo.isClosed {
		m_repo.isClosed = true

		for _, storeId := range []MagmaStoreID{
			MainMagmaStoreID, CommitLogMagmaStoreID, ServerConfigMagmaStoreID, LocalMagmaStoreID,
		} {
			repoKind := magmaStoreIDToRepoKind(storeId)
			for _, snapContainer := range m_repo.snapshots[repoKind] {
				// FIXME: should we only call this for snapContainers which have refCount == 0?
				snapContainer.close()
			}
		}

		// Free C-allocated DEK cache (allocated via jemalloc)
		m_repo.dekCacheMu.Lock()
		for keyID, dek := range m_repo.dekCache {
			if keyID == "" {
				continue
			}
			C.je_free(unsafe.Pointer(dek.id))
			C.je_free(unsafe.Pointer(dek.cipher))
			if dek.key.data != nil {
				C.je_free(unsafe.Pointer(dek.key.data))
			}
			C.je_free(unsafe.Pointer(dek))
		}
		m_repo.dekCache = nil
		m_repo.currentDEK = nil
		m_repo.dekCacheMu.Unlock()

		m_repo.pinner.Unpin()

		C.MKV_DestroyWorkContext(m_repo.memAllocator /*ctx *C.MagmaWorkContext*/)
		C.DestroyMagmaKVStore(m_repo.mInst /*inst *C.MagmaKVStore*/)
		m_repo.mInst = nil
		log.Current.Infof("MagmaRepository:Close:: closed magma repo")
	}
}

func convert2GoBytesNoCopy(data unsafe.Pointer, n int) []byte {
	var bs []byte

	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&bs))
	hdr.Data = uintptr(data)
	hdr.Len = n
	hdr.Cap = hdr.Len
	return bs
}

func createSizedBufUsingValue[T string | []byte](val T) MagmaStringBuf {
	bufLen := size_t(len(val))
	// this will allocate memory using the C memory allocator used by magma (aka je_malloc)
	buf := C.MKV_Alloc(bufLen /*len C.size_t*/)
	bufBytes := convert2GoBytesNoCopy(unsafe.Pointer(buf.data), int(buf.len))
	copy(bufBytes, val)
	return buf
}

func newWriteMagmaRecord(op MagmaOp, key string, value, meta []byte) *MagmaRecord {
	return &MagmaRecord{
		Op:   op,
		Key:  createSizedBufUsingValue(key),
		Val:  createSizedBufUsingValue(value),
		Meta: createSizedBufUsingValue(meta),
	}
}

func (m_repo *Magma_Repository) setNoCommitNoLock(kind RepoKind, key string, content []byte) error {
	storeID := repoKindToMagmaStoreID(kind)

	// write with increasing seqNum as it is a requirement for magma stores
	seqNum := m_repo.storeSeqNum[kind].Add(1)

	meta := binary.LittleEndian.AppendUint64(nil, seqNum)

	// newWriteMagmaRecord performs copy/memcopy from golang strings,bytes to c allocated memory
	// this is because we want to pass ownership of the key,content memory to magma. it should
	// not be freed or overwritten by the golang GC
	rec := newWriteMagmaRecord(MagmaOpUpsert, key, content, meta)

	log.Current.Debugf("MagmaRepository:SetNCNL:: {key: %s, len(val): %d, seqNo: %d, sID: %d}",
		key, len(content), seqNum, storeID)

	// Magma write API is designed with batch inserts in mind. we always insert a single key,val
	cstatus := C.MKV_Write(
		m_repo.mInst,          // *C.MagmaKVStore
		storeID,               // C.uint16_t
		m_repo.storeRev[kind], // C.uint32_t
		rec,                   // recs (*C.CRecord) -> []{Op, C.SizedBuf, C.SizedBuf, C.SizedBuf}
		1,                     // numRecs C.size_t
	)
	err := translateMagmaErrToStoreErr(cstatus)
	if err != nil {
		log.Current.Errorf("MagmaRepository::SetNCNL:: write error {key: %s, len(val): %d, seqNo: %d, store: %s, err: %v}",
			key, len(content), seqNum, storeIDString(storeID), err)
	}
	return err
}

func (m_repo *Magma_Repository) SetNoCommit(kind RepoKind, key string, content []byte) error {
	m_repo.Lock()
	defer m_repo.Unlock()

	if m_repo.isClosed {
		log.Current.Warnf("MagmaRepository:SetNC:: on closed repo {key: %s, len(val): %d, repo: %v}",
			key, len(content), kind)
		return magmaErrRepoClosed
	}

	return m_repo.setNoCommitNoLock(kind, key, content)
}

func (m_repo *Magma_Repository) Set(kind RepoKind, key string, content []byte) error {
	m_repo.Lock()
	defer m_repo.Unlock()

	if m_repo.isClosed {
		log.Current.Warnf("MagmaRepository:Set:: on closed repo {key: %s, len(val): %d, repo: %v}",
			key, len(content), kind)
		return magmaErrRepoClosed
	}

	if err := m_repo.setNoCommitNoLock(kind, key, content); err != nil {
		return err
	}

	// On every SyncKVStore, magma dumps the current memtable to SS table. this can lead to an issue
	// that we are creating very small SSTables on disk. The files are 8Kb large but barely have
	// data in them. Since we have WAL enabled, we can rather skip the sync to disk as the WAL
	// guarantees durable writes

	// storeID := repoKindToMagmaStoreID(kind)

	// cstatus := C.MKV_SyncKVStore(
	// 	m_repo.mInst, // *C.MagmaKVStore
	// 	storeID,      // C.uint16_t
	// )
	// err := translateMagmaErrToStoreErr(cstatus)
	// if err != nil {
	// 	log.Current.Errorf("MagmaRepository::Set:: sync error {key: %s, len(val): %d, store: %s, err: %v}",
	// 		key, len(content), storeIDString(storeID), err)
	// }
	return nil
}

func (m_repo *Magma_Repository) Get(kind RepoKind, key string) ([]byte, error) {
	m_repo.Lock()
	defer m_repo.Unlock()

	if m_repo.isClosed {
		log.Current.Warnf("MagmaRepository:Get:: on closed repo {key: %s, repo: %v}",
			key, kind)
		return nil, magmaErrRepoClosed
	}

	return m_repo.getNoLock(kind, key)
}

func (m_repo *Magma_Repository) getNoLock(kind RepoKind, key string) ([]byte, error) {

	storeID := repoKindToMagmaStoreID(kind)

	var ckey, ckeyLen = C.CString(key), size_t(len(key))
	defer C.free(unsafe.Pointer(ckey))

	var val MagmaStringBuf

	m_repo.memAllocMutex.Lock()
	// Lookup will load value into val. the memory for it will be allocated by magma work context
	// this work context will allocate memory in the C world. so we need to copy the values into
	// golang memory
	cstatus := C.MKV_Lookup(
		m_repo.mInst,        // *C.MagmaKVStore
		storeID,             // C.uint16_t
		ckey,                // *C.char
		ckeyLen,             // C.size_t
		&val,                // *C.SizedBuf
		m_repo.memAllocator, // *C.MagmaWorkContext
	)
	m_repo.memAllocMutex.Unlock()

	if cstatus.Code != 0 {
		err := translateMagmaErrToStoreErr(cstatus)
		// reading non existent values is a valid API call. it will return an error.
		// do not log it at error level for missing item
		if merr, ok := err.(*StoreError); ok &&
			merr != nil && merr.Code() != ErrResultNotFoundCode {
			log.Current.Warnf("MagmaRepository:GetNL:: {key: %s, err: %v, sID: %d}",
				key, err, storeID)
		} else {
			log.Current.Debugf("MagmaRepository:GetNL:: {key: %s, err: %v, sID: %d}",
				key, err, storeID)
		}
		return nil, err
	}

	res := C.GoBytes(unsafe.Pointer(val.data), C.int(val.len))

	log.Current.Debugf("MagmaRepository:GetNL:: {key: %s, len(val): %v, sID: %d}",
		key, len(res), storeID)
	return res, nil
}

func (m_repo *Magma_Repository) deleteNoCommitNoLock(kind RepoKind, key string) error {

	// verify that key exists. if not, don't attempt delete as it can hang magma compaction
	if _, err := m_repo.getNoLock(kind, key); err != nil {
		magmaErr, ok := err.(*StoreError)
		if ok && magmaErr.storeCode == ErrResultNotFoundCode {
			// we should return nil if key does not exist
			return nil
		}
		return err
	}

	storeID := repoKindToMagmaStoreID(kind)

	// write with increasing seqNum as it is a requirement for magma stores
	seqNum := m_repo.storeSeqNum[kind].Add(1)

	meta := binary.LittleEndian.AppendUint64(nil, seqNum)
	rec := newWriteMagmaRecord(MagmaOpDelete, key, nil, meta)

	log.Current.Debugf("MagmaRepository:DeleteNCNL:: {key: %s, seqNo: %d, sID: %d}",
		key, seqNum, storeID)

	// to delete records from Magma, we have to use the Write API itself but the OpCode is
	// MagmaOpDelete with increasing seqNum
	cstatus := C.MKV_Write(
		m_repo.mInst,          // *C.MagmaKVStore
		storeID,               // C.uint16_t
		m_repo.storeRev[kind], // C.uint32_t
		rec,                   // recs (*C.CRecord) -> []{Op, C.SizedBuf, C.SizedBuf, C.SizedBuf}
		1,                     // numRecs C.size_t
	)

	err := translateMagmaErrToStoreErr(cstatus)
	if err != nil {
		log.Current.Errorf("MagmaRepository::DeleteNCNL:: write error {key: %s, store: %s, err: %v}",
			key, storeIDString(storeID), err)
	}
	return err
}

func (m_repo *Magma_Repository) DeleteNoCommit(kind RepoKind, key string) error {
	m_repo.Lock()
	defer m_repo.Unlock()

	if m_repo.isClosed {
		log.Current.Warnf("MagmaRepository:DeleteNC:: on closed repo {key: %s, repo: %v}",
			key, kind)
		return magmaErrRepoClosed
	}

	return m_repo.deleteNoCommitNoLock(kind, key)
}

func (m_repo *Magma_Repository) Delete(kind RepoKind, key string) error {
	m_repo.Lock()
	defer m_repo.Unlock()

	if m_repo.isClosed {
		log.Current.Warnf("MagmaRepository:Delete:: on closed repo {key: %s, repo: %v}",
			key, kind)
		return magmaErrRepoClosed
	}

	if err := m_repo.deleteNoCommitNoLock(kind, key); err != nil {
		return err
	}

	// continue to sync KV store for deletes. Refer MB-70588 for more info
	storeID := repoKindToMagmaStoreID(kind)

	cstatus := C.MKV_SyncKVStore(
		m_repo.mInst, // *C.MagmaKVStore
		storeID,      // C.uint16_t
	)
	err := translateMagmaErrToStoreErr(cstatus)
	if err != nil {
		log.Current.Errorf("MagmaRepository::Delete:: sync error {key: %s, store: %s, err: %v}",
			key, storeIDString(storeID), err)
	}

	return err
}

// read entire stats object for a store. take lock on m_repo before calling this
func (m_repo *Magma_Repository) getRepoStats(kind RepoKind) *MagmaStoreStats {
	storeID := repoKindToMagmaStoreID(kind)

	statObj := &MagmaStoreStats{}

	C.MKV_GetKVStoreStats(
		m_repo.mInst, // *C.MagmaKVStore
		statObj,      // *C.MagmaPerKVStoreStats
		storeID,      // C.uint16_t
		1,            // range C.int (aggregate KVStats over range of kv stores)
	)

	return statObj
}

func (m_repo *Magma_Repository) getStoreStats() *MagmaStats {
	statObj := &MagmaStats{}

	C.MKV_GetStats(
		m_repo.mInst,
		statObj,
	)

	return statObj
}

// read buf memory used by magma. take lock on m_repo before calling this
func (m_repo *Magma_Repository) getBufMemoryUsedNoLock() uint64 {
	m_repo.memAllocMutex.Lock()
	defer m_repo.memAllocMutex.Unlock()
	return uint64(C.MKV_GetWorkContextMemUsed(m_repo.memAllocator /**C.MagmaWorkContext*/))
}

func (m_repo *Magma_Repository) Commit() error {
	m_repo.Lock()
	defer m_repo.Unlock()

	if m_repo.isClosed {
		log.Current.Warnf("MagmaRepository:Commit:: on closed repo")
		return magmaErrRepoClosed
	}

	cstatus := C.MKV_Sync(m_repo.mInst /* *C.MagmaKVStore */)
	if err := translateMagmaErrToStoreErr(cstatus); err != nil {
		log.Current.Errorf("MagmaRepository:Commit:: repo sync error - %v", err)
		return err
	}
	return nil
}

func (m_repo *Magma_Repository) Type() StoreType {
	return MagmaStoreType
}

type MagmaKIterWrapper struct {
	sync.Mutex
	iter             *MagmaKeyIterator
	snapC            *MagmaSnapshot // in case we use NewKeyIterator to get latest snapshot
	isClosed         bool
	startKey, endKey string
	kind             RepoKind
}

func NewMagmaKIterWrapper(
	iter *MagmaKeyIterator, startKey, endKey string, kind RepoKind, snapC *MagmaSnapshot,
) *MagmaKIterWrapper {
	if iter == nil {
		return nil
	}
	if len(startKey) != 0 {
		arr, arrLen := C.CString(startKey), size_t(len(startKey))
		defer C.free(unsafe.Pointer(arr))
		buf := &MagmaStringBuf{data: arr, len: arrLen}

		err := translateMagmaErrToStoreErr(C.MKVItr_Seek(
			iter, // *C.MagmaKeyIterator
			buf,  // startKey *C.char
		))
		if err != nil {
			log.Current.Errorf("NewMagmaKIterWrapper: couldn't perform seek to key %v. iterator error - %v",
				err)
			return nil
		}
	} else {
		// perform Seek to move cursor of iterator to the start. MagmaIterators will not return
		// data without this
		status := translateMagmaErrToStoreErr(C.MKVItr_SeekFirst(iter /**C.MagmaKeyIterator*/))
		if status != nil {
			log.Current.Errorf("NewMagmaKIterWrapper couldn't perform first seek on iterator for kind %v, iterator error - %v", kind, status)
			return nil
		}
	}
	log.Current.Debugf("NewMagmaKIterWrapper: created iterator for kind %d, startKey: %s, endKey: %s",
		kind, startKey, endKey)
	return &MagmaKIterWrapper{
		iter:     iter,
		startKey: startKey,
		endKey:   endKey,
		kind:     kind,
		snapC:    snapC,
	}
}

func (wrapper *MagmaKIterWrapper) closeNoLock() {
	if !wrapper.isClosed || wrapper.iter != nil {
		C.MKV_DestroyKeyIterator(wrapper.iter /**C.MagmaKeyIterator*/)
		if wrapper.snapC != nil {
			C.MKV_PutSnapshot(wrapper.snapC /**C.MagmaSnapHandle*/)
		}
		wrapper.isClosed = true
		wrapper.iter = nil
		log.Current.Debugf("MagmaKIterWrapper:closeNL:: closed iterator for kind %d",
			wrapper.kind)
	}
}

func (wrapper *MagmaKIterWrapper) Close() {
	if wrapper == nil {
		return
	}
	wrapper.Lock()
	defer wrapper.Unlock()
	wrapper.closeNoLock()
}

func (wrapper *MagmaKIterWrapper) Next() (string, []byte, error) {
	if wrapper == nil {
		return "", nil, magmaErrIterFail
	}

	wrapper.Lock()
	defer wrapper.Unlock()

	if wrapper.isClosed || wrapper.iter == nil {
		return "", nil, magmaErrIterFail
	}

	var key, val MagmaStringBuf
	isOk := C.MKVItr_GetRecord(
		wrapper.iter, // *C.MagmaKeyIterator
		&key,         // *C.SizedBuf
		&val,         // *C.SizedBuf
		0,            // int fetchMeta instead of value on 1
	) == 0
	if !isOk {
		if C.MKVItr_HasError(wrapper.iter /* *C.MagmaKeyIterator */) == 1 {
			err := C.GoString(C.MKVItr_GetError(wrapper.iter /* *C.MagmaKeyIterator */))
			log.Current.Errorf(
				"MagmaKIterWrapper::Next: iter encountered an err - %v, kind: %",
				err,
				wrapper.kind,
			)
			wrapper.closeNoLock()
		}
		return "", nil, magmaErrIterFail
	}

	// always copy key and val
	goKey := C.GoStringN(key.data, C.int(key.len))
	goVal := C.GoBytes(unsafe.Pointer(val.data), C.int(val.len))

	// sorted iterator returns key larger than endKey, close iterator and return
	if len(wrapper.endKey) > 0 && goKey > wrapper.endKey {
		// safety call. if any wrapper goes unclosed, we have open snapshots which prevents magma
		// close
		wrapper.closeNoLock()
		return "", nil, magmaErrIterFail
	}

	var retErr error
	C.MKVItr_Next(wrapper.iter /* *C.MagmaKeyIterator */)
	isValid := C.MKVItr_Valid(wrapper.iter /* *C.MagmaKeyIterator */) == 1
	if !isValid {
		// safety call. if any wrapper goes unclosed, we have open snapshots which prevents magma
		// close
		wrapper.closeNoLock()
	}

	return goKey, goVal, retErr
}

type magmaSnapContainer struct {
	snap      *MagmaSnapshot
	txnID     c.Txnid
	refCount  uint64
	iterators []*MagmaKIterWrapper
}

func (snapC *magmaSnapContainer) close() {
	// close all open iterators as we are going to destroy the underlying snapshot
	for _, iter := range snapC.iterators {
		iter.Close()
	}
	C.MKV_PutSnapshot(snapC.snap /* *C.MagmaSnapHandle */)
	log.Current.Debugf("MagmaSnapContainer:close:: closed snapshot %p", snapC)
}

func (m_repo *Magma_Repository) CreateSnapshot(kind RepoKind, txnid c.Txnid) error {
	m_repo.Lock()
	defer m_repo.Unlock()
	if m_repo.isClosed {
		log.Current.Warnf("MagmaRepository:CreateSnapshot:: on closed repo {kind: %d, txnid: %d}",
			kind, txnid)
		return magmaErrRepoClosed
	}

	storeID := repoKindToMagmaStoreID(kind)

	C.MKV_SyncKVStore(m_repo.mInst, storeID)

	snap := C.MKV_GetSnapshot(
		m_repo.mInst, /* *C.MagmaKVStore */
		storeID,      /* C.uint16_t */
		1,            /* C.int_t - use a disk snapshot on 1 */
	)
	if snap == nil {
		return translateMagmaErrToStoreErr(MagmaOpStatus{
			Code: MagmaStatusInternal,
		})
	}

	container := &magmaSnapContainer{
		snap:     snap,
		txnID:    txnid,
		refCount: 0,
	}

	// pruneSnapshots first then append
	m_repo.pruneSnapshotsNoLock(kind)

	m_repo.snapshots[kind] = append(m_repo.snapshots[kind], container)

	log.Current.Debugf("MagmaRepository:CreateSnapshot:: created snapshot %p for kind %d, txnid: %d",
		container, kind, txnid)

	return nil
}

func (m_repo *Magma_Repository) AcquireSnapshot(kind RepoKind) (c.Txnid, IRepoIterator, error) {
	m_repo.Lock()
	defer m_repo.Unlock()

	if m_repo.isClosed {
		log.Current.Warnf("MagmaRepository:AcquireSnapshot:: on closed repo {kind: %d}",
			kind)
		return 0, nil, magmaErrRepoClosed
	}

	if len(m_repo.snapshots[kind]) == 0 {
		log.Current.Warnf("MagmaRepository:AcquireSnapshot:: no snapshots for kind %d",
			kind)
		return 0, nil, nil
	}

	snapContainer := m_repo.snapshots[kind][len(m_repo.snapshots[kind])-1]
	snapContainer.refCount++

	iter := C.MKV_NewKeyIterator(
		m_repo.mInst,       /* *C.MagmaKVStore */
		snapContainer.snap, /* *C.MagmaSnapHandle */
	)
	if iter == nil {
		log.Current.Warnf("MagmaRepository:AcquireSnapshot:: failed to create iterator for kind %d, txnid: %d",
			kind, snapContainer.txnID)
		return 0, nil, translateMagmaErrToStoreErr(MagmaOpStatus{
			Code: MagmaStatusInternal,
		})
	}

	// do not pass snapC here. refer to NewIterator for more info about snapC. we do not pass
	// snapC there because we don't want to close the underlying magma snapshot even if
	// the iterator is closed. we could still create more iterators from the same snapshot
	iterWrapper := NewMagmaKIterWrapper(iter, "", "", kind, nil)
	snapContainer.iterators = append(snapContainer.iterators, iterWrapper)

	log.Current.Debugf("MagmaRepository:AcquireSnapshot:: acquired snapshot %p for kind %d, txnid: %d",
		snapContainer, kind, snapContainer.txnID)

	return snapContainer.txnID, iterWrapper, nil
}

// ReleaseSnapshot - does not release magma snapshot. call pruneSnapshotsNoLock for that
func (m_repo *Magma_Repository) ReleaseSnapshot(kind RepoKind, txnid c.Txnid) {
	m_repo.Lock()
	defer m_repo.Unlock()

	// dont check for isClosed here. we could have closed the repo and this is called for cleanup

	for _, snapContiner := range m_repo.snapshots[kind] {
		if snapContiner.txnID == txnid && snapContiner.refCount > 0 {
			snapContiner.refCount--
			log.Current.Debugf("MagmaRepository:ReleaseSnapshot:: ref count decr %v for snapshot %p for kind %d, txnid: %d",
				snapContiner.refCount, snapContiner.snap, kind, txnid)
			// do not release snapshot. we could still call AcquireSnapshot to get a snapshot
		}
	}
}

func (m_repo *Magma_Repository) NewIterator(kind RepoKind,
	startKey, endKey string,
) (IRepoIterator, error) {
	m_repo.Lock()
	defer m_repo.Unlock()

	if m_repo.isClosed {
		log.Current.Warnf("MagmaRepository:NewIterator:: on closed repo {kind: %d, startKey: %s, endKey: %s}",
			kind, startKey, endKey)
		return nil, magmaErrRepoClosed
	}

	storeID := repoKindToMagmaStoreID(kind)

	C.MKV_SyncKVStore(
		m_repo.mInst, /* *C.MagmaKVStore */
		storeID,      /* C.uint16_t */
	)

	// currently, magma NewKeyIterator API does not work without a snapshot. so create a magma
	// snapshot before calling new iterator
	snap := C.MKV_GetSnapshot(
		m_repo.mInst, /* *C.MagmaKVStore */
		storeID,      /* C.uint16_t */
		1 /*int diskSnap*/)
	if snap == nil {
		return nil, translateMagmaErrToStoreErr(MagmaOpStatus{
			Code: MagmaStatusInternal,
		})
	}

	iter := C.MKV_NewKeyIterator(m_repo.mInst, snap)
	if iter == nil {
		log.Current.Warnf("MagmaRepository:NewIterator:: failed to create iterator for kind %d, startKey: %s, endKey: %s",
			kind, startKey, endKey)
		return nil, translateMagmaErrToStoreErr(MagmaOpStatus{
			Code: MagmaStatusInternal,
		})
	}

	// keep a track of the magma snapshot. when we close iterators which are directly created
	// from NewIterator API, we will have to release the magma snapshot back on iterator close
	return NewMagmaKIterWrapper(iter, startKey, endKey, kind, snap), nil
}

func (m_repo *Magma_Repository) pruneSnapshotsNoLock(kind RepoKind) {
	newList := make([]*magmaSnapContainer, 0, len(m_repo.snapshots[kind]))
	for _, snapContainer := range m_repo.snapshots[kind] {
		if snapContainer.refCount <= 0 {
			log.Current.Debugf("MagmaRepository:pruneSnapshotsNL:: closing 0-ref snapshot %p for kind %d, txnid: %d",
				snapContainer, kind, snapContainer.txnID)
			snapContainer.close()
		} else {
			newList = append(newList, snapContainer)
		}
	}
	m_repo.snapshots[kind] = newList
	log.Current.Debugf("MagmaRepository:pruneSnapshotsNL:: open snapshots for kind %d: %d",
		kind, len(newList))
}

func (m_repo *Magma_Repository) GetStoreStats() MetastoreStats {

	m_repo.Lock()
	defer m_repo.Unlock()

	if m_repo.isClosed {
		return MetastoreStats{
			Type: MagmaStoreType,
		}
	}

	var diskInUse uint64 = 0
	var rawStats = make(map[string]interface{})

	if statObj := m_repo.getStoreStats(); statObj != nil {
		rawStats["magma_stats"] = statObj
		diskInUse = uint64(statObj.TotalDiskUsage)
	}

	var itemsCount uint64 = 0

	if statsObj := m_repo.getRepoStats(MAIN); statsObj != nil {
		itemsCount = uint64(statsObj.ItemCount)
		rawStats["main_store"] = statsObj
	}

	if statsObj := m_repo.getRepoStats(SERVER_CONFIG); statsObj != nil {
		rawStats["server_config_store"] = statsObj
	}
	if statsObj := m_repo.getRepoStats(COMMIT_LOG); statsObj != nil {
		rawStats["commit_log_store"] = statsObj
	}
	if statsObj := m_repo.getRepoStats(LOCAL); statsObj != nil {
		rawStats["local_store"] = statsObj
	}

	return MetastoreStats{
		Type:       MagmaStoreType,
		ItemsCount: itemsCount,
		MemInUse:   m_repo.getBufMemoryUsedNoLock(),
		DiskInUse:  diskInUse,
		Raw:        rawStats,
	}
}

func (m_repo *Magma_Repository) GetItemsCount(kind RepoKind) uint64 {
	m_repo.Lock()
	defer m_repo.Unlock()

	if !m_repo.isClosed {
		var statsObj = m_repo.getRepoStats(kind)

		if statsObj != nil {
			return uint64(statsObj.ItemCount)
		}
	}

	return 0
}

// global function which call the registered EaR keys store callbacks to make keyID available
// this method has to remain global as we cannot export a method on a type to other libraries.
//
//export go_global_get_encryption_key
func go_global_get_encryption_key(keyID MagmaStringBuf, ctx unsafe.Pointer) *MagmaDEK {
	if ctx == nil {
		return C.getUnencryptedDEK()
	}

	m_repo := (*Magma_Repository)(ctx)
	if m_repo == nil {
		return C.getUnencryptedDEK()
	}

	// Empty keyID means caller wants the current/active key
	if keyID.data == nil || keyID.len == 0 {
		// Try callback first if registered
		if m_repo.keyStoreCallbacks != nil {
			key, err := m_repo.keyStoreCallbacks.GetActiveKeyCipher()
			if err == nil && key != nil {
				return m_repo.getOrCreateDEK(key)
			}
		}
		// Fallback to cache
		m_repo.dekCacheMu.RLock()
		dek := m_repo.currentDEK
		m_repo.dekCacheMu.RUnlock()
		if dek != nil {
			return dek
		}
		return C.getUnencryptedDEK()
	}

	// Convert C keyID to Go string
	goKeyID := C.GoStringN(keyID.data, C.int(keyID.len))

	// Try cache first (most common path for lookup by ID)
	m_repo.dekCacheMu.RLock()
	dek, ok := m_repo.dekCache[goKeyID]
	m_repo.dekCacheMu.RUnlock()
	if ok {
		return dek
	}

	// Try callback if registered
	if m_repo.keyStoreCallbacks != nil {
		key, err := m_repo.keyStoreCallbacks.GetKeyCipherByID(goKeyID)
		if err == nil && key != nil {
			return m_repo.getOrCreateDEK(key)
		} else {
			log.Current.Warnf("go_global_get_encryption_key failed to get key %s: %v",
				goKeyID, err)
		}
	}

	// Not in cache, return unencrypted (key not found)
	return C.getUnencryptedDEK()
}

// getOrCreateDEK returns a C-allocated DEK from cache or creates a new one
func (m_repo *Magma_Repository) getOrCreateDEK(key *EarKey) *MagmaDEK {
	if key == nil {
		return C.getUnencryptedDEK()
	}

	// Check cache first
	m_repo.dekCacheMu.RLock()
	dek, ok := m_repo.dekCache[key.Id]
	m_repo.dekCacheMu.RUnlock()
	if ok {
		return dek
	}

	// Create new C-allocated DEK
	m_repo.dekCacheMu.Lock()
	defer m_repo.dekCacheMu.Unlock()

	// Double check after acquiring write lock
	if dek, ok := m_repo.dekCache[key.Id]; ok {
		return dek
	}

	if len(key.Id) == 0 {
		dek = C.getUnencryptedDEK()
	} else {
		// Allocate C memory for the DEK using jemalloc
		dek = (*MagmaDEK)(C.je_malloc(C.size_t(unsafe.Sizeof(MagmaDEK{}))))

		cId := C.CString(key.Id)
		dek.id = C.je_strdup(cId)
		C.free(unsafe.Pointer(cId))

		cCipher := C.CString(key.Cipher)
		dek.cipher = C.je_strdup(cCipher)
		C.free(unsafe.Pointer(cCipher))

		if len(key.Key) > 0 {
			dek.key.data = C.je_memdup((*C.char)(unsafe.Pointer(&key.Key[0])), C.size_t(len(key.Key)))
		} else {
			dek.key.data = nil
		}
		dek.key.len = C.size_t(len(key.Key))
	}

	m_repo.dekCache[key.Id] = dek
	return dek
}

// DropKeys implements [IEaRExtension] drop keys callback to remove data encrypted using keyID
// The expectation is that the data will be re-written with current active encryption key
func (m_repo *Magma_Repository) DropKeys(keyIDs []KeyID) error {
	m_repo.Lock()
	defer m_repo.Unlock()

	if m_repo.isClosed {
		return magmaErrRepoClosed
	}

	if len(keyIDs) == 0 {
		return nil
	}

	// Drop keys from all KV stores
	for _, storeID := range []MagmaStoreID{
		MainMagmaStoreID, CommitLogMagmaStoreID, ServerConfigMagmaStoreID, LocalMagmaStoreID,
	} {
		// Convert keyIDs to C array
		cKeys := make([]*C.char, len(keyIDs))
		for i, kid := range keyIDs {
			if kid == "" {
				cKeys[i] = C.CString("unencrypted")
			} else {
				cKeys[i] = C.CString(kid)
			}
			defer C.free(unsafe.Pointer(cKeys[i]))
		}

		cstatus := C.MKV_DropEncryptionKeys(
			m_repo.mInst,
			storeID,
			&cKeys[0],
			size_t(len(keyIDs)),
		)
		if err := translateMagmaErrToStoreErr(cstatus); err != nil {
			log.Current.Errorf("MagmaRepository::DropKeys:: failed for store %s: %v",
				storeIDString(storeID), err)
			return err
		}
	}

	log.Current.Infof("MagmaRepository::DropKeys:: dropped keys %v", keyIDs)
	return nil
}

// GetInuseKeys implements [IEaRExtension]. Can be used by caller to know which keys is the data
// currently encrypted using
func (m_repo *Magma_Repository) GetInuseKeys() ([]KeyID, error) {
	m_repo.Lock()
	defer m_repo.Unlock()

	if m_repo.isClosed {
		return nil, magmaErrRepoClosed
	}

	var (
		keyIDs  []KeyID
		cKeyIDs *MagmaStringBuf
		numKeys size_t
	) //nolint:golines // false positive

	cstatus := C.MKV_GetActiveEncryptionKeyIDs(
		m_repo.mInst,
		&cKeyIDs,
		&numKeys, //nolint:gocritic // false positive
	)
	if err := translateMagmaErrToStoreErr(cstatus); err != nil {
		log.Current.Errorf("MagmaRepository::GetInuseKeys:: failed: %v", err)
		return nil, err
	}
	unencryptedKeyID := C.GoString(C.getUnencryptedDEK().id)

	// Convert C array to Go slice and free memory allocated by Magma.
	// Per magma_capi.h: "Each SizedBuf must be freed individually by the caller.
	// keyIDs must also be freed by the caller."
	// Individual SizedBufs are freed via MKV_Free; the outer array via MKV_Free
	// with a wrapper SizedBuf since both use the same jemalloc-backed allocator.
	if numKeys > 0 {
		var cKeyIDSlice = unsafe.Slice(cKeyIDs, numKeys)
		for i := size_t(0); i < numKeys; i++ {
			keyID := C.GoStringN(cKeyIDSlice[i].data, C.int(cKeyIDSlice[i].len))
			if keyID == unencryptedKeyID {
				keyID = ""
			}
			keyIDs = append(keyIDs, keyID)
			C.MKV_Free(cKeyIDSlice[i])
		}
		// Free the outer SizedBuf array allocated by cb_malloc (jemalloc).
		// cb_malloc uses je_malloc internally, so we use je_free directly
		// since cb_free is not exported from libmagma_shared.
		C.je_free(unsafe.Pointer(cKeyIDs))
	}

	// Notify callback if registered
	if m_repo.keyStoreCallbacks != nil && len(keyIDs) > 0 {
		for _, kid := range keyIDs {
			err := m_repo.keyStoreCallbacks.SetInuseKeys(kid)
			if err != nil {
				log.Current.Warnf(
					"MagmaRepository::GetInuseKeys unable to report keyIDs %v with err %v",
					keyIDs, err,
				)
			}
		}
	}

	log.Current.Debugf("MagmaRepository::GetInuseKeys:: keys: %v", keyIDs)
	return keyIDs, nil
}

// RefreshKeys implements [IEaRExtension] which refreshs the current set of keys available
// with the system. this can be used to change the current active encryption key
func (m_repo *Magma_Repository) RefreshKeys() error {
	m_repo.Lock()
	defer m_repo.Unlock()

	if m_repo.isClosed {
		return magmaErrRepoClosed
	}

	// Get keys from callback if registered, otherwise use provided keys
	var keyInfo *EncryKeyInfo
	var activeKeyID string

	if m_repo.keyStoreCallbacks != nil {
		info, err := m_repo.keyStoreCallbacks.GetEncryptionKeys()
		if err != nil {
			err = fmt.Errorf("keyStoreCallbacks.GetEncryptionKeys failed: %w", err)
			log.Current.Errorf("MagmaRepository::RefreshKeys: %v", err)
			return err
		}
		if info != nil {
			keyInfo = info
			activeKeyID = info.ActiveKeyId
		}
	}

	if keyInfo == nil {
		return nil
	}

	// Build set of new key IDs
	newKeyIDs := make(map[string]bool)
	for i := range keyInfo.Keys {
		newKeyIDs[keyInfo.Keys[i].Id] = true
	}

	// Find and free keys no longer needed (allocated via jemalloc)
	m_repo.dekCacheMu.Lock()
	for keyID, dek := range m_repo.dekCache {
		if !newKeyIDs[keyID] {
			// Key no longer in use, free C memory (jemalloc)
			C.je_free(unsafe.Pointer(dek.id))
			C.je_free(unsafe.Pointer(dek.cipher))
			if dek.key.data != nil {
				C.je_free(unsafe.Pointer(dek.key.data))
			}
			C.je_free(unsafe.Pointer(dek))
			delete(m_repo.dekCache, keyID)
			log.Current.Debugf("MagmaRepository::RefreshKeys:: freed unused key %s", keyID)
		}
	}
	m_repo.dekCacheMu.Unlock()

	// Add or update keys in the DEK cache
	for i := range keyInfo.Keys {
		k := &keyInfo.Keys[i]
		m_repo.getOrCreateDEK(k)
	}

	// Detect active key change and notify Magma
	if activeKeyID != m_repo.currentKeyID {
		m_repo.dekCacheMu.RLock()
		dek, ok := m_repo.dekCache[activeKeyID]
		m_repo.dekCacheMu.RUnlock()

		if ok && dek != nil {
			cstatus := C.MKV_SetCurrentEncryptionKey(m_repo.mInst, dek)
			if err := translateMagmaErrToStoreErr(cstatus); err != nil {
				log.Current.Errorf(
					"MagmaRepository::RefreshKeys:: failed to set current key %s: %v",
					activeKeyID, err)
				return err
			}

			m_repo.dekCacheMu.Lock()
			m_repo.currentDEK = dek
			m_repo.currentKeyID = activeKeyID
			m_repo.dekCacheMu.Unlock()

			log.Current.Infof("MagmaRepository::RefreshKeys:: set current key to %s", activeKeyID)
		}
	}

	return nil
}

// RegisterEncryptionKeyStoreCallback implements [IEaRExtension]. It registers a set of callbacks
// required by the repository to get system keys
func (m_repo *Magma_Repository) RegisterEncryptionKeyStoreCallback(
	cb IEncryptionKeyStoreCallbacks,
) {

	m_repo.Lock()
	defer m_repo.Unlock()
	m_repo.keyStoreCallbacks = cb
}
