//go:build !community
// +build !community

package repository

//#cgo LDFLAGS: -lmagma_shared
//#include <stdlib.h>
//#include <libmagma/magma_capi.h>
import "C"

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
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
)

func defaultMagmaCfg() *MagmaShardConfig {
	numFlushers := uint64_t(1)
	cfg := C.MKV_DefaultConfig()
	cfg.EnableWAL = 1
	cfg.NumFlushers = numFlushers
	cfg.NumCompactors = numFlushers * 4
	cfg.StatsSamplePeriodSecs = 300
	cfg.MemoryQuota = 4 * 1024 * 1024
	cfg.EnableAutoCheckpointing = 1
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
)

// Magma_Repository is the global interface to magma backed store
type Magma_Repository struct {
	sync.Mutex
	mInst         *MagmaShard
	storeRev      map[RepoKind]uint32_t
	storeSeqNum   map[RepoKind]*atomic.Uint64
	snapshots     map[RepoKind][]*magmaSnapContainer
	memAllocMutex sync.Mutex // memAllocMutex is to serialize access to memAllocator. this is kept as a separate lock so we can remove overall lock in future
	memAllocator  *MagmaMemAllocator
	isClosed      bool
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
	repo, err := openMagmaRepository(
		magmaPath,
		params.MemoryQuota, params.CompactionMinFileSize,
		params.CompactionThresholdPercent,
		params.EnableWAL,
	)

	// CORRUPTION HANDLE TODO: handle StatusNotFound, StatusCorruption for magma store file
	// corruption

	// MIGRATION TODO: implement migration from forestDb to magma
	// MIGRATION TODO: add verification for migration

	return repo, err
}

func openMagmaRepository(
	path string,
	memoryQuota, minTreeSize uint64,
	compactionThreshold uint8,
	enableWAL bool,
) (IRepository, error) {
	cfg := defaultMagmaCfg()

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

	m_repo := &Magma_Repository{
		mInst:        mInst,
		storeRev:     make(map[RepoKind]uint32_t, 4),
		snapshots:    make(map[RepoKind][]*magmaSnapContainer, 4),
		storeSeqNum:  make(map[RepoKind]*atomic.Uint64, 4),
		memAllocator: C.MKV_CreateWorkContext(numRecsForMemAlloc /*numRecs C.size_t*/),
	}

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

	storeID := repoKindToMagmaStoreID(kind)

	if err := m_repo.setNoCommitNoLock(kind, key, content); err != nil {
		return err
	}

	cstatus := C.MKV_SyncKVStore(
		m_repo.mInst, // *C.MagmaKVStore
		storeID,      // C.uint16_t
	)
	err := translateMagmaErrToStoreErr(cstatus)
	if err != nil {
		log.Current.Errorf("MagmaRepository::Set:: sync error {key: %s, len(val): %d, store: %s, err: %v}",
			key, len(content), storeIDString(storeID), err)
	}
	return err
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

	storeID := repoKindToMagmaStoreID(kind)

	if err := m_repo.deleteNoCommitNoLock(kind, key); err != nil {
		return err
	}

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

func (m_repo *Magma_Repository) GetItemsCount(kind RepoKind) uint64 {
	m_repo.Lock()
	defer m_repo.Unlock()

	if m_repo.isClosed {
		return 0
	}

	storeID := repoKindToMagmaStoreID(kind)

	statObj := &MagmaStoreStats{}

	C.MKV_GetKVStoreStats(
		m_repo.mInst, // *C.MagmaKVStore
		statObj,      // *C.MagmaPerKVStoreStats
		storeID,      // C.uint16_t
		1,            // range C.int (aggregate KVStats over range of kv stores)
	)

	return uint64(statObj.TotalItemCount)
}

func (m_repo *Magma_Repository) GetBufMemoryUsed() uint64 {
	m_repo.Lock()
	defer m_repo.Unlock()

	if m_repo.isClosed {
		return 0
	}

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
