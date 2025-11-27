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
	"reflect"
	"sync"
	"sync/atomic"
	"unsafe"

	c "github.com/couchbase/gometa/common"
	"github.com/couchbase/gometa/log"
)

var l = log.Current

func newMagmaError(status MagmaOpStatus) error {
	if status.Code != 0 {
		return &StoreError{
			sType: MagmaStoreType,
			errMsg: fmt.Sprintf("(%d)%v",
				status.Code,
				C.GoStringN(
					status.ErrMsg.data,       // string *C.Char
					C.int(status.ErrMsg.len), // len C.int
				),
			),
		}
	}
	return nil
}

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

const (
	MagmaOpInsert MagmaOp = iota
	MagmaOpUpsert
	MagmaOpDelete
	MagmaOpLocalUpdate
	MagmaOpLocalDelete
	MagmaOpDiscard
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

func OpenMagmaRepository(path string) (IRepository, error) {
	cfg := defaultMagmaCfg()

	c_path := C.CString(path)
	defer C.free(unsafe.Pointer(c_path))
	mInst := C.NewMagmaKVStore(
		c_path, // path *C.char
		cfg,    // config *C.MagmaKVStoreConfig
	)
	cstatus := C.MKV_Open(mInst /*inst *C.MagmaKVStore*/)
	if err := newMagmaError(cstatus); err != nil {
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
			C.MKV_GetKVStoreRevision(
				mInst,   // *C.MagmaKVStore
				storeId, // C.uint16_t
				&rev,    // *C.uint32_t
			)
			m_repo.storeRev[repoKind] = rev

			var seqNum uint64_t
			C.MKV_GetMaxSeqNo(
				mInst,   // *C.MagmaKVStore
				storeId, // C.uint16_t
				&seqNum, // *C.uint64_t
			)
			m_repo.storeSeqNum[repoKind].Store(uint64(seqNum))

			l.Infof("OpenMagmaRepository: found store %v with rev %v highSeqNo %v",
				storeIDString(storeId), rev, seqNum)
		} else {
			m_repo.storeRev[repoKind] = 1
			cstatus = C.MKV_CreateKVStore(
				mInst,                     // *C.MagmaKVStore
				storeId,                   // C.uint16_t
				m_repo.storeRev[repoKind], // C.uint32_t
			)
			if err := newMagmaError(cstatus); err != nil {
				return m_repo, err
			}
			l.Infof("OpenMagmaRepository: created kv store %v using rev 1",
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

	// Magma write API is designed with batch inserts in mind. we always insert a single key,val
	cstatus := C.MKV_Write(
		m_repo.mInst,          // *C.MagmaKVStore
		storeID,               // C.uint16_t
		m_repo.storeRev[kind], // C.uint32_t
		rec,                   // recs (*C.CRecord) -> []{Op, C.SizedBuf, C.SizedBuf, C.SizedBuf}
		1,                     // numRecs C.size_t
	)
	return newMagmaError(cstatus)
}

func (m_repo *Magma_Repository) SetNoCommit(kind RepoKind, key string, content []byte) error {
	m_repo.Lock()
	defer m_repo.Unlock()

	if m_repo.isClosed {
		return ErrRepoClosed
	}

	return m_repo.setNoCommitNoLock(kind, key, content)
}

func (m_repo *Magma_Repository) Set(kind RepoKind, key string, content []byte) error {
	m_repo.Lock()
	defer m_repo.Unlock()

	if m_repo.isClosed {
		return ErrRepoClosed
	}

	storeID := repoKindToMagmaStoreID(kind)

	if err := m_repo.setNoCommitNoLock(kind, key, content); err != nil {
		return err
	}

	cstatus := C.MKV_SyncKVStore(
		m_repo.mInst, // *C.MagmaKVStore
		storeID,      // C.uint16_t
	)
	return newMagmaError(cstatus)
}

func (m_repo *Magma_Repository) Get(kind RepoKind, key string) ([]byte, error) {
	m_repo.Lock()
	defer m_repo.Unlock()

	if m_repo.isClosed {
		return nil, ErrRepoClosed
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
		return nil, newMagmaError(cstatus)
	}

	res := C.GoBytes(unsafe.Pointer(val.data), C.int(val.len))

	return res, nil
}

func (m_repo *Magma_Repository) deleteNoCommitNoLock(kind RepoKind, key string) error {

	// verify that key exists. if not, don't attempt delete as it can hang magma compaction
	if res, err := m_repo.getNoLock(kind, key); err != nil || len(res) == 0 {
		// TODO: understand if we should return error or nil if key does not exist
		// key does not exist
		return fmt.Errorf("key %v does not exist", key)
	}

	storeID := repoKindToMagmaStoreID(kind)

	// write with increasing seqNum as it is a requirement for magma stores
	seqNum := m_repo.storeSeqNum[kind].Add(1)

	meta := binary.LittleEndian.AppendUint64(nil, seqNum)
	rec := newWriteMagmaRecord(MagmaOpDelete, key, nil, meta)

	// to delete records from Magma, we have to use the Write API itself but the OpCode is
	// MagmaOpDelete with increasing seqNum
	cstatus := C.MKV_Write(
		m_repo.mInst,          // *C.MagmaKVStore
		storeID,               // C.uint16_t
		m_repo.storeRev[kind], // C.uint32_t
		rec,                   // recs (*C.CRecord) -> []{Op, C.SizedBuf, C.SizedBuf, C.SizedBuf}
		1,                     // numRecs C.size_t
	)

	return newMagmaError(cstatus)
}

func (m_repo *Magma_Repository) DeleteNoCommit(kind RepoKind, key string) error {
	m_repo.Lock()
	defer m_repo.Unlock()

	if m_repo.isClosed {
		return ErrRepoClosed
	}

	return m_repo.deleteNoCommitNoLock(kind, key)
}

func (m_repo *Magma_Repository) Delete(kind RepoKind, key string) error {
	m_repo.Lock()
	defer m_repo.Unlock()

	if m_repo.isClosed {
		return ErrRepoClosed
	}

	storeID := repoKindToMagmaStoreID(kind)

	if err := m_repo.deleteNoCommitNoLock(kind, key); err != nil {
		return err
	}

	cstatus := C.MKV_SyncKVStore(
		m_repo.mInst, // *C.MagmaKVStore
		storeID,      // C.uint16_t
	)
	return newMagmaError(cstatus)
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
		return ErrRepoClosed
	}

	cstatus := C.MKV_Sync(m_repo.mInst /* *C.MagmaKVStore */)
	return newMagmaError(cstatus)
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

		err := newMagmaError(C.MKVItr_Seek(
			iter, // *C.MagmaKeyIterator
			buf,  // startKey *C.char
		))
		if err != nil {
			l.Errorf("NewMagmaKIterWrapper: couldn't perform seek to key %v. iterator error - %v",
				err)
			return nil
		}
	} else {
		// perform Seek to move cursor of iterator to the start. MagmaIterators will not return
		// data without this
		status := newMagmaError(C.MKVItr_SeekFirst(iter /**C.MagmaKeyIterator*/))
		if status != nil {
			l.Errorf("NewMagmaKIterWrapper couldn't perform first seek on iterator for kind %v, iterator error - %v", kind, status)
		}
	}
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
		return "", nil, ErrIterFail
	}

	wrapper.Lock()
	defer wrapper.Unlock()

	if wrapper.isClosed || wrapper.iter == nil {
		return "", nil, ErrIterFail
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
			l.Errorf(
				"MagmaKIterWrapper::Next: iter encountered an err - %v, kind: %",
				err,
				wrapper.kind,
			)
			wrapper.closeNoLock()
		}
		return "", nil, ErrIterFail
	}

	// always copy key and val
	goKey := C.GoStringN(key.data, C.int(key.len))
	goVal := C.GoBytes(unsafe.Pointer(val.data), C.int(val.len))

	// sorted iterator returns key larger than endKey, close iterator and return
	if len(wrapper.endKey) > 0 && goKey > wrapper.endKey {
		// safety call. if any wrapper goes unclosed, we have open snapshots which prevents magma
		// close
		wrapper.closeNoLock()
		return "", nil, ErrIterFail
	}

	var retErr error
	C.MKVItr_Next(wrapper.iter /* *C.MagmaKeyIterator */)
	isValid := C.MKVItr_Valid(wrapper.iter /* *C.MagmaKeyIterator */) == 1
	if !isValid {
		// safety call. if any wrapper goes unclosed, we have open snapshots which prevents magma
		// close
		wrapper.closeNoLock()
		retErr = ErrIterFail
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
}

func (m_repo *Magma_Repository) CreateSnapshot(kind RepoKind, txnid c.Txnid) error {
	m_repo.Lock()
	defer m_repo.Unlock()
	if m_repo.isClosed {
		return ErrRepoClosed
	}

	storeID := repoKindToMagmaStoreID(kind)

	C.MKV_SyncKVStore(m_repo.mInst, storeID)

	snap := C.MKV_GetSnapshot(
		m_repo.mInst, /* *C.MagmaKVStore */
		storeID,      /* C.uint16_t */
		1,            /* C.int_t - use a disk snapshot on 1 */
	)
	if snap == nil {
		return StoreError{sType: MagmaStoreType,
			errMsg: fmt.Sprintf("failed to create snapshot for store %v",
				storeIDString(storeID))}
	}

	container := &magmaSnapContainer{
		snap:     snap,
		txnID:    txnid,
		refCount: 0,
	}

	// pruneSnapshots first then append
	m_repo.pruneSnapshotsNoLock(kind)

	m_repo.snapshots[kind] = append(m_repo.snapshots[kind], container)

	return nil
}

func (m_repo *Magma_Repository) AcquireSnapshot(kind RepoKind) (c.Txnid, IRepoIterator, error) {
	m_repo.Lock()
	defer m_repo.Unlock()

	if m_repo.isClosed {
		return 0, nil, ErrRepoClosed
	}

	if len(m_repo.snapshots[kind]) == 0 {
		return 0, nil, nil
	}

	snapContainer := m_repo.snapshots[kind][len(m_repo.snapshots[kind])-1]
	snapContainer.refCount++

	iter := C.MKV_NewKeyIterator(
		m_repo.mInst,       /* *C.MagmaKVStore */
		snapContainer.snap, /* *C.MagmaSnapHandle */
	)
	if iter == nil {
		return 0, nil, nil
	}

	// do not pass snapC here. refer to NewIterator for more info about snapC. we do not pass
	// snapC there because we don't want to close the underlying magma snapshot even if
	// the iterator is closed. we could still create more iterators from the same snapshot
	iterWrapper := NewMagmaKIterWrapper(iter, "", "", kind, nil)
	snapContainer.iterators = append(snapContainer.iterators, iterWrapper)

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
		return nil, ErrRepoClosed
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
		return nil, StoreError{sType: MagmaStoreType,
			errMsg: fmt.Sprintf("failed to create snapshot for store %v",
				storeIDString(storeID))}
	}

	iter := C.MKV_NewKeyIterator(m_repo.mInst, snap)
	if iter == nil {
		return nil, nil
	}

	// keep a track of the magma snapshot. when we close iterators which are directly created
	// from NewIterator API, we will have to release the magma snapshot back on iterator close
	return NewMagmaKIterWrapper(iter, startKey, endKey, kind, snap), nil
}

func (m_repo *Magma_Repository) pruneSnapshotsNoLock(kind RepoKind) {
	newList := make([]*magmaSnapContainer, 0, len(m_repo.snapshots[kind]))
	for _, snapContainer := range m_repo.snapshots[kind] {
		if snapContainer.refCount <= 0 {
			snapContainer.close()
		} else {
			newList = append(newList, snapContainer)
		}
	}
	m_repo.snapshots[kind] = newList
}
