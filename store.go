package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const headerSize = 16
const maxFileSize int64 = 10 * 1024 * 1024 // 10 MB
const maxSegmentEntries int = 100_000

type Entry struct {
	TimeStamp uint32
	KeySize   uint32
	ValueSize uint32
	Key       []byte
	Value     []byte
}

func (e *Entry) TombstoneEntry() *Entry {
	return &Entry{
		TimeStamp: uint32(time.Now().Unix()),
		KeySize:   e.KeySize,
		ValueSize: 0,
		Key:       e.Key,
		Value:     nil,
	}
}

func (e *Entry) verifyChecksum(checksum uint32) bool {
	totalSize := e.Size()

	buf := make([]byte, totalSize)

	binary.LittleEndian.PutUint32(buf[4:8], e.TimeStamp)
	binary.LittleEndian.PutUint32(buf[8:12], e.KeySize)
	binary.LittleEndian.PutUint32(buf[12:16], e.ValueSize)

	copy(buf[16:16+e.KeySize], e.Key)
	copy(buf[16+e.KeySize:], e.Value)

	return checksum == crc32.ChecksumIEEE(buf[4:])
}

func (e *Entry) Size() int {
	return 16 + int(e.KeySize) + int(e.ValueSize)
}

func (e *Entry) Serialize() []byte {
	totalSize := e.Size()

	buf := make([]byte, totalSize)

	binary.LittleEndian.PutUint32(buf[4:8], e.TimeStamp)
	binary.LittleEndian.PutUint32(buf[8:12], e.KeySize)
	binary.LittleEndian.PutUint32(buf[12:16], e.ValueSize)

	copy(buf[16:16+e.KeySize], e.Key)
	copy(buf[16+e.KeySize:], e.Value)

	checksum := crc32.ChecksumIEEE(buf[4:])

	binary.LittleEndian.PutUint32(buf[0:4], checksum)

	return buf
}

type Segment struct {
	id         int
	mu         sync.Mutex
	path       string
	file       *os.File
	size       int64
	entryCount int
	maxSize    int64
	maxEntries int
	isActive   bool
	isClosed   bool
}

var ErrSegmentClosed = errors.New("this Segment is Closed or not Active")

var ErrSegmentFull = errors.New("this Segment is Full")

func (s *Segment) Append(entry *Entry) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isClosed || !s.isActive {
		return 0, ErrSegmentClosed
	}

	if s.size >= s.maxSize || s.entryCount >= s.maxEntries {
		s.isActive = false
		return 0, ErrSegmentFull
	}

	data := entry.Serialize()
	offset := s.size

	_, err := s.file.Write(data)
	if err != nil {
		return 0, fmt.Errorf("failed to write entry: %w", err)
	}

	s.size += int64(len(data))
	s.entryCount++

	return offset, nil
}

type SegmentManager struct {
	mu       sync.Mutex
	basePath string
	segments map[int]*Segment
	activeID int
	nextID   int
}

func (sm *SegmentManager) createActiveSegment() error {

	fullPath := filepath.Join(sm.basePath, fmt.Sprintf("%04d.log", sm.nextID))

	file, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("failed creating segment: %w", err)
	}

	segment := &Segment{
		id:         sm.nextID,
		path:       fullPath,
		file:       file,
		size:       0,
		entryCount: 0,
		maxSize:    maxFileSize,
		maxEntries: maxSegmentEntries,
		isActive:   true,
		isClosed:   false,
	}
	sm.segments[sm.nextID] = segment
	sm.activeID = sm.nextID
	sm.nextID++

	return nil
}

func (sm *SegmentManager) Append(entry *Entry) (int, int64, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.activeID == 0 {
		err := sm.createActiveSegment()
		if err != nil {
			return 0, 0, err
		}
	}

	segment, exists := sm.segments[sm.activeID]

	if !exists {
		return 0, 0, fmt.Errorf("active segment %d not found", sm.activeID)
	}

	offset, err := segment.Append(entry)
	if err != nil {
		if errors.Is(err, ErrSegmentFull) {
			if err := sm.createActiveSegment(); err != nil {
				return 0, 0, nil
			}
			segment = sm.segments[sm.activeID]
			offset, err = segment.Append(entry)
			if err != nil {
				return 0, 0, err
			}
		} else {
			return 0, 0, err
		}
	}
	return segment.id, offset, nil
}

func (sm *SegmentManager) Read(segmentID int, valuePos int64) (*Entry, error) {
	path := sm.segments[segmentID].path

	logfile, err := os.Open(path)
	defer logfile.Close()

	if err != nil {
		return nil, fmt.Errorf("failed opening file at: %s", path)
	}

	_, err = logfile.Seek(valuePos, 0)
	if err != nil {
		return nil, err
	}

	headerBuffer := make([]byte, 16)

	_, err = logfile.Read(headerBuffer)
	if err != nil {
		return nil, fmt.Errorf("failed reading header: %w", err)
	}

	// read Headers
	checksum := binary.LittleEndian.Uint32(headerBuffer[0:4])
	timeStamp := binary.LittleEndian.Uint32(headerBuffer[4:8])
	keySize := binary.LittleEndian.Uint32(headerBuffer[8:12])
	valueSize := binary.LittleEndian.Uint32(headerBuffer[12:16])

	// read Key
	keyBuffer := make([]byte, keySize)
	logfile.Read(keyBuffer)

	// read Value
	valueBuffer := make([]byte, valueSize)
	_, err = logfile.Read(valueBuffer)

	if err != nil {
		return nil, fmt.Errorf("failed reading Value from logfile: %w", err)
	}

	entry := &Entry{
		TimeStamp: timeStamp,
		KeySize:   keySize,
		ValueSize: valueSize,
		Key:       keyBuffer,
		Value:     valueBuffer,
	}

	if !entry.verifyChecksum(checksum) {
		return nil, fmt.Errorf("corrupted Entry. Checksums don't match")
	}

	return entry, nil
}

type HashTableEntry struct {
	FieldID   int
	ValueSize uint32
	ValuePos  int64
	Timestamp uint32
}

type HashTable struct {
	mu    sync.Mutex
	index map[string]*HashTableEntry
}

func (ht *HashTable) Put(key string, segmentID int, offset int64, valueSize, timeStamp uint32) {
	hashTableEntry := &HashTableEntry{
		FieldID:   segmentID,
		ValueSize: valueSize,
		ValuePos:  offset,
		Timestamp: timeStamp,
	}
	ht.index[key] = hashTableEntry
}

func (ht *HashTable) Delete(key string) {
	delete(ht.index, key)
}

type Store struct {
	mu             sync.RWMutex
	hashTable      *HashTable
	segmentManager *SegmentManager
}

func (s *Store) SET(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.segmentManager == nil {
		return fmt.Errorf("store not properly initialized")
	}

	timeStamp := uint32(time.Now().Unix())
	valueSize := uint32(len(value))
	keySize := uint32(len(key))

	entry := &Entry{
		TimeStamp: timeStamp,
		KeySize:   keySize,
		ValueSize: valueSize,
		Key:       []byte(key),
		Value:     []byte(value),
	}

	segmentID, offset, err := s.segmentManager.Append(entry)

	if err != nil {
		return fmt.Errorf("failed appending entry to store: %w", err)
	}

	s.hashTable.Put(key, segmentID, offset, valueSize, timeStamp)

	return nil
}

func (s *Store) GET(key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	hashTableEntry, found := s.hashTable.index[key]
	if !found {
		return "", fmt.Errorf("no entry found with key: %s", key)
	}

	logEntry, err := s.segmentManager.Read(hashTableEntry.FieldID, hashTableEntry.ValuePos)
	if err != nil {
		return "", fmt.Errorf("failed reading entry: %w", err)
	}

	return string(logEntry.Value), nil
}

func (s *Store) DELETE(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.hashTable.index[key]
	if !exists {
		return fmt.Errorf("couldn't find entry with key: %s", key)
	}

	entry := Entry{}
	tombstoneEntry := entry.TombstoneEntry()

	_, _, err := s.segmentManager.Append(tombstoneEntry)

	if err != nil {
		return fmt.Errorf("failed appending tombstone entry: %w", err)
	}

	s.hashTable.Delete(key)
	return nil
}
