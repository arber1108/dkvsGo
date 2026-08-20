package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

/*
type Node struct {
	ID      string
	Address string
}

type Cluster struct {
	PeerNodes map[string]*Node
}
*/

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

func (e *Entry) Size() int {
	return 12 + int(e.KeySize) + int(e.ValueSize)
}

func (e *Entry) Serialize() []byte {
	headerSize := 20
	totalSize := headerSize + int(e.KeySize) + int(e.ValueSize)

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
	sm.mu.Lock()
	defer sm.mu.Unlock()

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
		maxSize:    10 * 1024 * 1024, // 10 MB
		maxEntries: 100_000,
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
		return 0, 0, fmt.Errorf("no active segment")
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

func PUT(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received PUT request")
}

func main() {
	/*
		peersEnv := os.Getenv("PEERS")
		peers := strings.Split(peersEnv, ",")
		cluster := Cluster{make(map[string]*Node)}

		for _, peer := range peers {
			id := strings.Split(peer, ":")[0]
			node := Node{ID: id, Address: peer}
			cluster.PeerNodes[id] = &node
		}
	*/

	http.HandleFunc("/PUT", PUT)
	err := http.ListenAndServe(":9000", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
