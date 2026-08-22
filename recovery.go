package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

/*
func (s *Store) removeDeletedEntries() error {
	segments := s.segmentManager.segments


	offset := 0
	entryCount := 0
	for offset < int(fileInfo.Size()) {
		entry, readErr := segmentManager.Read(segmentID, int64(offset))

		if readErr != nil {
			return nil, fmt.Errorf("failed reading entry while trying to recover: \n SegmentID: %d, %w", segmentID, readErr)
		}

		if entry.Value == nil {
			tombstoneEntries = append(tombstoneEntries, entry)
			continue
		}

		hashTable.Put(string(entry.Key), segmentID, int64(offset), entry.ValueSize, entry.TimeStamp)
		offset += entry.Size()
		entryCount++
	}

}

*/

func (s *Store) recoverStore() ([]*Entry, error) {
	segmentManager := &SegmentManager{
		mu:       sync.Mutex{},
		basePath: `C:\Users\arber\Coding\dkvsGo\logfiles`,
		segments: make(map[int]*Segment),
	}

	hashTable := &HashTable{
		mu:    sync.Mutex{},
		index: make(map[string]*HashTableEntry),
	}

	logFiles, err := os.ReadDir(segmentManager.basePath)
	if err != nil {
		return nil, fmt.Errorf("failed reading directory: %s, %w", segmentManager.basePath, err)
	}

	segmentManager.activeID = len(logFiles)
	segmentManager.nextID = segmentManager.activeID + 1

	var tombstoneEntries []*Entry

	for _, logFile := range logFiles {
		fileName := logFile.Name()[0:4]
		segmentID, nameErr := strconv.Atoi(fileName)
		if nameErr != nil {
			return nil, fmt.Errorf("failed parsing logfile name to int: %w", nameErr)
		}

		pathToLogfile := filepath.Join(segmentManager.basePath, logFile.Name())

		flags := os.O_RDONLY
		if segmentID == segmentManager.activeID {
			flags = os.O_RDWR | os.O_APPEND
		}

		file, fileOpenErr := os.OpenFile(pathToLogfile, flags, 0)
		if fileOpenErr != nil {
			return nil, fmt.Errorf("failed opening logfile %s: %w", pathToLogfile, fileOpenErr)
		}

		fileInfo, statErr := file.Stat()
		if statErr != nil {
			file.Close()
			return nil, fmt.Errorf("failed reading logfile metadata %s: %w", pathToLogfile, statErr)
		}
		segment := &Segment{
			id:         segmentID,
			mu:         sync.Mutex{},
			path:       pathToLogfile,
			file:       file,
			size:       fileInfo.Size(),
			maxSize:    maxFileSize,
			maxEntries: maxSegmentEntries,
			isActive:   false,
			isClosed:   false,
		}
		segmentManager.segments[segmentID] = segment

		offset := 0
		entryCount := 0
		for offset < int(fileInfo.Size()) {
			entry, readErr := segmentManager.Read(segmentID, int64(offset))

			if readErr != nil {
				return nil, fmt.Errorf("failed reading entry while trying to recover: \n SegmentID: %d, %w", segmentID, readErr)
			}

			if entry.Value == nil {
				tombstoneEntries = append(tombstoneEntries, entry)
				continue
			}

			hashTable.Put(string(entry.Key), segmentID, int64(offset), entry.ValueSize, entry.TimeStamp)
			offset += entry.Size()
			entryCount++
		}

		segment.entryCount = entryCount
		if segment.id == segmentManager.activeID {
			segment.isActive = true
		}
	}

	s.mu = sync.RWMutex{}
	s.hashTable = hashTable
	s.segmentManager = segmentManager

	return tombstoneEntries, nil
}
