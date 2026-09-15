package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

func (s *Store) createCompactedHashMap() (map[string]*Entry, error) {
	entryMap := make(map[string]*Entry)

	lastSegmentID := s.segmentManager.activeID - 1

	for idx := 1; idx <= lastSegmentID; idx++ {
		segment, exists := s.segmentManager.segments[idx]
		if !exists {
			continue
		}

		fileStats, statErr := segment.file.Stat()
		if statErr != nil {
			return nil, fmt.Errorf("failed reading logfile metadata while compacting: \n SegmentID: %d, %w", idx, statErr)
		}

		offset := 0
		for offset < int(fileStats.Size()) {
			entry, readErr := s.segmentManager.Read(idx, int64(offset))
			if readErr != nil {
				return nil, fmt.Errorf("failed reading entry while trying to compact: \n SegmentID: %d, %w", idx, readErr)
			}

			if entry.Value == nil {
				delete(entryMap, string(entry.Key))
			} else {
				entryMap[string(entry.Key)] = entry
			}

			offset += entry.Size()
		}
	}

	return entryMap, nil
}

func (s *Store) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	lastSegmentID := s.segmentManager.activeID - 1
	if lastSegmentID < 1 {
		return nil
	}

	entryMap, err := s.createCompactedHashMap()
	if err != nil {
		return fmt.Errorf("failed building compacted hashmap: %w", err)
	}

	liveEntries := make(map[string]*Entry)
	for key, entry := range entryMap {
		current, exists := s.hashTable.index[key]
		if !exists || current.SegmentID == s.segmentManager.activeID {
			continue
		}
		liveEntries[key] = entry
	}

	compactedSegmentID := 1
	compactedPath := filepath.Join(s.segmentManager.basePath, fmt.Sprintf("%04d.log", compactedSegmentID))

	var newSegment *Segment
	newHashEntries := make(map[string]*HashTableEntry)

	if len(liveEntries) > 0 {
		tempPath := compactedPath + ".compact.tmp"
		tempFile, createErr := os.Create(tempPath)
		if createErr != nil {
			return fmt.Errorf("failed creating compacted logfile: %w", createErr)
		}

		var size int64
		for key, entry := range liveEntries {
			data := entry.Serialize()
			if _, writeErr := tempFile.Write(data); writeErr != nil {
				tempFile.Close()
				os.Remove(tempPath)
				return fmt.Errorf("failed writing compacted entry: %w", writeErr)
			}

			newHashEntries[key] = &HashTableEntry{
				SegmentID: compactedSegmentID,
				ValueSize: entry.ValueSize,
				ValuePos:  size,
				Timestamp: entry.TimeStamp,
			}
			size += int64(len(data))
		}

		if closeErr := tempFile.Close(); closeErr != nil {
			os.Remove(tempPath)
			return fmt.Errorf("failed closing compacted logfile: %w", closeErr)
		}

		newSegment = &Segment{
			id:         compactedSegmentID,
			mu:         sync.Mutex{},
			path:       compactedPath,
			size:       size,
			entryCount: len(liveEntries),
			maxSize:    maxFileSize,
			maxEntries: maxSegmentEntries,
			isActive:   false,
			isClosed:   false,
		}
		defer func() { os.Remove(tempPath) }()

		for idx := 1; idx <= lastSegmentID; idx++ {
			segment, exists := s.segmentManager.segments[idx]
			if !exists {
				continue
			}
			if closeErr := segment.file.Close(); closeErr != nil {
				return fmt.Errorf("failed closing old segment %d during compaction: %w", idx, closeErr)
			}
			if removeErr := os.Remove(segment.path); removeErr != nil {
				return fmt.Errorf("failed removing old segment %d during compaction: %w", idx, removeErr)
			}
			delete(s.segmentManager.segments, idx)
		}

		if renameErr := os.Rename(tempPath, compactedPath); renameErr != nil {
			return fmt.Errorf("failed renaming compacted logfile: %w", renameErr)
		}

		compactedFile, openErr := os.OpenFile(compactedPath, os.O_RDONLY, 0)
		if openErr != nil {
			return fmt.Errorf("failed opening compacted logfile: %w", openErr)
		}
		newSegment.file = compactedFile
	} else {
		for idx := 1; idx <= lastSegmentID; idx++ {
			segment, exists := s.segmentManager.segments[idx]
			if !exists {
				continue
			}
			if closeErr := segment.file.Close(); closeErr != nil {
				return fmt.Errorf("failed closing old segment %d during compaction: %w", idx, closeErr)
			}
			if removeErr := os.Remove(segment.path); removeErr != nil {
				return fmt.Errorf("failed removing old segment %d during compaction: %w", idx, removeErr)
			}
			delete(s.segmentManager.segments, idx)
		}
	}

	if newSegment != nil {
		s.segmentManager.segments[compactedSegmentID] = newSegment
		for key, hashEntry := range newHashEntries {
			s.hashTable.index[key] = hashEntry
		}
	}

	return nil
}

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
