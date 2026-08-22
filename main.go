package main

import (
	"fmt"
)

func main() {
	/*
		segmentManager := &SegmentManager{
			mu:       sync.Mutex{},
			basePath: `C:\Users\arber\Coding\dkvsGo\logfiles`,
			segments: make(map[int]*Segment),
			activeID: 0,
			nextID:   1,
		}
		err := segmentManager.createActiveSegment()
		if err != nil {
			fmt.Printf("Failed creating first segment: %w", err)
			return
		}

		hashTable := &HashTable{
			mu:    sync.Mutex{},
			index: make(map[string]*HashTableEntry),
		}

		store := Store{
			mu:             sync.RWMutex{},
			hashTable:      hashTable,
			segmentManager: segmentManager,
		}


		err = store.SET("Test", "Arber GOAT")
		if err != nil {
			fmt.Printf("Failed setting testValue: %v", err)
		}

		value, err := store.GET("Test")
		if err != nil {
			fmt.Printf("Failed getting testValue: %v", err)
			return
		}
		fmt.Printf("FOUND VALUE: %s \n", value)

		err = store.SET("Test", "NEW VALUE")
		if err != nil {
			fmt.Printf("Failed setting new value: %v", err)
		}

	*/
	store := Store{}
	_, err := store.recoverStore()

	if err != nil {
		fmt.Printf("Failed recovering store: %v", err)
		return
	}

	err = store.SET("Test", "BLABALLAB")
	if err != nil {
		fmt.Printf("Failed setting value: %v", err)
		return
	}

	value, err := store.GET("Test")
	if err != nil {
		fmt.Printf("Failed getting testValue: %v", err)
		return
	}
	fmt.Printf("FOUND VALUE: %s \n", value)
}
