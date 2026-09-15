package main

import (
	"fmt"
)

func main() {
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
