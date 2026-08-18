package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
)

type Node struct {
	ID      string
	Address string
}

type Cluster struct {
	PeerNodes map[string]*Node
}

func PUT(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received PUT request")
}

func main() {
	peersEnv := os.Getenv("PEERS")
	peers := strings.Split(peersEnv, ",")
	cluster := Cluster{make(map[string]*Node)}

	for _, peer := range peers {
		id := strings.Split(peer, ":")[0]
		node := Node{ID: id, Address: peer}
		cluster.PeerNodes[id] = &node
	}
	http.HandleFunc("/PUT", PUT)
	err := http.ListenAndServe(":9000", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
