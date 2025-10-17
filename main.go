package main

import (
	"fmt"
	"net"
	"time"
)

const (
	// API Key for Create Kafka Topics
	apiKeyCreateTopics = int16(19)

	// API Version for Create Kafka Topics
	apiVersionCreateTopics = int16(4)

	// Create Topic related error codes
	errorCodeNone               = 0
	errorCodeTopicAlreadyExists = 36
	errorCodeInvalidPartitions  = 37
	errorCodeInvalidTopicName   = 60
)

type TopicStore interface {
	CreateTopic(name string, partitions int32, replicationFactor int16) (int16, string)
	Close() error
}

type InMemoryStore struct {
	topics map[string]*Topic
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		topics: make(map[string]*Topic),
	}
}

func (s *InMemoryStore) CreateTopic(name string, partitions int32, replicationFactor int16) (int16, string) {
	if name == "" {
		return errorCodeInvalidTopicName, "Topic Name cannot be empty"
	}

	if partitions < 1 {
		return errorCodeInvalidPartitions, "Number of partitions must be at least 1"
	}

	if _, exists := s.topics[name]; exists {
		return errorCodeTopicAlreadyExists, "Topic already exists"
	}

	// Create & Store New Topic
	s.topics[name] = &Topic{
		Name:              name,
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
		CreatedAt:         time.Now(),
	}
	return errorCodeNone, "Topic created successfully"

}

func (s *InMemoryStore) Close() error {
	return nil
}

type Topic struct {
	Name              string
	NumPartitions     int32
	ReplicationFactor int16
	CreatedAt         time.Time
}

type Server struct {
	address string
	store   TopicStore
}

func NewServer(address string, store TopicStore) *Server {
	return &Server{
		address: address,
		store:   store,
	}
}

func (s *Server) Start() error {
	// Create Listner for Server
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.address, err)
	}
	defer listener.Close()

	fmt.Printf("Kafka Server listening on %s", s.address)

	// Accept Connections from Clients and Process Requests
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Failed to accept connection: %v", err)
		}
		// Handle Connection in separate Goroutine
		go s.handleConnection(conn)
	}
}

// Kafka Wire Protocol Connection Handler
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	// Log Client Address which is Initiating the Connection
	fmt.Printf("\nConnection initiatied from %s", conn.RemoteAddr().String())

	// TODO implement Kafka Protocol handling
}

func main() {
	store := NewInMemoryStore()
	server := NewServer(":9092", store)
	server.Start()
}
