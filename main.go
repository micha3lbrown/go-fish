package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
)

func main() {
	consumer := initKafka()
	consumeKafka(consumer)
}

func initKafka() *kafka.Reader {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "test",
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	return r
}

func consumeKafka(r *kafka.Reader) {
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}

	r.Close()
}
