package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/geraud22/mqtt_connector"
)

type TestingHandler struct {
	payloadChannel chan []byte
	errorChannel   chan error
}

func (h *TestingHandler) SendMessageToChannel(payload []byte) {
	h.payloadChannel <- payload
}

func (h *TestingHandler) GetPayloadChannel() <-chan []byte {
	return h.payloadChannel
}

func (h *TestingHandler) GetErrorChannel() chan error {
	return h.errorChannel
}

func (h *TestingHandler) ClosePayloadChannel() {
	close(h.payloadChannel)
}

func (h *TestingHandler) CloseErrorChannel() {
	close(h.errorChannel)
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	handler, err := mqtt_connector.NewDefaultHandler()
	if err != nil {
		log.Fatalf("error initializing: %v", err)
	}

	processFunc := func(payload []byte) error {
		log.Printf("Processing %s", string(payload))
		return nil
	}
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		handler.AsyncPayloadHandler(ctx, handler, 10, processFunc)
	}()

	for i := 0; i < 1000000; i++ {
		handler.GetPayloadChannel() <- []byte(fmt.Sprintf("payload %d", i))
	}
	cancel()
	wg.Wait()
	log.Printf("Time taken: %v", time.Since(start))
}
