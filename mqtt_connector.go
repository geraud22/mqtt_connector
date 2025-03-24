package mqtt_connector

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	cfy "github.com/geraud22/config-from-yaml"
)

var once sync.Once

type SubscriptionHandler interface {
	SendMessageToChannel(payload []byte)
	GetPayloadChannel() <-chan []byte
	GetErrorChannel() chan error
	Close() error
	Subscribe(topic string) error
	AsyncPayloadProcess(ctx context.Context, numWorkers int, processFunc func([]byte) error)
	PayloadProcess(processFunc func([]byte) error) error
	GetClient() (mqtt.Client, error)
}

func GetDefaultOpts() *mqtt.ClientOptions {
	config := cfy.Get("config")
	broker := config.GetString("MQTT.Broker")
	port := config.GetInt("MQTT.Port")
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port))
	clientID := config.GetString("MQTT.ClientID")
	username := config.GetString("MQTT.Username")
	password := config.GetString("MQTT.Password")
	opts.SetClientID(clientID)
	opts.SetUsername(username)
	opts.SetPassword(password)
	opts.SetKeepAlive(60 * time.Second)
	return opts
}

func ConnectMqtt(opts *mqtt.ClientOptions) (mqtt.Client, error) {
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("Error connecting to MQTT: %v", token.Error())
	}
	return client, nil
}

type DefaultHandler struct {
	client         mqtt.Client
	payloadChannel chan []byte
	errorChannel   chan error
	subbedTopics   map[string]string
}

func NewDefaultHandler() (*DefaultHandler, error) {
	h := DefaultHandler{
		payloadChannel: make(chan []byte),
		errorChannel:   make(chan error),
		subbedTopics:   make(map[string]string),
	}
	opts := GetDefaultOpts()
	opts.SetDefaultPublishHandler(h.messageHandler)
	opts.OnConnect = h.connectHandler
	opts.OnConnectionLost = h.connectLostHandler
	client, err := ConnectMqtt(opts)
	if err != nil {
		return nil, fmt.Errorf("error connecting to mqtt: %v", err)
	}
	h.client = client
	return &h, nil
}

func (h *DefaultHandler) SendMessageToChannel(payload []byte) {
	h.payloadChannel <- payload
}

func (h *DefaultHandler) GetPayloadChannel() <-chan []byte {
	return h.payloadChannel
}

func (h *DefaultHandler) GetErrorChannel() chan error {
	return h.errorChannel
}

func (h *DefaultHandler) Close() error {
	once.Do(func() {
		close(h.payloadChannel)
		close(h.errorChannel)
	})
	for _, topic := range h.subbedTopics {
		if token := h.client.Unsubscribe(topic); !token.WaitTimeout(5 * time.Second) {
			return fmt.Errorf("error unsubscribing from topic: %s", topic)
		}
	}
	h.client.Disconnect(250)
	return nil
}

func (h *DefaultHandler) GetClient() (mqtt.Client, error) {
	if h.client.IsConnected() {
		return h.client, nil
	}
	return nil, fmt.Errorf("client not connected")
}

func (h *DefaultHandler) match(wildcard, topic string) bool {
	wildcardParts := strings.Split(wildcard, "/")
	topicParts := strings.Split(topic, "/")
	if len(wildcardParts) != len(topicParts) {
		return false
	}
	for i, wildcardPart := range wildcardParts {
		if wildcardPart == "+" {
			continue
		}
		if wildcardPart == topicParts[i] {
			continue
		}
		return false
	}
	return true
}

func (h *DefaultHandler) messageHandler(client mqtt.Client, msg mqtt.Message) {
	topic := msg.Topic()
	if _, exists := h.subbedTopics[topic]; exists {
		h.SendMessageToChannel(msg.Payload())
		return
	}
	for possibleWildcard := range h.subbedTopics {
		if h.match(possibleWildcard, topic) {
			h.SendMessageToChannel(msg.Payload())
			return
		}
	}
}

func (h *DefaultHandler) connectHandler(client mqtt.Client) {
	log.Println("Client Connected")
}

func (h *DefaultHandler) connectLostHandler(client mqtt.Client, err error) {
	log.Printf("Connection lost: %v", err)
}

func (h *DefaultHandler) Subscribe(topic string) error {
	h.subbedTopics[topic] = ""
	token := h.client.Subscribe(topic, 1, nil)
	if ok := token.WaitTimeout(10 * time.Second); !ok {
		return fmt.Errorf("failed to subscribe to topic: %s", topic)
	}
	log.Printf("Subscribed to topic: %s", topic)
	return nil
}

// AsyncPayloadHandler listens on the channel of the given SubscriptionHandler Interface
// and processes incoming MQTT payloads asynchronously.
//
// It continues running until the context is canceled.
// Errors are sent to the handler's error channel.
//
// Parameters:
// - numWorkers: Determines how many workers are spawned to handle payload processing.
// - processFunc: A client-defined function that takes a byte slice (representing the MQTT payload) and processes it.
func (h *DefaultHandler) AsyncPayloadProcess(ctx context.Context, numWorkers int, processFunc func([]byte) error) {
	var wg sync.WaitGroup
	payloadCh := h.GetPayloadChannel()
	workerTask := func() {
		defer wg.Done()
		for {
			select {
			case payload, ok := <-payloadCh:
				if !ok {
					return
				}
				if err := processFunc(payload); err != nil {
					select {
					case h.GetErrorChannel() <- err:
					case <-ctx.Done():
						return
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go workerTask()
	}
	<-ctx.Done()
	log.Println("payload handler received shutdown signal")
	wg.Wait()
	log.Println("all workers stopped, handler channels remain open.")
}

// PayloadProcess handles the first payload it receives, before exiting..
func (h *DefaultHandler) PayloadProcess(processFunc func([]byte) error) error {
	payload := <-h.GetPayloadChannel()
	if err := processFunc(payload); err != nil {
		return fmt.Errorf("error processing payload: %v", err)
	}
	return nil
}
