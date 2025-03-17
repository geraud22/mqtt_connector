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

// Default ConnectMqtt will get its connection information from config.yml file.
func ConnectMqtt() (mqtt.Client, error) {
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
	opts.SetDefaultPublishHandler(messagePubHandler)
	opts.SetKeepAlive(60 * time.Second)
	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler
	opts.SetDefaultPublishHandler(messagePubHandler)
	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("Error connecting to MQTT: %v", token.Error())
	}
	return client, nil
}

var handlers = make(map[string]SubscriptionHandler)

func Match(wildcard, topic string) bool {
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

var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	topic := msg.Topic()
	if handler, exists := handlers[msg.Topic()]; exists {
		handler.SendMessageToChannel(msg.Payload())
		return
	}
	for wildcard, handler := range handlers {
		if Match(wildcard, topic) {
			handler.SendMessageToChannel(msg.Payload())
			return
		}
	}
}

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	fmt.Println("Client Connected")
}

var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	fmt.Printf("Connection lost: %v\n", err)
}

type SubscriptionHandler interface {
	SendMessageToChannel(payload []byte)
	GetPayloadChannel() <-chan []byte
	GetErrorChannel() chan error
	Close() error
	Subscribe(topic string) error
	AsyncPayloadProcess(ctx context.Context, numWorkers int, processFunc func([]byte) error)
}

type DefaultHandler struct {
	client         mqtt.Client
	payloadChannel chan []byte
	errorChannel   chan error
	subbedTopics   map[string]string
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
	close(h.payloadChannel)
	close(h.errorChannel)
	return nil
}

func NewDefaultHandler() (*DefaultHandler, error) {
	client, err := ConnectMqtt()
	if err != nil {
		return nil, fmt.Errorf("error connecting to mqtt: %v", err)
	}
	return &DefaultHandler{
		client:         client,
		payloadChannel: make(chan []byte),
		errorChannel:   make(chan error),
		subbedTopics:   make(map[string]string),
	}, nil
}

// Sub will subscribe to an MQTT topic, only if the client connection has already been established.
//
// Parameters:
// - topicToSub: The string representing the MQTT topic to subscribe to.
//
// Returns:
//   - SubscriptionHandler: An interface which provides a channel where incoming message payloads will be sent,
//     via the package variable messagePubHandler
//   - Error: If the request to subscribe to the given topic times out after 10 seconds, will return error.
func (h *DefaultHandler) Subscribe(topic string) error {
	h.subbedTopics[topic] = ""
	token := h.client.Subscribe(topic, 1, nil)
	if ok := token.WaitTimeout(10 * time.Second); !ok {
		return fmt.Errorf("failed to subscribe to topic: " + topic)
	}
	fmt.Printf("Subscribed to topic: %s\n", topic)
	return nil
}

// AsyncPayloadHandler listens on the channel of the given SubscriptionHandler Interface
// and processes incoming MQTT payloads asynchronously using the provided processFunc.
//
// It continues running until the context is canceled.
// Errors are sent to the handler's error channel.
//
// Parameters:
// - ctx: A context.WithCancel used to control the lifetime of the handler. It should be cancelled to stop the handler gracefully.
// - handler: A SubscriptionHandler that manages the channel through which payloads are received.
// - numWorkers: Determines how many workers are spawned to handle payload processing.
// - processFunc: A client-defined function that takes a byte slice (representing the MQTT payload) and processes it.
func (h *Handler) AsyncPayloadProcess(ctx context.Context, numWorkers int, processFunc func([]byte) error) {
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
	h.Close()
	wg.Wait()
	log.Println("all workers stopped, error channel closed")
}

// PayloadHandler listens on the channel of the given SubscriptionHandler Interface
// and processes the incoming MQTT payload using the provided processFunc.
//
// It will only process one payload before exiting.
//
// Parameters:
// - handler: A SubscriptionHandler that manages the channel through which a payload is received.
// - processFunc: A client-defined function that takes a byte slice (representing the MQTT payload) and processes it.
//
// Returns:
// - An error if something goes wrong during processing.
func PayloadHandler(handler SubscriptionHandler, processFunc func([]byte) error) error {
	payload := <-handler.GetPayloadChannel()
	if err := processFunc(payload); err != nil {
		return fmt.Errorf("error processing payload: %v", err)
	}
	return nil
}
