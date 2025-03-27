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

type MqttHandler interface {
	messageHandler(client mqtt.Client, msg mqtt.Message)
	Close() error
	Subscribe(topic string) (TopicProcessor, error)
	GetClient() (mqtt.Client, error)
}

type TopicProcessor interface {
	GetPayloadChannel() <-chan []byte
	GetErrorChannel() (chan error, error)
	AsyncPayloadProcess(ctx context.Context, numWorkers int, processFunc func([]byte) error)
	PayloadProcess(processFunc func([]byte) error) error
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
	client          mqtt.Client
	payloadChannels map[string]chan []byte
	errorChannels   map[string]chan error
}

func NewDefaultHandler() (MqttHandler, error) {
	h := DefaultHandler{
		payloadChannels: make(map[string]chan []byte, 0),
		errorChannels:   make(map[string]chan error),
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

func (h *DefaultHandler) Close() error {
	for topic := range h.payloadChannels {
		if token := h.client.Unsubscribe(topic); !token.WaitTimeout(5 * time.Second) {
			return fmt.Errorf("error unsubscribing from topic: %s", topic)
		}
	}
	once.Do(func() {
		for _, c := range h.payloadChannels {
			close(c)
		}
		for _, c := range h.errorChannels {
			close(c)
		}
	})
	h.client.Disconnect(250)
	return nil
}

func (h *DefaultHandler) GetClient() (mqtt.Client, error) {
	if !h.client.IsConnected() {
		return nil, fmt.Errorf("client not connected")
	}
	return h.client, nil
}

func (h *DefaultHandler) match(wildcard, topic string) bool {
	if wildcard == topic {
		return true
	}
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
	if _, ok := h.payloadChannels[topic]; ok {
		h.payloadChannels[topic] <- msg.Payload()
	}
	for possibleWildcard := range h.payloadChannels {
		if h.match(possibleWildcard, topic) {
			h.payloadChannels[topic] <- msg.Payload()
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

func (h *DefaultHandler) Subscribe(topic string) (TopicProcessor, error) {
	token := h.client.Subscribe(topic, 1, nil)
	if ok := token.WaitTimeout(10 * time.Second); !ok {
		return nil, fmt.Errorf("failed to subscribe to topic: %s", topic)
	}
	h.payloadChannels[topic] = make(chan []byte)
	h.errorChannels[topic] = make(chan error)
	log.Printf("Subscribed to topic: %s", topic)
	return &DefaultProcessor{
		payloadChannel: h.payloadChannels[topic],
		errorChannel:   h.errorChannels[topic],
	}, nil
}

type DefaultProcessor struct {
	payloadChannel chan []byte
	errorChannel   chan error
}

func (p *DefaultProcessor) GetPayloadChannel() <-chan []byte {
	return p.payloadChannel
}

func (p *DefaultProcessor) GetErrorChannel() (chan error, error) {
	if p.errorChannel == nil {
		return nil, fmt.Errorf("error channel is nil")
	}
	return p.errorChannel, nil
}

// AsyncPayloadHandler listens on the channel of the given MqttHandler Interface
// and processes incoming MQTT payloads asynchronously.
//
// It continues running until the context is canceled.
// Errors are sent to the handler's error channel.
//
// Parameters:
// - numWorkers: Determines how many workers are spawned to handle payload processing.
// - processFunc: A client-defined function that takes a byte slice (representing the MQTT payload) and processes it.
func (p *DefaultProcessor) AsyncPayloadProcess(ctx context.Context, numWorkers int, processFunc func([]byte) error) {
	var wg sync.WaitGroup
	workerTask := func() {
		defer wg.Done()
		for {
			select {
			case payload, ok := <-p.GetPayloadChannel():
				if !ok {
					return
				}
				if err := processFunc(payload); err != nil {
					errCh, errGet := p.GetErrorChannel()
					if errGet != nil {
						return
					}
					select {
					case errCh <- err:
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
func (p *DefaultProcessor) PayloadProcess(timeout time.Duration, processFunc func([]byte) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	select {
	case payload := <-p.GetPayloadChannel():
		if err := processFunc(payload); err != nil {
			return fmt.Errorf("error processing payload: %v", err)
		}
	case <-ctx.Done():
		return fmt.Errorf("operation time out before payload received: %v", ctx.Err())
	}
	return nil
}
