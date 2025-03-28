package mqtt_connector

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/dimonomid/clock"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	cfy "github.com/geraud22/config-from-yaml"
)

var once sync.Once

type MqttHandler interface {
	MessageHandler(client mqtt.Client, msg mqtt.Message)
	Close() error
	Subscribe(topic string) (TopicProcessor, error)
	GetClient() (mqtt.Client, error)
	WildCardMatch(wildcard, topic string) bool
}

type TopicProcessor interface {
	GetPayloadChannel() <-chan []byte
	GetErrorChannel() (chan error, error)
	AsyncPayloadProcess(ctx context.Context, numWorkers int, processFunc func([]byte) error)
	PayloadProcess(timeout time.Duration, processFunc func([]byte) error) error
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
	Client          mqtt.Client
	PayloadChannels map[string]chan []byte
	ErrorChannels   map[string]chan error
}

func NewDefaultHandler() (MqttHandler, error) {
	h := DefaultHandler{
		PayloadChannels: make(map[string]chan []byte, 0),
		ErrorChannels:   make(map[string]chan error),
	}
	opts := GetDefaultOpts()
	opts.SetDefaultPublishHandler(h.MessageHandler)
	opts.OnConnect = h.connectHandler
	opts.OnConnectionLost = h.connectLostHandler
	client, err := ConnectMqtt(opts)
	if err != nil {
		return nil, fmt.Errorf("error connecting to mqtt: %v", err)
	}
	h.Client = client
	return &h, nil
}

func (h *DefaultHandler) Close() error {
	for topic := range h.PayloadChannels {
		if token := h.Client.Unsubscribe(topic); !token.WaitTimeout(5 * time.Second) {
			return fmt.Errorf("error unsubscribing from topic: %s", topic)
		}
	}
	once.Do(func() {
		for _, c := range h.PayloadChannels {
			close(c)
		}
		for _, c := range h.ErrorChannels {
			close(c)
		}
	})
	h.Client.Disconnect(250)
	return nil
}

func (h *DefaultHandler) GetClient() (mqtt.Client, error) {
	if !h.Client.IsConnected() {
		return nil, fmt.Errorf("client not connected")
	}
	return h.Client, nil
}

func (h *DefaultHandler) WildCardMatch(wildcard, topic string) bool {
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

func (h *DefaultHandler) MessageHandler(client mqtt.Client, msg mqtt.Message) {
	topic := msg.Topic()
	if _, ok := h.PayloadChannels[topic]; ok {
		h.PayloadChannels[topic] <- msg.Payload()
	}
	for possibleWildcard := range h.PayloadChannels {
		if h.WildCardMatch(possibleWildcard, topic) {
			h.PayloadChannels[topic] <- msg.Payload()
			return
		}
	}
}

func (h *DefaultHandler) connectHandler(client mqtt.Client) {
	log.Println("Mqtt Connector - Client Connected")
}

func (h *DefaultHandler) connectLostHandler(client mqtt.Client, err error) {
	log.Printf("Mqtt Connector - Connection lost: %v", err)
}

func (h *DefaultHandler) Subscribe(topic string) (TopicProcessor, error) {
	token := h.Client.Subscribe(topic, 1, nil)
	if ok := token.WaitTimeout(10 * time.Second); !ok {
		return nil, fmt.Errorf("failed to subscribe to topic: %s", topic)
	}
	h.PayloadChannels[topic] = make(chan []byte)
	h.ErrorChannels[topic] = make(chan error)
	log.Printf("Mqtt Connector - Subscribed to topic: %s", topic)
	p, err := NewDefaultProcessor(clock.New(), h.PayloadChannels[topic], h.ErrorChannels[topic])
	if err != nil {
		return nil, fmt.Errorf("error creating processor: %v", err)
	}
	return p, nil
}

type DefaultProcessor struct {
	PayloadChannel chan []byte
	ErrorChannel   chan error
	Clock          clock.Clock
}

func NewDefaultProcessor(clock clock.Clock, payloadCh chan []byte, errCh chan error) (*DefaultProcessor, error) {
	if clock == nil {
		return nil, fmt.Errorf("received nil clock")
	}
	return &DefaultProcessor{
		PayloadChannel: payloadCh,
		ErrorChannel:   errCh,
		Clock:          clock,
	}, nil
}

func (p *DefaultProcessor) GetPayloadChannel() <-chan []byte {
	return p.PayloadChannel
}

func (p *DefaultProcessor) GetErrorChannel() (chan error, error) {
	if p.ErrorChannel == nil {
		return nil, fmt.Errorf("error channel is nil")
	}
	return p.ErrorChannel, nil
}

// AsyncPayloadHandler listens on the TopicProcessor payload channel
// and processes incoming MQTT payloads asynchronously.
//
// It continues running until the context is canceled.
// Errors are sent to the TopicProcessor's error channel.
//
// Parameters:
// - numWorkers: Determines how many workers are spawned to handle payload processing.
// - processFunc: A client-defined function that defines what to do with a received payload.
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
	log.Println("Mqtt Connector - payload handler received shutdown signal")
	wg.Wait()
	log.Println("Mqtt Connector - all workers stopped.")
}

// PayloadProcess handles the first payload it receives, before exiting.
func (p *DefaultProcessor) PayloadProcess(timeout time.Duration, processFunc func([]byte) error) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	select {
	case payload := <-p.GetPayloadChannel():
		if err := processFunc(payload); err != nil {
			return fmt.Errorf("error processing payload: %v", err)
		}
	case <-p.Clock.After(timeout):
		return fmt.Errorf("operation time out before payload received: %v", ctx.Err())
	case <-ctx.Done():
		return fmt.Errorf("operation cancelled: %v", ctx.Err())
	}
	return nil
}
