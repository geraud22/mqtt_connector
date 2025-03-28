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

type ProcessFunc func([]byte) error

type MqttHandler interface {
	Close() error
	MessageHandler(client mqtt.Client, msg mqtt.Message)
	Subscribe(topic string) (TopicProcessor, error)
	GetClient() (mqtt.Client, error)
}

// Note: TopicProcessor is spawned by MqttHandler Subscribe. Therefore, MqttHandler remains responsible for closing spawned TopicProcessors.
type TopicProcessor interface {
	Close() error
	SendPayload(payload []byte)
	AsyncPayloadProcess(ctx context.Context, numWorkers int, processFunc ProcessFunc)
	PayloadProcess(ctx context.Context, processFunc ProcessFunc) error
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
	Client     mqtt.Client
	processors map[string]TopicProcessor
}

func NewDefaultHandler() (MqttHandler, error) {
	h := DefaultHandler{
		processors: make(map[string]TopicProcessor, 0),
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
	for topic := range h.processors {
		if token := h.Client.Unsubscribe(topic); !token.WaitTimeout(5 * time.Second) {
			return fmt.Errorf("error unsubscribing from topic: %s", topic)
		}
	}
	h.Client.Disconnect(250)
	return nil
}

func (h *DefaultHandler) Subscribe(topic string) (TopicProcessor, error) {
	token := h.Client.Subscribe(topic, 1, nil)
	if ok := token.WaitTimeout(10 * time.Second); !ok {
		return nil, fmt.Errorf("failed to subscribe to topic: %s", topic)
	}
	log.Printf("Mqtt Connector - Subscribed to topic: %s", topic)
	p := &defaultProcessor{
		payloadChannel: make(chan []byte),
		errorChannel:   make(chan error),
	}
	h.processors[topic] = p
	return p, nil
}

func (h *DefaultHandler) MessageHandler(client mqtt.Client, msg mqtt.Message) {
	topic := msg.Topic()
	if p, ok := h.processors[topic]; ok {
		p.SendPayload(msg.Payload())
	}
	for wildcard := range h.processors {
		if h.match(wildcard, topic) {
			if p, ok := h.processors[wildcard]; ok {
				p.SendPayload(msg.Payload())
				return
			}
		}
	}
}

func (h *DefaultHandler) connectHandler(client mqtt.Client) {
	log.Println("Mqtt Connector - Client Connected")
}

func (h *DefaultHandler) connectLostHandler(client mqtt.Client, err error) {
	log.Printf("Mqtt Connector - Connection lost: %v", err)
}

func (h *DefaultHandler) GetClient() (mqtt.Client, error) {
	if !h.Client.IsConnected() {
		return nil, fmt.Errorf("client not connected")
	}
	return h.Client, nil
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

type defaultProcessor struct {
	payloadChannel chan []byte
	errorChannel   chan error
}

func (p *defaultProcessor) Close() error {
	once.Do(func() {
		close(p.payloadChannel)
		close(p.errorChannel)
	})
	return nil
}

func (p *defaultProcessor) getPayloadChannel() <-chan []byte {
	return p.payloadChannel
}

func (p *defaultProcessor) getErrorChannel() (chan error, error) {
	if p.errorChannel == nil {
		return nil, fmt.Errorf("error channel is nil")
	}
	return p.errorChannel, nil
}

func (p *defaultProcessor) SendPayload(payload []byte) {
	if p.payloadChannel == nil {
		return
	}
	p.payloadChannel <- payload
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
func (p *defaultProcessor) AsyncPayloadProcess(ctx context.Context, numWorkers int, processFunc ProcessFunc) {
	var wg sync.WaitGroup
	workerTask := func() {
		defer wg.Done()
		for {
			select {
			case payload, ok := <-p.payloadChannel:
				if !ok {
					return
				}
				if err := processFunc(payload); err != nil {
					errCh, closedChErr := p.getErrorChannel()
					if closedChErr != nil {
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
func (p *defaultProcessor) PayloadProcess(ctx context.Context, processFunc ProcessFunc) error {
	select {
	case payload := <-p.payloadChannel:
		if err := processFunc(payload); err != nil {
			return fmt.Errorf("error processing payload: %v", err)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("operation cancelled: %v", ctx.Err())
	}
}
