package tests

import (
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/geraud22/mqtt_connector"
)

var dh = &mqtt_connector.DefaultHandler{
	Client:          &MockMqttClient{},
	PayloadChannels: make(map[string]chan []byte),
	ErrorChannels:   make(map[string]chan error),
}

type MockToken struct{}

func (t *MockToken) Wait() bool {
	return true
}
func (t *MockToken) WaitTimeout(time.Duration) bool {
	return true
}
func (t *MockToken) Done() <-chan struct{} {
	return make(<-chan struct{})
}
func (t *MockToken) Error() error {
	return nil
}

type MockMqttClient struct{}

func (m *MockMqttClient) IsConnected() bool {
	return true
}
func (m *MockMqttClient) IsConnectionOpen() bool {
	return true
}
func (m *MockMqttClient) Connect() mqtt.Token {
	return &MockToken{}
}
func (m *MockMqttClient) Disconnect(quiesce uint) {}
func (m *MockMqttClient) Publish(
	topic string,
	qos byte,
	retained bool,
	payload interface{},
) mqtt.Token {
	return &MockToken{}
}
func (m *MockMqttClient) Subscribe(
	topic string,
	qos byte,
	callBack mqtt.MessageHandler,
) mqtt.Token {
	return &MockToken{}
}
func (m *MockMqttClient) SubscribeMultiple(
	filters map[string]byte,
	callback mqtt.MessageHandler,
) mqtt.Token {
	return &MockToken{}
}
func (m *MockMqttClient) Unsubscribe(topics ...string) mqtt.Token {
	return &MockToken{}
}
func (m *MockMqttClient) AddRoute(topic string, callback mqtt.MessageHandler) {}
func (m *MockMqttClient) OptionsReader() mqtt.ClientOptionsReader {
	return mqtt.ClientOptionsReader{}
}

func TestSubscribe(t *testing.T) {
	tests := []struct {
		name    string
		topic   string
		wantErr bool
	}{
		{
			name:  "Successful Subscribe",
			topic: "success",
		},
	}

	for _, tt := range tests {
		_, err := dh.Subscribe(tt.topic)
		if tt.wantErr != (err != nil) {
			t.Fatalf("%s failed: expected err: %v, got: %v", tt.name, tt.wantErr, err)
		}
		if _, ok := dh.PayloadChannels[tt.topic]; !ok {
			t.Fatalf("%s channel is invalid", tt.topic)
		}
	}
}

func TestPayloadProcess(t *testing.T) {
	testErrCh := make(chan error, 1)
	p := &mqtt_connector.DefaultProcessor{
		PayloadChannel: make(chan []byte, 1),
		ErrorChannel:   make(chan error, 1),
	}
	processFunc := func(_ []byte) error {
		return nil
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := p.PayloadProcess(time.Duration(5*time.Second), processFunc)
		testErrCh <- err
	}()
	p.PayloadChannel <- []byte("test data")
	wg.Wait()
	close(testErrCh)
	for err := range testErrCh {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}
