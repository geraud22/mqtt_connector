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
	p := &mqtt_connector.DefaultProcessor{}
	processFunc := func(_ []byte) error {
		return nil
	}
	tests := []struct {
		name             string
		testErrCh        chan error
		processTimeout   time.Duration
		payloadSendDelay time.Duration
		wantErr          bool
	}{
		{
			name:           "successful payload process",
			testErrCh:      make(chan error, 1),
			processTimeout: time.Duration(5 * time.Second),
		},
		{
			name:             "payload process timeout before payload received",
			testErrCh:        make(chan error, 1),
			processTimeout:   time.Duration(1 * time.Millisecond),
			payloadSendDelay: time.Duration(5 * time.Millisecond),
			wantErr:          true,
		},
	}
	var wg sync.WaitGroup
	for _, tt := range tests {
		p.PayloadChannel = make(chan []byte, 1)
		p.ErrorChannel = make(chan error, 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := p.PayloadProcess(tt.processTimeout, processFunc)
			tt.testErrCh <- err
		}()
		time.Sleep(tt.payloadSendDelay)
		p.PayloadChannel <- []byte("test data")
		wg.Wait()
		close(tt.testErrCh)
		for err := range tt.testErrCh {
			if tt.wantErr != (err != nil) {
				t.Fatalf("%s failed: expected error: %v, got: %v", tt.name, tt.wantErr, err)
			}
		}
		close(p.PayloadChannel)
		close(p.ErrorChannel)
	}
}

func TestMatch(t *testing.T) {
	tests := []struct {
		name     string
		wildcard string
		topic    string
		want     bool
	}{
		{
			name:     "match identical",
			wildcard: "identical",
			topic:    "identical",
			want:     true,
		},
		{
			name:     "match wildcard",
			wildcard: "match/+/wildcard",
			topic:    "match/some/wildcard",
			want:     true,
		},
		{
			name:     "no match",
			wildcard: "no",
			topic:    "match",
			want:     false,
		},
		{
			name:     "no match wildcard",
			wildcard: "no/+",
			topic:    "no/match/wildcard",
			want:     false,
		},
		{
			name:     "no match different part lengths",
			wildcard: "part/1",
			topic:    "part/1/2",
			want:     false,
		},
	}

	for _, tt := range tests {
		if ok := dh.Match(tt.wildcard, tt.topic); ok != tt.want {
			t.Fatalf("%s failed: expected %v, got %v", tt.name, tt.want, ok)
		}
	}
}
