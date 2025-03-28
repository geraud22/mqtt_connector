package mqtt_connector

import (
	"context"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var dh = &DefaultHandler{
	client:     &MockMqttClient{},
	processors: make(map[string]TopicProcessor),
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
			t.Fatalf("%s FAILED: expected err: %v, got: %v", tt.name, tt.wantErr, err)
		}
		if _, ok := dh.processors[tt.topic]; !ok {
			t.Fatalf("%s channel is invalid", tt.topic)
		}
	}
}

func TestPayloadProcess(t *testing.T) {
	p := &defaultProcessor{}
	processFunc := func(_ []byte) error {
		return nil
	}
	tests := []struct {
		name      string
		cancelCtx bool
		wantErr   bool
	}{
		{
			name:      "returns after receiving payload",
			cancelCtx: false,
			wantErr:   false,
		},
		{
			name:      "returns after context cancelled",
			cancelCtx: true,
			wantErr:   true,
		},
	}
	var wg sync.WaitGroup
	for _, tt := range tests {
		ctx, cancel := context.WithCancel(context.Background())
		p.payloadChannel = make(chan []byte)
		defer cancel()
		defer close(p.payloadChannel)
		p.errorChannel = make(chan error, 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := p.PayloadProcess(ctx, processFunc)
			p.errorChannel <- err
		}()
		if tt.cancelCtx {
			cancel()
		} else {
			p.payloadChannel <- []byte("test data")
		}
		wg.Wait()
		close(p.errorChannel)
		for err := range p.errorChannel {
			if tt.wantErr != (err != nil) {
				t.Fatalf("%s FAILED: expected error: %v, got: %v", tt.name, tt.wantErr, err)
			}
		}
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
		if ok := dh.match(tt.wildcard, tt.topic); ok != tt.want {
			t.Fatalf("%s FAILED expected %v, got %v", tt.name, tt.want, ok)
		}
	}
}
