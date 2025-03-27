package tests

import (
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/geraud22/mqtt_connector"
)

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

var dh = mqtt_connector.DefaultHandler{
	Client: &MockMqttClient{},
}
