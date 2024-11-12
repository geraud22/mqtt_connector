package mqtt_connector

import (
	"testing"
)

func TestMatch(t *testing.T) {
	got := Match("application/+/device/+/event/up", "application/123/device/456/event/up")
	expected := true
	if got != expected {
		t.Errorf("got %v expected %v", got, expected)
	}
}
