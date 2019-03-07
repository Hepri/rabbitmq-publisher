package config

import (
	"time"
)

func ListenerMinReconnectInterval() time.Duration {
	return cfg.ListenerMinReconnectInterval
}

func ListenerMaxReconnectInterval() time.Duration {
	return cfg.ListenerMaxReconnectInterval
}

func ListenerChannelName() string {
	return cfg.ListenerChannelName
}
