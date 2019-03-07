package config

import (
	"time"

	"github.com/caarlos0/env"
)

// Represents a structure with all env variables needed by the backend.
var cfg struct {
	DatabaseUser                 string        `env:"DATABASE_USER,required"`
	DatabasePassword             string        `env:"DATABASE_PASSWORD,required"`
	DatabaseHost                 string        `env:"DATABASE_HOST,required"`
	DatabasePort                 int           `env:"DATABASE_PORT" envDefault:"5432"`
	DatabaseName                 string        `env:"DATABASE_NAME,required"`
	DatabaseSSLMode              string        `env:"DATABASE_SSLMODE" envDefault:"disable"`
	RMQHost                      string        `env:"RMQ_HOST,required"`
	RMQPort                      int           `env:"RMQ_PORT,required"`
	RMQVHost                     string        `env:"RMQ_VHOST,required"`
	RMQUser                      string        `env:"RMQ_USER,required"`
	RMQPassword                  string        `env:"RMQ_PASSWORD,required"`
	RMQExchanges                 []string      `env:"RMQ_EXCHANGES,required"`
	ListenerMinReconnectInterval time.Duration `env:"LISTENER_MIN_RECONNECT_INTERVAL" envDefault:"5s"`
	ListenerMaxReconnectInterval time.Duration `env:"LISTENER_MAX_RECONNECT_INTERVAL" envDefault:"30s"`
	ListenerChannelName          string        `env:"LISTENER_CHANNEL_NAME" envDefault:"amqp_events"`
}

func init() {
	if err := env.Parse(&cfg); err != nil {
		panic(err)
	}
}
