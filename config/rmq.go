package config

import "fmt"

func RMQConnectionURL() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%d/%s",
		cfg.RMQUser,
		cfg.RMQPassword,
		cfg.RMQHost,
		cfg.RMQPort,
		cfg.RMQVHost,
	)
}

func RMQExchanges() []string {
	return cfg.RMQExchanges
}
