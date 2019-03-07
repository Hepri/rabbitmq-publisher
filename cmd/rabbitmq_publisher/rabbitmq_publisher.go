package main

import (
	"database/sql"
	"fmt"

	"github.com/Hepri/rabbitmq_publisher/config"
	"github.com/Hepri/rabbitmq_publisher/publisher"
	_ "github.com/jackc/pgx/stdlib"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

func openDB() (*sql.DB, error) {
	// Start a database connection.
	db, err := sql.Open("pgx", config.DatabaseURL())
	if err != nil {
		return nil, err
	}

	// Actually test the connection against the database, so we catch
	// problematic connections early.
	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

func openListener() (*pq.Listener, error) {
	listener := pq.NewListener(
		config.DatabaseURL(),
		config.ListenerMinReconnectInterval(),
		config.ListenerMaxReconnectInterval(),
		func(ev pq.ListenerEventType, err error) {
			if err != nil {
				logrus.WithError(err).Error("listener error occured")
			}
		})

	if err := listener.Listen(config.ListenerChannelName()); err != nil {
		logrus.WithField("channel", config.ListenerChannelName()).Error("failed to listen channel")
		return nil, err
	}

	return listener, nil
}

func main() {
	db, err := openDB()
	if err != nil {
		panic(fmt.Sprintf("failed to connect database: %v", err))
	}
	defer db.Close()

	listener, err := openListener()
	if err != nil {
		panic(fmt.Sprintf("failed to open listener: %v", err))
	}
	defer listener.Close()

	// run publishing
	done := make(chan bool)
	pub := publisher.NewEventPublisher(config.RMQConnectionURL(), config.RMQExchanges(), db, listener)
	pub.Start()

	<-done
}
