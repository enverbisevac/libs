package pubsub

import "time"

type Config struct {
	App       string
	Namespace string

	SendTimeout time.Duration
	ChannelSize int
}
