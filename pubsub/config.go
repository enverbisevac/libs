package pubsub

import "time"

const (
	DefaultAppName   = "app"
	DefaultNamespace = "default"
)

type Config struct {
	App       string
	Namespace string

	SendTimeout time.Duration
	ChannelSize int
}
