package config

import "sync"

type Config struct {
	Host	string
	Port	int
	Role	ServerRole
}

var (
	instance	*Config
	once		sync.Once
)

func Init(opts *Config) {
	once.Do(func ()  {
		instance = &Config{
			Host: "0.0.0.0",
			Port: 6379,
			Role: Master,
		}

		if opts.Host != "" {
			instance.Host = opts.Host
		}

		if opts.Port != 0 {
			instance.Port = opts.Port
		}

		if opts.Role != "" {
			instance.Role = opts.Role
		}
	})
}

func GetInstance() *Config {
	if instance == nil {
		Init(&Config{})
	}
	return instance
}