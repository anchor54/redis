package config

import (
	"fmt"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

type Config struct {
	Host			string
	Port			int
	Role			ServerRole
	ReplicationID 	string
	Offset			int
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

		instance.ReplicationID = utils.GenerateUniqueID()
		instance.Offset = 0
	})
}

func GetInstance() *Config {
	if instance == nil {
		Init(&Config{})
	}
	return instance
}

func (config *Config) String() string {
	return fmt.Sprintf(
		"role:%s\nmaster_replid:%s\nmaster_repl_offset:%d",
		config.Role,
		config.ReplicationID,
		config.Offset,
	)
}