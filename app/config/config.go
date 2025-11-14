package config

import (
	"fmt"
	"strings"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

type Config struct {
	Host          string
	Port          int
	Role          ServerRole
	MasterHost    string
	MasterPort    string
	ReplicationID string
	Offset        int
}

type ConfigOptions struct {
	Port          int
	Role          ServerRole
	MasterAddress string
}

var (
	instance *Config
	once     sync.Once
)

func Init(opts *ConfigOptions) {
	once.Do(func() {
		instance = &Config{
			Host:          "0.0.0.0",
			Port:          6379,
			Role:          Master,
			ReplicationID: utils.GenerateUniqueID(),
		}

		if opts.Port != 0 {
			instance.Port = opts.Port
		}

		if opts.Role != "" {
			instance.Role = opts.Role
		}

		if opts.MasterAddress != "" {
			parts := strings.Split(opts.MasterAddress, " ")
			instance.MasterHost, instance.MasterPort = parts[0], parts[1]
		}

		if instance.Role == Slave {
			instance.ReplicationID = "?"
			instance.Offset = -1
		}
	})
}

func GetInstance() *Config {
	if instance == nil {
		Init(&ConfigOptions{})
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
