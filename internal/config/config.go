package config

import (
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

// Build information -ldflags .
const (
	version    string = "dev"
	commitHash string = "-"
)

var cfg *Config

// GetConfigInstance returns service config
func GetConfigInstance() Config {
	if cfg != nil {
		return *cfg
	}

	return Config{}
}

// Project - contains all parameters project information.
type Project struct {
	Debug       bool   `yaml:"debug"`
	Name        string `yaml:"name"`
	Environment string `yaml:"environment"`
	Version     string
	CommitHash  string
}

// Metrics - contains all parameters metrics information.
type Metrics struct {
	Port int    `yaml:"port"`
	Host string `yaml:"host"`
	Path string `yaml:"path"`
}

// Jaeger - contains all parameters metrics information.
type Jaeger struct {
	Service string `yaml:"service"`
	Host    string `yaml:"host"`
	Port    string `yaml:"port"`
}

// Database - contains all parameters database connection.
type Database struct {
	Host        string `yaml:"host"`
	Port        string `yaml:"port"`
	User        string `yaml:"user"`
	Password    string `yaml:"password"`
	Name        string `yaml:"name"`
	SslMode     string `yaml:"sslmode"`
	Driver      string `yaml:"driver"`
	Connections DBCons `yaml:"connections"`
}

type DBCons struct {
	MaxOpenCons     int           `yaml:"maxOpenCons"`
	MaxIdleCons     int           `yaml:"maxIdleCons"`
	ConnMaxIdleTime time.Duration `yaml:"connMaxIdleTime"`
	ConnMaxLifeTime time.Duration `yaml:"connMaxLifeTime"`
}

// Retranslator config for service
type Retranslator struct {
	ChannelSize   uint64        `yaml:"channelSize"`
	ConsumerCount uint64        `yaml:"consumerCount"`
	BatchSize     uint64        `yaml:"batchSize"`
	Ticker        time.Duration `yaml:"ticker"`
	ProducerCount uint64        `yaml:"producerCount"`
	WorkerCount   int           `yaml:"workerCount"`
}

// Kafka - contains all parameters kafka information.
type Kafka struct {
	Topics  []string      `yaml:"topics"`
	GroupID string        `yaml:"groupId"`
	Brokers []string      `yaml:"brokers"`
	Tick    time.Duration `yaml:"tick"`
}

// Config - contains all configuration parameters in config package.
type Config struct {
	Project      Project      `yaml:"project"`
	Database     Database     `yaml:"database"`
	Metrics      Metrics      `yaml:"metrics"`
	Jaeger       Jaeger       `yaml:"jaeger"`
	Kafka        Kafka        `yaml:"kafka"`
	Retranslator Retranslator `yaml:"retranslator"`
}

// ReadConfigYML - read configurations from file and init instance Config.
func ReadConfigYML(filePath string) error {
	if cfg != nil {
		return nil
	}

	file, err := os.Open(filepath.Clean(filePath))
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()

	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(&cfg); err != nil {
		return err
	}

	cfg.Project.Version = version
	cfg.Project.CommitHash = commitHash

	return nil
}
