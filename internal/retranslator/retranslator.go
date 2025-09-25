package retranslator

import (
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stormbeaver/logistic-pack-retranslator/internal/config"
	"github.com/stormbeaver/logistic-pack-retranslator/internal/consumer"
	"github.com/stormbeaver/logistic-pack-retranslator/internal/producer"
	"github.com/stormbeaver/logistic-pack-retranslator/internal/repo"
	"github.com/stormbeaver/logistic-pack-retranslator/internal/sender"

	"github.com/gammazero/workerpool"
)

type Retranslator interface {
	Start()
	Close()
}

type Config struct {
	ChannelSize uint64

	ConsumerCount uint64

	BatchSize uint64
	Ticker    time.Duration

	ProducerCount uint64
	WorkerCount   int

	Repo   repo.EventRepo
	Sender sender.EventSender
}

type retranslator struct {
	events     chan repo.PackEvent
	consumer   consumer.Consumer
	producer   producer.Producer
	workerPool *workerpool.WorkerPool
}

func RetConfig(cfg *config.Config, db *sqlx.DB) *Config {
	return &Config{
		ChannelSize:   cfg.Retranslator.ChannelSize,
		ConsumerCount: cfg.Retranslator.ConsumerCount,
		BatchSize:     cfg.Retranslator.BatchSize,
		ProducerCount: cfg.Retranslator.ProducerCount,
		WorkerCount:   cfg.Retranslator.WorkerCount,
		Ticker:        cfg.Retranslator.Ticker,
		Repo:          repo.NewEventRepo(db),
		Sender:        sender.NewEventSender(cfg.Kafka.Brokers),
	}
}

func NewRetranslator(cfg Config) Retranslator {
	events := make(chan repo.PackEvent, cfg.ChannelSize)
	workerPool := workerpool.New(cfg.WorkerCount)

	consumer := consumer.NewDbConsumer(
		cfg.ConsumerCount,
		cfg.BatchSize,
		cfg.Ticker,
		cfg.Repo,
		events)

	producer := producer.NewKafkaProducer(
		cfg.ProducerCount,
		cfg.Sender,
		events,
		workerPool,
		cfg.Repo,
		cfg.Ticker,
		cfg.BatchSize)

	return &retranslator{
		events:     events,
		consumer:   consumer,
		producer:   producer,
		workerPool: workerPool,
	}
}

func (r *retranslator) Start() {
	r.producer.Start()
	r.consumer.Start()
}

func (r *retranslator) Close() {
	r.consumer.Close()
	r.producer.Close()
	r.workerPool.StopWait()
}
