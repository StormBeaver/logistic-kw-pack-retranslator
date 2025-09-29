package sender

import (
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"
	"github.com/stormbeaver/logistic-pack-retranslator/internal/repo"
)

type EventSender interface {
	Send(pack *repo.PackEvent) error
}

type Sender struct {
	sarama.SyncProducer
}

func (s Sender) Send(pack *repo.PackEvent) error {
	msg, err := json.Marshal(*pack)
	if err != nil {
		return fmt.Errorf("marshaling pack: %w", err)
	}

	_, _, err = s.SendMessage(prepareMessage(pack.Type, msg))
	if err != nil {
		return fmt.Errorf("send message to Kafka: %w", err)
	}
	log.Debug().Uint64("send event ID:", pack.ID).Send()

	return nil
}

func NewEventSender(brokers []string) EventSender {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatal().Err(err)
	}

	return Sender{producer}
}

func prepareMessage(topic string, message []byte) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Value:     sarama.ByteEncoder(message),
	}
	return msg
}
