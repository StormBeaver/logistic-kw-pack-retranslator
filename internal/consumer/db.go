package consumer

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/stormbeaver/logistic-pack-retranslator/internal/eventCounter"
	"github.com/stormbeaver/logistic-pack-retranslator/internal/repo"
)

type Consumer interface {
	Start()
	Close()
}

type consumer struct {
	consumerCount uint64
	events        chan<- repo.PackEvent

	repo repo.EventRepo

	batchSize uint64
	tick      time.Duration

	wg *sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc
}

func NewDbConsumer(
	consumerCount uint64,
	batchSize uint64,
	consumeTimeout time.Duration,
	repo repo.EventRepo,
	events chan<- repo.PackEvent) *consumer {

	wg := &sync.WaitGroup{}

	return &consumer{
		consumerCount: consumerCount,
		batchSize:     batchSize,
		tick:          consumeTimeout,
		repo:          repo,
		events:        events,
		wg:            wg,
	}
}

func (c *consumer) Start() {
	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.lockDelivery()
	c.mainDelivery()
}

func (c *consumer) Close() {
	c.cancel()
	c.wg.Wait()
}

func (c *consumer) lockDelivery() {
	for range c.consumerCount {
		c.wg.Add(1)

		go func() {
			defer c.wg.Done()
			ticker := time.NewTicker(c.tick)
			for {
				select {
				case <-ticker.C:
					events, err := c.repo.PreProcess(c.ctx, c.batchSize)
					if err != nil {
						continue
					}
					eventCounter.EventsCount.Add(float64(len(events)))

					for _, event := range events {
						c.events <- event
					}
					if len(events) == 0 {
						return
					}
				case <-c.ctx.Done():
					return
				}
			}
		}()
	}
	c.wg.Wait()
}

func (c *consumer) mainDelivery() {
	for range c.consumerCount {
		c.wg.Add(1)

		go func() {
			defer c.wg.Done()
			ticker := time.NewTicker(c.tick)
			for {
				select {
				case <-ticker.C:
					events, err := c.repo.Lock(c.ctx, c.batchSize)
					if err != nil {
						log.Err(err).Msg("mission failed")
						continue
					}
					eventCounter.EventsCount.Add(float64(len(events)))

					for _, event := range events {
						c.events <- event
					}
				case <-c.ctx.Done():
					return
				}
			}
		}()
	}
}
