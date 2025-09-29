package producer

import (
	"context"

	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/stormbeaver/logistic-pack-retranslator/internal/eventCounter"
	"github.com/stormbeaver/logistic-pack-retranslator/internal/repo"
	"github.com/stormbeaver/logistic-pack-retranslator/internal/sender"

	"github.com/gammazero/workerpool"
)

type Producer interface {
	Start()
	Close()
}

type producer struct {
	producerCount uint64
	batchSize     uint64
	tick          time.Duration

	sender sender.EventSender
	events <-chan repo.PackEvent

	repo repo.EventRepo

	workerPool *workerpool.WorkerPool

	wg *sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc
}

func NewKafkaProducer(
	producerCount uint64,
	sender sender.EventSender,
	events <-chan repo.PackEvent,
	workerPool *workerpool.WorkerPool,
	repo repo.EventRepo,
	ticker time.Duration,
	batchSize uint64,
) Producer {
	wg := &sync.WaitGroup{}

	return &producer{
		producerCount: producerCount,
		sender:        sender,
		events:        events,
		workerPool:    workerPool,
		repo:          repo,
		wg:            wg,
		tick:          ticker,
		batchSize:     batchSize,
	}
}

func (p *producer) Start() {
	p.ctx, p.cancel = context.WithCancel(context.Background())
	for range p.producerCount {
		p.wg.Add(1)
		go func() {

			toUnlock := make([]uint64, 0, int(p.batchSize))
			toRemove := make([]uint64, 0, int(p.batchSize))

			defer p.wg.Done()
			ticker := time.NewTicker(p.tick)
			for {
				select {
				case event := <-p.events:
					if err := p.sender.Send(&event); err != nil {
						log.Err(err).Msg("send to kafka failed")
						toUnlock = append(toUnlock, event.ID)
					} else {
						toRemove = append(toRemove, event.ID)
					}
				case <-ticker.C:
					p.delivery(toRemove, toUnlock)
					log.Debug().Int("send to remove", len(toRemove)).
						Int("send to unlock", len(toUnlock)).
						Uints64("send to remove", toRemove).
						Uints64("send to unlock", toUnlock).Send()
					toUnlock = toUnlock[:0]
					toRemove = toRemove[:0]
				case <-p.ctx.Done():
					p.delivery(toRemove, toUnlock)
					log.Debug().Uints64("ctx.Done() case toUnlock", toUnlock).
						Uints64("ctx.Done() case toRemove", toRemove)
					return
				}
			}
		}()
	}
}

func (p *producer) Close() {
	p.cancel()
	p.wg.Wait()
}

func (p *producer) delivery(toRemove, toUnlock []uint64) {
	ctx, _ := context.WithCancel(context.Background())

	if len(toUnlock) != 0 {
		tUnlock := append(make([]uint64, 0, len(toUnlock)), toUnlock...)
		p.workerPool.Submit(func() {
			if err := p.repo.Unlock(ctx, tUnlock); err != nil {
				log.Err(err).Msg("delivery unlock fail")
			}
			eventCounter.EventsCount.Sub(float64(len(tUnlock)))
		})
	}

	if len(toRemove) != 0 {
		tRemove := append(make([]uint64, 0, len(toRemove)), toRemove...)
		p.workerPool.Submit(func() {
			if err := p.repo.Remove(ctx, tRemove); err != nil {
				log.Err(err).Msg("delivery remove fail")
				if err := p.repo.Unlock(p.ctx, tRemove); err != nil {
					log.Err(err).Msg("delivery unlock in remove - failed")
				}
			}
			eventCounter.EventsCount.Sub(float64(len(tRemove)))
		})
	}
}
