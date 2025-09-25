package consumer

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	_ "github.com/jackc/pgx/v4"
	_ "github.com/jackc/pgx/v4/stdlib"
	_ "github.com/lib/pq"
	"github.com/stormbeaver/logistic-pack-retranslator/internal/config"
	"github.com/stormbeaver/logistic-pack-retranslator/internal/database"
	"github.com/stormbeaver/logistic-pack-retranslator/internal/mocks"
	"github.com/stormbeaver/logistic-pack-retranslator/internal/repo"
	model "github.com/stormbeaver/logistic-pack-retranslator/internal/repo"
	"github.com/stormbeaver/logistic-pack-retranslator/internal/sender"
)

type ConfigDB struct {
	ChannelSize uint64

	ConsumerCount uint64

	BatchSize uint64
	Ticker    time.Duration

	ProducerCount uint64
	WorkerCount   int

	Repo   repo.EventRepo
	Sender sender.EventSender
}

type ConfigMock struct {
	n         uint64
	events    chan<- model.PackEvent
	repo      repo.EventRepo
	batchSize uint64
	timeout   time.Duration
}

func TestMainDeliveryMock(t *testing.T) {
	var (
		Id          uint64
		mu          sync.Mutex
		countErr    int
		events      = make(chan model.PackEvent, 512)
		ctrl        = gomock.NewController(t)
		repo        = mocks.NewMockEventRepo(ctrl)
		err         = errors.New("planned Error")
		ctx, cancel = context.WithCancel(context.Background())
	)

	defer ctrl.Finish()

	cfg := ConfigMock{
		n:         2,
		events:    events,
		repo:      repo,
		batchSize: 5,
		timeout:   2 * time.Second,
	}

	firstCase := repo.EXPECT().Lock(ctx, cfg.batchSize).Times(int(cfg.n) * 2).DoAndReturn(func(ctx context.Context, n uint64) ([]model.PackEvent, error) {
		output := make([]model.PackEvent, 2)
		for i := range output {
			output[i] = model.PackEvent{ID: Id}
			mu.Lock()
			Id++
			mu.Unlock()
		}
		return output, nil
	})

	secondCase := repo.EXPECT().Lock(ctx, cfg.batchSize).Times(int(cfg.n) * 2).DoAndReturn(func(ctx context.Context, n uint64) ([]model.PackEvent, error) {
		mu.Lock()
		countErr++
		mu.Unlock()
		fmt.Println("error #:", countErr)
		return []model.PackEvent{}, err
	}).After(firstCase)

	repo.EXPECT().Lock(ctx, cfg.batchSize).Times(int(cfg.n) * 2).DoAndReturn(func(ctx context.Context, n uint64) ([]model.PackEvent, error) {
		output := make([]model.PackEvent, rand.N(cfg.batchSize))
		for i := range output {
			output[i] = model.PackEvent{ID: Id}
			mu.Lock()
			Id++
			mu.Unlock()
		}
		return output, nil
	}).After(secondCase)

	c := NewDbConsumer(cfg.n,
		cfg.batchSize,
		cfg.timeout,
		cfg.repo,
		cfg.events)

	c.ctx, c.cancel = ctx, cancel

	c.mainDelivery()

	go func() {
		for v := range events {
			fmt.Println(v.ID)
		}
	}()
	time.Sleep(13 * time.Second)
	c.Close()
}

func TestMainDeliveryMocksV2(t *testing.T) {
	var (
		Id, idCounter uint64
		mu            sync.Mutex
		events        = make(chan model.PackEvent, 512)
		ctrl          = gomock.NewController(t)
		repo          = mocks.NewMockEventRepo(ctrl)
		ctx, cancel   = context.WithCancel(context.Background())
	)

	defer ctrl.Finish()

	cfg := ConfigMock{
		n:         1,
		events:    events,
		repo:      repo,
		batchSize: 5,
		timeout:   2 * time.Second,
	}

	repo.EXPECT().Lock(ctx, cfg.batchSize).Times(int(cfg.n) * 2).DoAndReturn(func(ctx context.Context, n uint64) ([]model.PackEvent, error) {
		output := make([]model.PackEvent, 2)
		for i := range output {
			output[i] = model.PackEvent{ID: Id}
			mu.Lock()
			Id++
			mu.Unlock()
		}
		return output, nil
	})

	c := NewDbConsumer(cfg.n,
		cfg.batchSize,
		cfg.timeout,
		cfg.repo,
		cfg.events)

	c.ctx, c.cancel = ctx, cancel

	c.mainDelivery()
	go func() {
		for v := range events {
			if v.ID != uint64(idCounter) {
				t.Fail()
			}
			idCounter++
		}
	}()
	time.Sleep(5 * time.Second)
	c.Close()
}

func TestLockDeliveryMock(t *testing.T) {
	var (
		events      = make(chan model.PackEvent, 512)
		ctrl        = gomock.NewController(t)
		repo        = mocks.NewMockEventRepo(ctrl)
		ctx, cancel = context.WithCancel(context.Background())
	)

	defer ctrl.Finish()

	cfg := ConfigMock{
		n:         2,
		events:    events,
		repo:      repo,
		batchSize: 5,
		timeout:   2 * time.Second,
	}

	c := NewDbConsumer(cfg.n,
		cfg.batchSize,
		cfg.timeout,
		cfg.repo,
		cfg.events)

	c.ctx, c.cancel = ctx, cancel

	repo.EXPECT().PreProcess(ctx, cfg.batchSize).Times(int(cfg.n)).Return([]model.PackEvent{}, nil)

	go c.lockDelivery()
	time.Sleep(3 * time.Second)

}

func TestConsumerDB(t *testing.T) {
	if err := config.ReadConfigYML("config.yml"); err != nil {
		panic(err)
	}

	cfg := config.GetConfigInstance()

	dsn := fmt.Sprintf("host=%v port=%v user=%v password=%v dbname=%v sslmode=%v",
		cfg.Database.Host,
		cfg.Database.Port,
		cfg.Database.User,
		cfg.Database.Password,
		cfg.Database.Name,
		cfg.Database.SslMode,
	)

	initCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := database.NewPostgres(initCtx, dsn, cfg.Database.Driver, &cfg.Database.Connections)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	cfgR := ConfigDB{
		// ChannelSize:   cfg.Kafka.Capacity,
		ConsumerCount: cfg.Retranslator.ConsumerCount,
		BatchSize:     cfg.Retranslator.BatchSize,
		// ProducerCount: cfg.Retranslator.ProducerCount,
		// WorkerCount:   cfg.Retranslator.WorkerCount,
		Repo: repo.NewEventRepo(db),
	}

	events := make(chan model.PackEvent)

	consumer := NewDbConsumer(
		cfgR.ConsumerCount,
		cfgR.BatchSize,
		cfgR.Ticker,
		cfgR.Repo,
		events)

	res, err := consumer.repo.PreProcess(initCtx, 3)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}
	fmt.Println("PreProcess test result:", res)

	res, err = consumer.repo.Lock(initCtx, 3)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}
	fmt.Println("Lock test result:", res)
}
