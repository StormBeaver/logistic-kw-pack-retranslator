package producer

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stormbeaver/logistic-pack-retranslator/internal/mocks"
	model "github.com/stormbeaver/logistic-pack-retranslator/internal/repo"

	"github.com/gammazero/workerpool"
	"github.com/golang/mock/gomock"
)

var _ gomock.Matcher = &myMatcher{}

type myMatcher struct {
	waitingId uint64
}

func (m *myMatcher) Matches(x any) bool {
	event, ok := x.(*model.PackEvent)
	if !ok {
		return false
	}

	if event.ID == m.waitingId {
		atomic.AddUint64(&m.waitingId, 1)
		return true
	}

	return false
}
func (m *myMatcher) String() string { return fmt.Sprintf("Waiting for %d", m.waitingId) }

var (
	events     = make(chan model.PackEvent, 512)
	err        = errors.New("planned Error")
	workerPool = workerpool.New(2)
	slc        = []model.PackEvent{
		{ID: 1},
		{ID: 2},
		{ID: 3},
		{ID: 4},
		{ID: 5},
		{ID: 6},
	}
	ctx, _ = context.WithTimeout(context.Background(), 10*time.Second)
)

func TestProducerStart(t *testing.T) {

	var (
		mtchr  = &myMatcher{waitingId: 1}
		ctrl   = gomock.NewController(t)
		repo   = mocks.NewMockEventRepo(ctrl)
		sender = mocks.NewMockEventSender(ctrl)
		tick   = time.Second * 2
	)

	defer ctrl.Finish()

	sender.EXPECT().Send(mtchr).Return(nil).Times(6)
	repo.EXPECT().Remove(ctx, gomock.InAnyOrder([]uint64{1, 2, 3, 4, 5, 6})).Return(err)
	repo.EXPECT().Unlock(ctx, gomock.InAnyOrder([]uint64{1, 2, 3, 4, 5, 6})).Return(err)

	p := NewKafkaProducer(
		2,
		sender,
		events,
		workerPool,
		repo,
		tick,
		10,
	)

	p.Start()
	for _, v := range slc {
		events <- v
	}

	time.Sleep(3 * time.Second)
	p.Close()
}

func TestProducerStartWithErr(t *testing.T) {

	var (
		ctrl   = gomock.NewController(t)
		repo   = mocks.NewMockEventRepo(ctrl)
		sender = mocks.NewMockEventSender(ctrl)
		tick   = time.Second * 2
		mtchr2 = &myMatcher{waitingId: 1}
	)

	defer ctrl.Finish()

	sender.EXPECT().Send(mtchr2).Return(err).Times(len(slc))
	repo.EXPECT().Unlock(ctx, gomock.InAnyOrder([]uint64{1, 2, 3, 4, 5, 6}))

	p := NewKafkaProducer(
		2,
		sender,
		events,
		workerPool,
		repo,
		tick,
		10,
	)

	p.Start()
	for _, v := range slc {
		events <- v
	}

	time.Sleep(3 * time.Second)
	p.Close()
}
