package retranslator

import (
	"context"
	"testing"
	"time"

	"github.com/stormbeaver/logistic-pack-retranslator/internal/mocks"

	"github.com/golang/mock/gomock"
)

func TestStart(t *testing.T) {

	ctrl := gomock.NewController(t)
	repo := mocks.NewMockEventRepo(ctrl)
	sender := mocks.NewMockEventSender(ctrl)
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	repo.EXPECT().PreProcess(ctx, gomock.Any()).AnyTimes()
	repo.EXPECT().Lock(ctx, gomock.Any()).AnyTimes()

	cfg := Config{
		ChannelSize:   512,
		ConsumerCount: 2,
		BatchSize:     10,
		Ticker:        10 * time.Second,
		ProducerCount: 2,
		WorkerCount:   2,
		Repo:          repo,
		Sender:        sender,
	}

	retranslator := NewRetranslator(cfg)
	retranslator.Start()

	retranslator.Close()
}
