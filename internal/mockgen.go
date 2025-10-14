package internal

//go:generate mockgen -destination=./internal/mocks/repo_mock.go -package=mocks github.com/StormBeaver/logistic-pack-api/internal/app/repo EventRepo
//go:generate mockgen -destination=./internal/mocks/sender_mock.go -package=mocks github.com/StormBeaver/logistic-pack-api/internal/app/sender EventSender
