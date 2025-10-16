.PHONY: go-run
run:
	go run cmd/main.go

.PHONY: go-build
go-build: .go-build

.go-build:
	go build \
		-tags='no_mysql no_sqlite3' \
		-ldflags=" \
			-X 'github.com/$(SERVICE_PATH)/internal/config.version=$(VERSION)' \
			-X 'github.com/$(SERVICE_PATH)/internal/config.commitHash=$(COMMIT_HASH)' \
		" \
		-o ./bin/retranslator$(shell go env GOEXE) ./cmd/main.go

.PHONY: docker-build
docker-build:
	docker build . -t stormbeaver/logistic-pack-retranslator

.PHONY: docker-push
docker-push:
	docker push stormbeaver/logistic-pack-retranslator