# Builder
FROM golang:1.24.1-alpine AS builder

ARG GITHUB_PATH=github.com/StormBeaver/logistic-pack-retranslator

WORKDIR /home/${GITHUB_PATH}

RUN apk add --update make git
COPY Makefile Makefile
COPY . .
RUN make go-build

# retranslator

FROM alpine:latest AS retranslator

ARG GITHUB_PATH=github.com/StormBeaver/logistic-pack-retranslator

LABEL org.opencontainers.image.source=https://${GITHUB_PATH}
RUN apk --no-cache add ca-certificates
WORKDIR /root/

COPY --from=builder /home/${GITHUB_PATH}/bin/retranslator .
COPY --from=builder /home/${GITHUB_PATH}/config.yml .

RUN chown root:root retranslator

EXPOSE 9101

CMD ["./retranslator"]
