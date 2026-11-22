# syntax=docker/dockerfile:1
FROM golang:1.25.5-bookworm@sha256:d9132cce84391efab786495288756d60e1da215b1f94e87860aeefc3d4c45b6d AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download
COPY *.go *.s ./
COPY internal/native/ ./internal/native/
RUN CGO_ENABLED=1 CGO_CXXFLAGS_ALLOW="-m(avx2|popcnt|bmi)" GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -tags=nativecsv,netgo,osusergo -trimpath -ldflags="-linkmode=external -extldflags=-static -s -w" -o /shopping-sessions .

FROM scratch
COPY --from=build /shopping-sessions /shopping-sessions
USER 1000:1000
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch HOME=/scratch NCPU=8 MEM_GB=24 GOGC=200
ENTRYPOINT ["/shopping-sessions"]
