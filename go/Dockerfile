FROM golang:1.25-alpine@sha256:1ae0735f00daffa3aaf1363a5184c0d2dc55c78e3db4ec70241cdac97bf84b59 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN for attempt in 1 2 3; do go mod download && exit 0; sleep 3; done; exit 1
COPY . .
RUN mkdir /out && for service in api scheduler prober projector publisher notifier migrate; do \
      CGO_ENABLED=0 go build -trimpath -o /out/$service ./cmd/$service || exit 1; \
    done

FROM alpine:3.21@sha256:ce64758a109eb420d874a118f87920e625e12d3634e03b4a5573fd9f6e5d3507
RUN adduser -D -u 10001 app
USER app
COPY --from=build /out/ /bin/
