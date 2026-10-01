package run

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"os/signal"
	"syscall"

	"pulsewatch/internal/db"
)

func Service(name string, fn func(context.Context, *sql.DB) error) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	conn, err := db.Open(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	log.Printf("starting %s", name)
	if err := fn(ctx, conn); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}
