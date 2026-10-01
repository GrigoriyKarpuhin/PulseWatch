package publisher

import (
	"context"
	"database/sql"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"pulsewatch/internal/events"
)

func Run(ctx context.Context, conn *sql.DB) error {
	writer := &kafka.Writer{Addr: kafka.TCP(events.Brokers()...), RequiredAcks: kafka.RequireAll, WriteTimeout: 5 * time.Second, BatchTimeout: 10 * time.Millisecond}
	defer writer.Close()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := publishBatch(ctx, conn, writer); err != nil && ctx.Err() == nil {
			log.Printf("outbox: %v", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func publishBatch(ctx context.Context, conn *sql.DB, writer *kafka.Writer) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id,topic,monitor_id,payload FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT 20 FOR UPDATE SKIP LOCKED`)
	if err != nil {
		return err
	}
	type item struct {
		id, monitorID int64
		topic         string
		payload       []byte
	}
	var items []item
	for rows.Next() {
		var x item
		if err := rows.Scan(&x.id, &x.topic, &x.monitorID, &x.payload); err != nil {
			rows.Close()
			return err
		}
		items = append(items, x)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, x := range items {
		if err := writer.WriteMessages(ctx, kafka.Message{Topic: x.topic, Key: events.Key(x.monitorID), Value: x.payload}); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE outbox SET published_at=now() WHERE id=$1`, x.id); err != nil {
			return err
		}
	}
	return tx.Commit()
}
