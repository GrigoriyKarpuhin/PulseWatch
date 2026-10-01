package db

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

func Open(ctx context.Context) (*sql.DB, error) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://pulsewatch:pulsewatch@localhost:5432/pulsewatch?sslmode=disable"
	}
	conn, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	conn.SetMaxOpenConns(10)
	conn.SetMaxIdleConns(5)
	for attempt := 0; attempt < 30; attempt++ {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err = conn.PingContext(pingCtx)
		cancel()
		if err == nil {
			return conn, nil
		}
		log.Printf("waiting for postgres: %v", err)
		select {
		case <-ctx.Done():
			conn.Close()
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	conn.Close()
	return nil, err
}

func Migrate(ctx context.Context, conn *sql.DB) error {
	if _, err := conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (name TEXT PRIMARY KEY)`); err != nil {
		return err
	}
	files, err := migrationFiles.ReadDir("migrations")
	if err != nil {
		return err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Name() < files[j].Name() })
	for _, file := range files {
		name := file.Name()
		if file.IsDir() {
			continue
		}
		var exists bool
		if err := conn.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE name=$1)`, name).Scan(&exists); err != nil {
			return err
		}
		if exists {
			continue
		}
		body, err := migrationFiles.ReadFile("migrations/" + name)
		if err != nil {
			return err
		}
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, string(body)); err == nil {
			_, err = tx.ExecContext(ctx, `INSERT INTO schema_migrations(name) VALUES($1)`, name)
		}
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("migration %s: %w", name, err)
		}
		if err = tx.Commit(); err != nil {
			return err
		}
		log.Printf("applied migration %s", name)
	}
	return nil
}

func InsertOutbox(ctx context.Context, tx *sql.Tx, id, topic string, monitorID int64, payload []byte) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO outbox(event_id,topic,monitor_id,payload) VALUES($1,$2,$3,$4) ON CONFLICT(event_id) DO NOTHING`, id, topic, monitorID, payload)
	return err
}

func RecordDeadLetter(ctx context.Context, conn *sql.DB, topic string, partition int, offset int64, payload []byte, reason error) error {
	if reason == nil {
		return errors.New("dead letter reason is required")
	}
	_, err := conn.ExecContext(ctx, `INSERT INTO dead_letters(topic,partition_number,offset_number,payload,reason) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, topic, partition, offset, payload, reason.Error())
	return err
}
