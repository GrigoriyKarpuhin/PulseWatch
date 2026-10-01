package events

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"pulsewatch/internal/db"
)

const (
	ProbeJobs      = "probe-jobs"
	CheckResults   = "check-results"
	IncidentEvents = "incident-events"
)

func ID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes[:]), nil
}

func Brokers() []string {
	value := os.Getenv("KAFKA_BROKERS")
	if value == "" {
		value = "kafka:19092"
	}
	return strings.Split(value, ",")
}

func Enqueue(ctx context.Context, tx *sql.Tx, topic, id string, monitorID int64, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return db.InsertOutbox(ctx, tx, id, topic, monitorID, payload)
}

func Consume(ctx context.Context, conn *sql.DB, topic, group string, handler func(context.Context, []byte) error) error {
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: Brokers(), Topic: topic, GroupID: group, StartOffset: kafka.FirstOffset, MinBytes: 1, MaxBytes: 1e6, MaxWait: time.Second})
	defer reader.Close()
	for ctx.Err() == nil {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("%s fetch: %v", topic, err)
			time.Sleep(time.Second)
			continue
		}
		for ctx.Err() == nil {
			err = handler(ctx, message.Value)
			if err == nil {
				break
			}
			var poison *PoisonError
			if errors.As(err, &poison) {
				if saveErr := db.RecordDeadLetter(ctx, conn, topic, message.Partition, message.Offset, message.Value, err); saveErr == nil {
					log.Printf("dead letter %s/%d/%d: %v", topic, message.Partition, message.Offset, err)
					break
				}
			}
			log.Printf("%s handler retry: %v", topic, err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
			}
		}
		if ctx.Err() != nil {
			return nil
		}
		if err := reader.CommitMessages(ctx, message); err != nil {
			log.Printf("%s offset commit: %v", topic, err)
		}
	}
	return nil
}

type PoisonError struct{ Err error }

func (e *PoisonError) Error() string { return fmt.Sprintf("invalid event: %v", e.Err) }
func (e *PoisonError) Unwrap() error { return e.Err }

func Decode(data []byte, value any) error {
	if err := json.Unmarshal(data, value); err != nil {
		return &PoisonError{Err: err}
	}
	return nil
}

func Key(monitorID int64) []byte { return []byte(strconv.FormatInt(monitorID, 10)) }
