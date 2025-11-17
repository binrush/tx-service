package main

import (
	"context"
	"errors"
	"log"
	"time"

	internalpkg "github.com/binrush/tx-service/internal"
	"github.com/jackc/pgx/v5"
	"github.com/segmentio/kafka-go"
)

const (
	batchSize              = 10
	batchFlushInterval     = 2 * time.Second
	insertTransactionQuery = `
		INSERT INTO transactions (id, user_id, transaction_type, amount, ts)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (id) DO NOTHING
	`
)

type transactionalPool interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

type kafkaReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Config() kafka.ReaderConfig
}

type kafkaWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

type Consumer struct {
	pool      transactionalPool
	reader    kafkaReader
	dlqWriter kafkaWriter
}

func NewConsumer(pool transactionalPool, reader kafkaReader, dlqWriter kafkaWriter) *Consumer {
	return &Consumer{
		pool:      pool,
		reader:    reader,
		dlqWriter: dlqWriter,
	}
}

// backoff implements exponential backoff with a maximum delay.
type backoff struct {
	current time.Duration
	initial time.Duration
	max     time.Duration
}

func newBackoff(initial, max time.Duration) *backoff {
	return &backoff{
		current: initial,
		initial: initial,
		max:     max,
	}
}

// sleep sleeps for the current backoff duration or returns early if context is cancelled.
// On successful sleep, it doubles the backoff duration (up to max).
// Returns true if sleep completed normally, false if context was cancelled.
func (b *backoff) sleep(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(b.current):
		b.current = min(b.current*2, b.max)
		return true
	}
}

// reset resets the backoff to its initial duration.
func (b *backoff) reset() {
	b.current = b.initial
}

func (c *Consumer) Start(ctx context.Context) {
	log.Printf("Kafka consumer started (brokers=%v)", c.reader.Config().Brokers)
	defer log.Println("Kafka consumer shut down")

	fetchBackoff := newBackoff(100*time.Millisecond, 5*time.Second)
	flushBackoff := newBackoff(100*time.Millisecond, 5*time.Second)
	commitBackoff := newBackoff(100*time.Millisecond, 5*time.Second)

	batch := make([]kafka.Message, 0, batchSize)
	for {
		fetchCtx, cancel := context.WithTimeout(ctx, batchFlushInterval)
		for len(batch) < batchSize {
			msg, err := c.reader.FetchMessage(fetchCtx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					cancel()
					return
				}
				if errors.Is(err, context.DeadlineExceeded) {
					break
				}
				log.Printf("failed to fetch message: %v", err)

				if !fetchBackoff.sleep(ctx) {
					cancel()
					return
				}
				continue
			}
			fetchBackoff.reset()
			batch = append(batch, msg)
		}
		cancel()

		if len(batch) == 0 {
			continue
		}

		if err := c.flushBatch(ctx, batch); err != nil {
			log.Printf("failed to flush batch: %v", err)
			if !flushBackoff.sleep(ctx) {
				return
			}
			continue
		}
		flushBackoff.reset()
		if err := c.reader.CommitMessages(ctx, batch...); err != nil {
			log.Printf("failed to commit messages: %v", err)
			if !commitBackoff.sleep(ctx) {
				return
			}
			continue
		}
		commitBackoff.reset()
		batch = batch[:0]
	}
}

func (c *Consumer) flushBatch(ctx context.Context, batch []kafka.Message) error {
	if len(batch) == 0 {
		return nil
	}

	pgTx, err := c.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if pgTx != nil {
			_ = pgTx.Rollback(ctx)
		}
	}()

	for _, msg := range batch {
		tx, err := internalpkg.NewTransactionFromJSON(msg.Value)
		if err != nil {
			log.Printf("invalid message sent to DLQ (%v): %s", err, string(msg.Value))
			dlqMsg := kafka.Message{
				Key:   msg.Key,
				Value: msg.Value,
			}
			if dlqErr := c.dlqWriter.WriteMessages(ctx, dlqMsg); dlqErr != nil {
				log.Printf("failed to write to DLQ: %v", dlqErr)
			}
			continue
		}
		if _, err := pgTx.Exec(ctx, insertTransactionQuery, tx.ID, tx.UserId, tx.TransactionType, int64(tx.Amount), tx.Timestamp); err != nil {
			return err
		}
	}

	if err := pgTx.Commit(ctx); err != nil {
		return err
	}

	pgTx = nil
	return nil
}
