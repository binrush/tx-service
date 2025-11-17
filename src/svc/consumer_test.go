package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	internalpkg "github.com/binrush/tx-service/internal"
	"github.com/google/uuid"
	pgxmock "github.com/pashagolub/pgxmock/v3"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

type fetchResult struct {
	msg kafka.Message
	err error
}

type mockKafkaReader struct {
	config    kafka.ReaderConfig
	fetchCh   chan fetchResult
	commitCh  chan []kafka.Message
	commitErr error
	mu        sync.RWMutex
}

func newMockKafkaReader(config kafka.ReaderConfig) *mockKafkaReader {
	return &mockKafkaReader{
		config:   config,
		fetchCh:  make(chan fetchResult, 16),
		commitCh: make(chan []kafka.Message, 4),
	}
}

func (m *mockKafkaReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	select {
	case <-ctx.Done():
		return kafka.Message{}, ctx.Err()
	case res, ok := <-m.fetchCh:
		if !ok {
			return kafka.Message{}, context.Canceled
		}
		return res.msg, res.err
	}
}

func (m *mockKafkaReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	// copy to avoid data races on reused batch slice
	copied := make([]kafka.Message, len(msgs))
	copy(copied, msgs)
	m.commitCh <- copied
	m.mu.RLock()
	err := m.commitErr
	m.mu.RUnlock()
	return err
}

func (m *mockKafkaReader) Config() kafka.ReaderConfig {
	return m.config
}

func (m *mockKafkaReader) setCommitErr(err error) {
	m.mu.Lock()
	m.commitErr = err
	m.mu.Unlock()
}

func (m *mockKafkaReader) pushFetchResult(msg kafka.Message, err error) {
	m.fetchCh <- fetchResult{msg: msg, err: err}
}

func (m *mockKafkaReader) awaitCommit(t *testing.T) []kafka.Message {
	t.Helper()
	select {
	case msgs := <-m.commitCh:
		return msgs
	case <-time.After(time.Second):
		t.Fatal("expected commit to happen")
		return nil
	}
}

type mockKafkaWriter struct {
	writeCh chan []kafka.Message
}

func newMockKafkaWriter() *mockKafkaWriter {
	return &mockKafkaWriter{
		writeCh: make(chan []kafka.Message, 16),
	}
}

func (m *mockKafkaWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	copied := make([]kafka.Message, len(msgs))
	copy(copied, msgs)
	m.writeCh <- copied
	return nil
}

func (m *mockKafkaWriter) awaitWrite(t *testing.T) []kafka.Message {
	t.Helper()
	select {
	case msgs := <-m.writeCh:
		return msgs
	case <-time.After(time.Second):
		t.Fatal("expected DLQ write to happen")
		return nil
	}
}

func newTestConsumer(t *testing.T) (*Consumer, pgxmock.PgxPoolIface, *mockKafkaReader, *mockKafkaWriter) {
	t.Helper()

	mockPool, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)
	t.Cleanup(mockPool.Close)

	reader := newMockKafkaReader(kafka.ReaderConfig{Brokers: []string{"test-broker"}})
	dlqWriter := newMockKafkaWriter()

	return NewConsumer(mockPool, reader, dlqWriter), mockPool, reader, dlqWriter
}

func TestConsumerProcessesValidMessages(t *testing.T) {
	t.Parallel()

	consumer, mockPool, reader, _ := newTestConsumer(t)

	tx := internalpkg.Transaction{
		ID:              uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		UserId:          "user-1",
		TransactionType: internalpkg.TransactionTypeBet,
		Amount:          100,
		Timestamp:       time.Date(2024, time.March, 1, 10, 0, 0, 0, time.UTC),
	}
	payload, err := json.Marshal(tx)
	require.NoError(t, err)

	mockPool.ExpectBegin()
	mockPool.ExpectExec(insertTransactionQuery).
		WithArgs(tx.ID, tx.UserId, tx.TransactionType, int64(tx.Amount), tx.Timestamp).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mockPool.ExpectCommit()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		consumer.Start(ctx)
		close(done)
	}()

	reader.pushFetchResult(kafka.Message{Value: payload}, nil)
	reader.pushFetchResult(kafka.Message{}, context.DeadlineExceeded)

	committed := reader.awaitCommit(t)
	require.Len(t, committed, 1)
	require.Equal(t, payload, committed[0].Value)

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("consumer did not stop after context cancellation")
	}

	require.NoError(t, mockPool.ExpectationsWereMet())
}

func TestConsumerSkipsInvalidMessages(t *testing.T) {
	t.Parallel()

	consumer, mockPool, reader, dlqWriter := newTestConsumer(t)

	validTx := internalpkg.Transaction{
		ID:              uuid.MustParse("00000000-0000-0000-0000-000000000002"),
		UserId:          "user-2",
		TransactionType: internalpkg.TransactionTypeWin,
		Amount:          250,
		Timestamp:       time.Date(2024, time.March, 2, 11, 0, 0, 0, time.UTC),
	}

	payload, err := json.Marshal(validTx)
	require.NoError(t, err)

	mockPool.ExpectBegin()
	mockPool.ExpectExec(insertTransactionQuery).
		WithArgs(validTx.ID, validTx.UserId, validTx.TransactionType, int64(validTx.Amount), validTx.Timestamp).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mockPool.ExpectCommit()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		consumer.Start(ctx)
		close(done)
	}()

	invalidMsg := kafka.Message{Value: []byte("invalid-json")}
	reader.pushFetchResult(invalidMsg, nil)
	reader.pushFetchResult(kafka.Message{Value: payload}, nil)
	reader.pushFetchResult(kafka.Message{}, context.DeadlineExceeded)

	// Verify invalid message was sent to DLQ
	dlqMessages := dlqWriter.awaitWrite(t)
	require.Len(t, dlqMessages, 1)
	require.Equal(t, []byte("invalid-json"), dlqMessages[0].Value)

	committed := reader.awaitCommit(t)
	require.Len(t, committed, 2)
	require.Equal(t, []byte("invalid-json"), committed[0].Value)
	require.Equal(t, payload, committed[1].Value)

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("consumer did not stop after context cancellation")
	}

	require.NoError(t, mockPool.ExpectationsWereMet())
}

func TestConsumerFlushBatchHandlesBeginErrors(t *testing.T) {
	t.Parallel()

	consumer, mockPool, _, _ := newTestConsumer(t)

	mockPool.ExpectBegin().WillReturnError(errors.New("begin failed"))

	err := consumer.flushBatch(context.Background(), []kafka.Message{{Value: []byte(`{}`)}})
	require.EqualError(t, err, "begin failed")

	require.NoError(t, mockPool.ExpectationsWereMet())
}

func TestConsumerFlushBatchRollsBackOnExecError(t *testing.T) {
	t.Parallel()

	consumer, mockPool, _, _ := newTestConsumer(t)

	tx := internalpkg.Transaction{
		ID:              uuid.MustParse("00000000-0000-0000-0000-000000000003"),
		UserId:          "user-3",
		TransactionType: internalpkg.TransactionTypeBet,
		Amount:          50,
		Timestamp:       time.Date(2024, time.March, 3, 12, 0, 0, 0, time.UTC),
	}
	payload, err := json.Marshal(tx)
	require.NoError(t, err)

	mockPool.ExpectBegin()
	mockPool.ExpectExec(insertTransactionQuery).
		WithArgs(tx.ID, tx.UserId, tx.TransactionType, int64(tx.Amount), tx.Timestamp).
		WillReturnError(errors.New("insert failed"))
	mockPool.ExpectRollback()

	err = consumer.flushBatch(context.Background(), []kafka.Message{{Value: payload}})
	require.EqualError(t, err, "insert failed")

	require.NoError(t, mockPool.ExpectationsWereMet())
}

func TestConsumerRetriesOnFlushError(t *testing.T) {
	t.Parallel()

	consumer, mockPool, reader, _ := newTestConsumer(t)

	tx := internalpkg.Transaction{
		ID:              uuid.MustParse("00000000-0000-0000-0000-000000000004"),
		UserId:          "user-4",
		TransactionType: internalpkg.TransactionTypeBet,
		Amount:          100,
		Timestamp:       time.Date(2024, time.March, 4, 10, 0, 0, 0, time.UTC),
	}
	payload, err := json.Marshal(tx)
	require.NoError(t, err)

	// First attempt fails
	mockPool.ExpectBegin().WillReturnError(errors.New("db error"))
	// Second attempt succeeds
	mockPool.ExpectBegin()
	mockPool.ExpectExec(insertTransactionQuery).
		WithArgs(tx.ID, tx.UserId, tx.TransactionType, int64(tx.Amount), tx.Timestamp).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mockPool.ExpectCommit()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		consumer.Start(ctx)
		close(done)
	}()

	// Push message and trigger timeout to complete batch
	reader.pushFetchResult(kafka.Message{Value: payload}, nil)
	reader.pushFetchResult(kafka.Message{}, context.DeadlineExceeded)

	// After backoff (100ms), the consumer will retry with same batch
	// It will re-enter the fetch loop, so push another timeout
	time.Sleep(150 * time.Millisecond)
	reader.pushFetchResult(kafka.Message{}, context.DeadlineExceeded)

	// Wait for the commit
	committed := reader.awaitCommit(t)
	require.Len(t, committed, 1)

	cancel()
	<-done

	require.NoError(t, mockPool.ExpectationsWereMet())
}

func TestConsumerRetriesOnCommitError(t *testing.T) {
	t.Parallel()

	consumer, mockPool, reader, _ := newTestConsumer(t)

	tx := internalpkg.Transaction{
		ID:              uuid.MustParse("00000000-0000-0000-0000-000000000005"),
		UserId:          "user-5",
		TransactionType: internalpkg.TransactionTypeWin,
		Amount:          200,
		Timestamp:       time.Date(2024, time.March, 5, 11, 0, 0, 0, time.UTC),
	}
	payload, err := json.Marshal(tx)
	require.NoError(t, err)

	// First flush+commit succeeds in DB, but kafka commit fails
	mockPool.ExpectBegin()
	mockPool.ExpectExec(insertTransactionQuery).
		WithArgs(tx.ID, tx.UserId, tx.TransactionType, int64(tx.Amount), tx.Timestamp).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mockPool.ExpectCommit()

	// Retry: flush+commit again (idempotent insert via ON CONFLICT)
	mockPool.ExpectBegin()
	mockPool.ExpectExec(insertTransactionQuery).
		WithArgs(tx.ID, tx.UserId, tx.TransactionType, int64(tx.Amount), tx.Timestamp).
		WillReturnResult(pgxmock.NewResult("INSERT", 0))
	mockPool.ExpectCommit()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		consumer.Start(ctx)
		close(done)
	}()

	reader.setCommitErr(errors.New("commit failed"))
	reader.pushFetchResult(kafka.Message{Value: payload}, nil)
	reader.pushFetchResult(kafka.Message{}, context.DeadlineExceeded)

	// First commit attempt fails
	select {
	case <-reader.commitCh:
	case <-time.After(time.Second):
		t.Fatal("expected first commit attempt")
	}

	// Allow retry to succeed
	reader.setCommitErr(nil)

	// After backoff (100ms), consumer retries, so push timeout again
	time.Sleep(150 * time.Millisecond)
	reader.pushFetchResult(kafka.Message{}, context.DeadlineExceeded)

	// After backoff, retry succeeds
	committed := reader.awaitCommit(t)
	require.Len(t, committed, 1)

	cancel()
	<-done

	require.NoError(t, mockPool.ExpectationsWereMet())
}

func TestConsumerRetriesOnFetchError(t *testing.T) {
	t.Parallel()

	consumer, mockPool, reader, _ := newTestConsumer(t)

	tx := internalpkg.Transaction{
		ID:              uuid.MustParse("00000000-0000-0000-0000-000000000006"),
		UserId:          "user-6",
		TransactionType: internalpkg.TransactionTypeBet,
		Amount:          300,
		Timestamp:       time.Date(2024, time.March, 6, 12, 0, 0, 0, time.UTC),
	}
	payload, err := json.Marshal(tx)
	require.NoError(t, err)

	mockPool.ExpectBegin()
	mockPool.ExpectExec(insertTransactionQuery).
		WithArgs(tx.ID, tx.UserId, tx.TransactionType, int64(tx.Amount), tx.Timestamp).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mockPool.ExpectCommit()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		consumer.Start(ctx)
		close(done)
	}()

	// Fetch error, then success
	reader.pushFetchResult(kafka.Message{}, errors.New("fetch error"))
	reader.pushFetchResult(kafka.Message{Value: payload}, nil)
	reader.pushFetchResult(kafka.Message{}, context.DeadlineExceeded)

	committed := reader.awaitCommit(t)
	require.Len(t, committed, 1)

	cancel()
	<-done

	require.NoError(t, mockPool.ExpectationsWereMet())
}

func TestConsumerShutdownDuringBackoff(t *testing.T) {
	t.Parallel()

	consumer, mockPool, reader, _ := newTestConsumer(t)

	// Trigger flush error to enter backoff
	mockPool.ExpectBegin().WillReturnError(errors.New("db error"))

	tx := internalpkg.Transaction{
		ID:              uuid.MustParse("00000000-0000-0000-0000-000000000007"),
		UserId:          "user-7",
		TransactionType: internalpkg.TransactionTypeBet,
		Amount:          150,
		Timestamp:       time.Date(2024, time.March, 7, 13, 0, 0, 0, time.UTC),
	}
	payload, err := json.Marshal(tx)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		consumer.Start(ctx)
		close(done)
	}()

	reader.pushFetchResult(kafka.Message{Value: payload}, nil)
	reader.pushFetchResult(kafka.Message{}, context.DeadlineExceeded)

	// Give it time to hit the flush error and enter backoff
	time.Sleep(50 * time.Millisecond)

	// Cancel during backoff sleep
	cancel()

	select {
	case <-done:
		// Success - consumer stopped quickly during backoff
	case <-time.After(time.Second):
		t.Fatal("consumer did not stop quickly during backoff")
	}

	require.NoError(t, mockPool.ExpectationsWereMet())
}
