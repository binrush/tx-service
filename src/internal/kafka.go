package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

const (
	DefaultBroker      = "localhost:9092"
	kafkaBrokerVars    = "KAFKA_BROKERS"
	TransactionsTopic  = "transactions"
	defaultGeneratedID = "user-%s"
)

type ProducerParams struct {
	ID              uuid.UUID
	UserID          string
	TransactionType string
	Amount          uint32
	Timestamp       time.Time
	Count           int
}

type KafkaProducer struct {
	writer *kafka.Writer
}

func NewKafkaProducer(brokers []string) *KafkaProducer {
	return &KafkaProducer{
		writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers: brokers,
			Topic:   TransactionsTopic,
		}),
	}
}

func (kp *KafkaProducer) Close() error {
	if kp == nil || kp.writer == nil {
		return nil
	}
	return kp.writer.Close()
}

func (kp *KafkaProducer) GenerateTransactions(ctx context.Context, params ProducerParams) ([]Transaction, error) {
	if kp == nil || kp.writer == nil {
		return nil, fmt.Errorf("kafka producer not initialized")
	}

	count := params.Count
	if count <= 0 {
		count = 1
	}

	switch params.TransactionType {
	case "", TransactionTypeBet, TransactionTypeWin:
		break
	default:
		return nil, fmt.Errorf("invalid transaction type %q", params.TransactionType)
	}

	transactions := make([]Transaction, 0, count)
	for range make([]struct{}, count) {
		tx := Transaction{
			ID:              resolveTransactionID(params.ID),
			UserId:          resolveUserID(params.UserID),
			TransactionType: resolveTransactionType(params.TransactionType),
			Amount:          resolveAmount(params.Amount),
			Timestamp:       resolveTimestamp(params.Timestamp),
		}
		transactions = append(transactions, tx)
	}

	if err := kp.PublishTransactions(ctx, transactions); err != nil {
		return nil, err
	}

	return transactions, nil
}

func (kp *KafkaProducer) PublishTransactions(ctx context.Context, transactions []Transaction) error {
	if kp == nil || kp.writer == nil {
		return fmt.Errorf("kafka producer not initialized")
	}
	if len(transactions) == 0 {
		return nil
	}

	messages := make([]kafka.Message, 0, len(transactions))
	for _, tx := range transactions {
		payload, err := json.Marshal(tx)
		if err != nil {
			return fmt.Errorf("marshal transaction: %w", err)
		}

		messages = append(messages, kafka.Message{
			Key:   []byte(tx.UserId),
			Value: payload,
		})
	}

	if err := kp.writer.WriteMessages(ctx, messages...); err != nil {
		return fmt.Errorf("write kafka messages: %w", err)
	}

	return nil
}

func resolveTransactionID(id uuid.UUID) uuid.UUID {
	if id != uuid.Nil {
		return id
	}
	return uuid.New()
}

func resolveUserID(userID string) string {
	if trimmed := strings.TrimSpace(userID); trimmed != "" {
		return trimmed
	}
	return fmt.Sprintf(defaultGeneratedID, uuid.NewString())
}

func resolveTransactionType(txType string) string {
	if txType == "" {
		return []string{TransactionTypeBet, TransactionTypeWin}[rand.IntN(2)]
	}
	return txType
}

func resolveAmount(amount uint32) uint32 {
	if amount > 0 {
		return amount
	}
	return uint32(rand.IntN(1000) + 1)
}

func resolveTimestamp(timestamp time.Time) time.Time {
	if timestamp.IsZero() {
		return time.Now().UTC()
	}
	return timestamp
}

func Brokers() []string {
	brokers := strings.TrimSpace(os.Getenv(kafkaBrokerVars))
	if brokers == "" {
		return []string{DefaultBroker}
	}

	var result []string
	for _, entry := range strings.Split(brokers, ",") {
		if trimmed := strings.TrimSpace(entry); trimmed != "" {
			result = append(result, trimmed)
		}
	}

	if len(result) == 0 {
		return []string{DefaultBroker}
	}

	return result
}
