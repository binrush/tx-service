package internal

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	TransactionTypeWin = "win"
	TransactionTypeBet = "bet"
)

type Transaction struct {
	ID              uuid.UUID `json:"id"`
	UserId          string    `json:"user_id"`
	TransactionType string    `json:"transaction_type"`
	Amount          uint32    `json:"amount"`
	Timestamp       time.Time `json:"timestamp"`
}

// NewTransactionFromJSON creates a Transaction from JSON data with validation
func NewTransactionFromJSON(data []byte) (*Transaction, error) {
	var tx Transaction
	if err := json.Unmarshal(data, &tx); err != nil {
		return nil, err
	}

	if strings.TrimSpace(tx.UserId) == "" {
		return nil, errors.New("user_id cannot be empty")
	}

	if tx.TransactionType != TransactionTypeWin && tx.TransactionType != TransactionTypeBet {
		return nil, errors.New("transaction_type must be either 'win' or 'bet'")
	}

	if tx.Amount == 0 {
		return nil, errors.New("amount must be positive")
	}

	if tx.Timestamp.IsZero() {
		return nil, errors.New("timestamp cannot be empty")
	}

	if tx.ID == uuid.Nil {
		return nil, errors.New("id cannot be empty and must be a valid UUID")
	}

	return &tx, nil
}
