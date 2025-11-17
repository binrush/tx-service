package internal

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTransactionFromJSON_Valid(t *testing.T) {
	testCases := []struct {
		name            string
		id              string
		userId          string
		transactionType string
		amount          uint32
		expectedType    string
	}{
		{
			name:            "valid_bet_transaction",
			id:              "550e8400-e29b-41d4-a716-446655440000",
			userId:          "user123",
			transactionType: "bet",
			amount:          100,
			expectedType:    "bet",
		},
		{
			name:            "valid_win_transaction",
			id:              "650e8400-e29b-41d4-a716-446655440001",
			userId:          "user456",
			transactionType: "win",
			amount:          200,
			expectedType:    "win",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			jsonData := fmt.Sprintf(`{
				"id": "%s",
				"user_id": "%s",
				"transaction_type": "%s",
				"amount": %d,
				"timestamp": "2025-11-17T10:00:00Z"
			}`, tc.id, tc.userId, tc.transactionType, tc.amount)

			tx, err := NewTransactionFromJSON([]byte(jsonData))
			require.NoError(t, err)
			require.NotNil(t, tx)

			assert.Equal(t, tc.id, tx.ID.String())
			assert.Equal(t, tc.userId, tx.UserId)
			assert.Equal(t, tc.expectedType, tx.TransactionType)
			assert.Equal(t, tc.amount, tx.Amount)
			assert.False(t, tx.Timestamp.IsZero())
		})
	}
}

func TestNewTransactionFromJSON_Invalid(t *testing.T) {
	testCases := []struct {
		name          string
		jsonData      string
		expectedError string
	}{
		{
			name:          "invalid_json",
			jsonData:      `{"invalid json`,
			expectedError: "",
		},
		{
			name: "empty_user_id",
			jsonData: `{
				"id": "550e8400-e29b-41d4-a716-446655440000",
				"user_id": "",
				"transaction_type": "bet",
				"amount": 100,
				"timestamp": "2025-11-17T10:00:00Z"
			}`,
			expectedError: "user_id cannot be empty",
		},
		{
			name: "whitespace_user_id",
			jsonData: `{
				"id": "550e8400-e29b-41d4-a716-446655440000",
				"user_id": "   ",
				"transaction_type": "bet",
				"amount": 100,
				"timestamp": "2025-11-17T10:00:00Z"
			}`,
			expectedError: "user_id cannot be empty",
		},
		{
			name: "empty_transaction_type",
			jsonData: `{
				"id": "550e8400-e29b-41d4-a716-446655440000",
				"user_id": "user123",
				"transaction_type": "",
				"amount": 100,
				"timestamp": "2025-11-17T10:00:00Z"
			}`,
			expectedError: "transaction_type must be either 'win' or 'bet'",
		},
		{
			name: "invalid_transaction_type_deposit",
			jsonData: `{
				"id": "550e8400-e29b-41d4-a716-446655440000",
				"user_id": "user123",
				"transaction_type": "deposit",
				"amount": 100,
				"timestamp": "2025-11-17T10:00:00Z"
			}`,
			expectedError: "transaction_type must be either 'win' or 'bet'",
		},
		{
			name: "zero_amount",
			jsonData: `{
				"id": "550e8400-e29b-41d4-a716-446655440000",
				"user_id": "user123",
				"transaction_type": "bet",
				"amount": 0,
				"timestamp": "2025-11-17T10:00:00Z"
			}`,
			expectedError: "amount must be positive",
		},
		{
			name: "missing_timestamp",
			jsonData: `{
				"id": "550e8400-e29b-41d4-a716-446655440000",
				"user_id": "user123",
				"transaction_type": "bet",
				"amount": 100
			}`,
			expectedError: "timestamp cannot be empty",
		},
		{
			name: "invalid_timestamp",
			jsonData: `{
				"id": "550e8400-e29b-41d4-a716-446655440000",
				"user_id": "user123",
				"transaction_type": "bet",
				"amount": 100,
				"timestamp": "2025"
			}`,
			expectedError: "",
		},
		{
			name: "nil_uuid",
			jsonData: `{
				"id": "00000000-0000-0000-0000-000000000000",
				"user_id": "user123",
				"transaction_type": "bet",
				"amount": 100,
				"timestamp": "2025-11-17T10:00:00Z"
			}`,
			expectedError: "id cannot be empty and must be a valid UUID",
		},
		{
			name: "missing_id",
			jsonData: `{
				"user_id": "user123",
				"transaction_type": "bet",
				"amount": 100,
				"timestamp": "2025-11-17T10:00:00Z"
			}`,
			expectedError: "id cannot be empty and must be a valid UUID",
		},
		{
			name: "invalid_uuid_format",
			jsonData: `{
				"id": "not-a-valid-uuid",
				"user_id": "user123",
				"transaction_type": "bet",
				"amount": 100,
				"timestamp": "2025-11-17T10:00:00Z"
			}`,
			expectedError: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tx, err := NewTransactionFromJSON([]byte(tc.jsonData))
			assert.Error(t, err)
			assert.Nil(t, tx)
			if tc.expectedError != "" {
				assert.Contains(t, err.Error(), tc.expectedError)
			}
		})
	}
}
