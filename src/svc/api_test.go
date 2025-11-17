package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	internalpkg "github.com/binrush/tx-service/internal"
	"github.com/google/uuid"
	pgxmock "github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestTransactionAPI(t *testing.T) (*TransactionApi, pgxmock.PgxPoolIface) {
	t.Helper()

	mockPool, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("failed to create pgx mock: %v", err)
	}

	api := NewTransactionApi(context.Background(), mockPool)
	t.Cleanup(func() {
		mockPool.Close()
	})

	return api, mockPool
}

func TestTransactionsAPI(t *testing.T) {
	t.Parallel()

	// using single response for all cases because we use mock database
	mockTx := internalpkg.Transaction{
		ID:              uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		UserId:          "user-1",
		TransactionType: internalpkg.TransactionTypeBet,
		Amount:          500,
		Timestamp:       time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC),
	}

	cases := []struct {
		name              string
		queryString       string
		expectedCountQ    string
		expectedCountArgs []any
		expectedQuery     string
		expectedArgs      []any
		expectedStatus    int
		expectResponse    bool
		expectedResponse  TransactionsResponse
		expectedErrorMsg  string
	}{
		{
			name:              "with user_id and transaction_type",
			queryString:       "?user_id=user-1&transaction_type=bet",
			expectedCountQ:    "SELECT COUNT(*) FROM transactions WHERE user_id = $1 AND transaction_type = $2",
			expectedCountArgs: []any{"user-1", internalpkg.TransactionTypeBet},
			expectedQuery:     "SELECT id, user_id, transaction_type, amount, ts FROM transactions WHERE user_id = $1 AND transaction_type = $2 ORDER BY ts ASC LIMIT $3 OFFSET $4",
			expectedArgs:      []any{"user-1", internalpkg.TransactionTypeBet, limitDefault, 0},
			expectedStatus:    http.StatusOK,
			expectResponse:    true,
			expectedResponse: TransactionsResponse{
				Items:  []internalpkg.Transaction{mockTx},
				Offset: 0,
				Limit:  limitDefault,
				Total:  1,
			},
		},
		{
			name:              "without filters",
			queryString:       "",
			expectedCountQ:    "SELECT COUNT(*) FROM transactions",
			expectedCountArgs: []any{},
			expectedQuery:     "SELECT id, user_id, transaction_type, amount, ts FROM transactions ORDER BY ts ASC LIMIT $1 OFFSET $2",
			expectedArgs:      []any{limitDefault, 0},
			expectedStatus:    http.StatusOK,
			expectResponse:    true,
			expectedResponse: TransactionsResponse{
				Items:  []internalpkg.Transaction{mockTx},
				Offset: 0,
				Limit:  limitDefault,
				Total:  1,
			},
		},
		{
			name:              "with user_id and offset",
			queryString:       "?user_id=user-99&offset=10",
			expectedCountQ:    "SELECT COUNT(*) FROM transactions WHERE user_id = $1",
			expectedCountArgs: []any{"user-99"},
			expectedQuery:     "SELECT id, user_id, transaction_type, amount, ts FROM transactions WHERE user_id = $1 ORDER BY ts ASC LIMIT $2 OFFSET $3",
			expectedArgs:      []any{"user-99", limitDefault, 10},
			expectedStatus:    http.StatusOK,
			expectResponse:    true,
			expectedResponse: TransactionsResponse{
				Items:  []internalpkg.Transaction{mockTx},
				Offset: 10,
				Limit:  limitDefault,
				Total:  1,
			},
		},
		{
			name:              "with explicit limit",
			queryString:       "?user_id=user-1&limit=50",
			expectedCountQ:    "SELECT COUNT(*) FROM transactions WHERE user_id = $1",
			expectedCountArgs: []any{"user-1"},
			expectedQuery:     "SELECT id, user_id, transaction_type, amount, ts FROM transactions WHERE user_id = $1 ORDER BY ts ASC LIMIT $2 OFFSET $3",
			expectedArgs:      []any{"user-1", 50, 0},
			expectedStatus:    http.StatusOK,
			expectResponse:    true,
			expectedResponse: TransactionsResponse{
				Items:  []internalpkg.Transaction{mockTx},
				Offset: 0,
				Limit:  50,
				Total:  1,
			},
		},
		{
			name:             "invalid transaction_type",
			queryString:      "?transaction_type=foo",
			expectedStatus:   http.StatusBadRequest,
			expectedErrorMsg: "transaction_type must be either win or bet",
		},
		{
			name:             "invalid offset",
			queryString:      "?offset=abc",
			expectedStatus:   http.StatusBadRequest,
			expectedErrorMsg: `invalid offset: strconv.Atoi: parsing "abc": invalid syntax`,
		},
		{
			name:             "invalid limit",
			queryString:      "?limit=xyz",
			expectedStatus:   http.StatusBadRequest,
			expectedErrorMsg: `invalid limit: strconv.Atoi: parsing "xyz": invalid syntax`,
		},
		{
			name:             "negative offset",
			queryString:      "?offset=-1",
			expectedStatus:   http.StatusBadRequest,
			expectedErrorMsg: "offset must be non-negative",
		},
		{
			name:             "zero limit",
			queryString:      "?limit=0",
			expectedStatus:   http.StatusBadRequest,
			expectedErrorMsg: "limit must be positive",
		},
		{
			name:             "negative limit",
			queryString:      "?limit=-10",
			expectedStatus:   http.StatusBadRequest,
			expectedErrorMsg: "limit must be positive",
		},
		{
			name:             "limit exceeds maximum",
			queryString:      "?limit=10000",
			expectedStatus:   http.StatusBadRequest,
			expectedErrorMsg: "limit must not exceed 1000",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert := assert.New(t)
			api, mockPool := newTestTransactionAPI(t)

			if tc.expectedQuery != "" {
				// Mock COUNT query
				countRows := pgxmock.NewRows([]string{"count"}).
					AddRow(tc.expectedResponse.Total)
				mockPool.ExpectQuery(tc.expectedCountQ).
					WithArgs(tc.expectedCountArgs...).
					WillReturnRows(countRows)

				// Mock SELECT query
				rows := pgxmock.NewRows([]string{"id", "user_id", "transaction_type", "amount", "ts"})
				for _, item := range tc.expectedResponse.Items {
					rows.AddRow(item.ID, item.UserId, item.TransactionType, item.Amount, item.Timestamp)
				}

				mockPool.ExpectQuery(tc.expectedQuery).
					WithArgs(tc.expectedArgs...).
					WillReturnRows(rows)
			}

			req := httptest.NewRequest(http.MethodGet, "/transactions"+tc.queryString, nil)
			rec := httptest.NewRecorder()

			api.transactionsHandler(rec, req)

			assert.Equal(tc.expectedStatus, rec.Code)

			if tc.expectResponse {
				var resp TransactionsResponse
				if !assert.NoError(json.NewDecoder(rec.Body).Decode(&resp)) {
					return
				}
				assert.Equal(tc.expectedResponse, resp)
			} else {
				var errResp errorResponse
				if !assert.NoError(json.NewDecoder(rec.Body).Decode(&errResp)) {
					return
				}
				assert.Equal(tc.expectedErrorMsg, errResp.Error)
			}

			assert.NoError(mockPool.ExpectationsWereMet())
		})
	}
}

func TestTransactionsAPICountQueryError(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	api, mockPool := newTestTransactionAPI(t)

	expectedCountQuery := "SELECT COUNT(*) FROM transactions"
	mockPool.ExpectQuery(expectedCountQuery).
		WithArgs().
		WillReturnError(errors.New("database error"))

	req := httptest.NewRequest(http.MethodGet, "/transactions", nil)
	rec := httptest.NewRecorder()

	api.transactionsHandler(rec, req)

	assert.Equal(http.StatusInternalServerError, rec.Code)

	var errResp errorResponse
	if assert.NoError(json.NewDecoder(rec.Body).Decode(&errResp)) {
		assert.Equal(errInternalMessage, errResp.Error)
	}

	assert.NoError(mockPool.ExpectationsWereMet())
}

func TestTransactionsAPISelectQueryError(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	api, mockPool := newTestTransactionAPI(t)

	expectedCountQuery := "SELECT COUNT(*) FROM transactions"
	countRows := pgxmock.NewRows([]string{"count"}).AddRow(10)
	mockPool.ExpectQuery(expectedCountQuery).
		WithArgs().
		WillReturnRows(countRows)

	expectedQuery := "SELECT id, user_id, transaction_type, amount, ts FROM transactions ORDER BY ts ASC LIMIT $1 OFFSET $2"
	mockPool.ExpectQuery(expectedQuery).
		WithArgs(limitDefault, 0).
		WillReturnError(errors.New("database error"))

	req := httptest.NewRequest(http.MethodGet, "/transactions", nil)
	rec := httptest.NewRecorder()

	api.transactionsHandler(rec, req)

	assert.Equal(http.StatusInternalServerError, rec.Code)

	var errResp errorResponse
	if assert.NoError(json.NewDecoder(rec.Body).Decode(&errResp)) {
		assert.Equal(errInternalMessage, errResp.Error)
	}

	assert.NoError(mockPool.ExpectationsWereMet())
}

func TestTransactionAPIStartWithListener(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockPool, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
	require.NoError(t, err)
	t.Cleanup(mockPool.Close)

	api := NewTransactionApi(ctx, mockPool)

	expectedCountQuery := "SELECT COUNT(*) FROM transactions"
	countRows := pgxmock.NewRows([]string{"count"}).AddRow(0)
	mockPool.ExpectQuery(expectedCountQuery).
		WithArgs().
		WillReturnRows(countRows)

	rows := pgxmock.NewRows([]string{"id", "user_id", "transaction_type", "amount", "ts"})

	expectedQuery := "SELECT id, user_id, transaction_type, amount, ts FROM transactions ORDER BY ts ASC LIMIT $1 OFFSET $2"
	mockPool.ExpectQuery(expectedQuery).
		WithArgs(limitDefault, 0).
		WillReturnRows(rows)

	errCh := make(chan error, 1)
	go func() {
		errCh <- api.Start(listener)
	}()

	require.Eventually(t, func() bool {
		conn, err := net.Dial(listener.Addr().Network(), listener.Addr().String())
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, time.Second, 20*time.Millisecond)

	client := &http.Client{Timeout: time.Second}
	resp, err := client.Get("http://" + listener.Addr().String() + "/transactions")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var apiResp TransactionsResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&apiResp))
	require.Equal(t, []internalpkg.Transaction{}, apiResp.Items)
	require.Equal(t, 0, apiResp.Offset)
	require.Equal(t, limitDefault, apiResp.Limit)
	require.Equal(t, 0, apiResp.Total)

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("server did not shut down")
	}

	require.NoError(t, mockPool.ExpectationsWereMet())
}

func TestTransactionsAPIMethodValidation(t *testing.T) {
	t.Parallel()

	methods := []string{
		http.MethodPost,
		http.MethodPut,
		http.MethodDelete,
		http.MethodPatch,
		http.MethodHead,
		http.MethodOptions,
	}

	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			t.Parallel()

			assert := assert.New(t)
			api, mockPool := newTestTransactionAPI(t)

			req := httptest.NewRequest(method, "/transactions", nil)
			rec := httptest.NewRecorder()

			api.transactionsHandler(rec, req)

			assert.Equal(http.StatusMethodNotAllowed, rec.Code)

			var errResp errorResponse
			if assert.NoError(json.NewDecoder(rec.Body).Decode(&errResp)) {
				assert.Equal("only GET method is allowed", errResp.Error)
			}

			assert.NoError(mockPool.ExpectationsWereMet())
		})
	}
}
