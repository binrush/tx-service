package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	internalpkg "github.com/binrush/tx-service/internal"
	"github.com/jackc/pgx/v5"
)

const (
	queryTimeout       = 3 * time.Second
	errUserIDRequired  = "user_id query parameter is required"
	errInternalMessage = "internal server error"
	limitDefault       = 100
	limitMax           = 1000
)

type TransactionsResponse struct {
	Items  []internalpkg.Transaction `json:"items"`
	Offset int                       `json:"offset"`
	Limit  int                       `json:"limit"`
	Total  int                       `json:"total"`
}

type errorResponse struct {
	Error string `json:"error"`
}

type QueryTransactionsParams struct {
	UserID          string
	TransactionType string
	Offset          int
	Limit           int
}

type queryablePool interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type TransactionApi struct {
	pool        queryablePool
	shutdownCtx context.Context
}

func NewTransactionApi(ctx context.Context, pool queryablePool) *TransactionApi {
	return &TransactionApi{
		pool:        pool,
		shutdownCtx: ctx,
	}
}

func (ts *TransactionApi) buildWhereClause(params *QueryTransactionsParams) (string, []any) {
	paramsCount := 1
	sqlParams := []any{}
	whereConditions := []string{}

	for _, param := range []struct{ paramCol, paramVal string }{
		{paramCol: "user_id", paramVal: params.UserID},
		{paramCol: "transaction_type", paramVal: params.TransactionType},
	} {
		if param.paramVal != "" {
			whereConditions = append(whereConditions, fmt.Sprintf("%s = $%d", param.paramCol, paramsCount))
			sqlParams = append(sqlParams, param.paramVal)
			paramsCount += 1
		}
	}

	if len(whereConditions) > 0 {
		return " WHERE " + strings.Join(whereConditions, " AND "), sqlParams
	}

	return "", sqlParams
}

func (ts *TransactionApi) buildQuery(params *QueryTransactionsParams) (string, []any) {
	QueryBuilder := strings.Builder{}
	QueryBuilder.WriteString("SELECT id, user_id, transaction_type, amount, ts FROM transactions")

	whereClause, sqlParams := ts.buildWhereClause(params)
	paramsCount := len(sqlParams) + 1

	QueryBuilder.WriteString(whereClause)
	QueryBuilder.WriteString(" ORDER BY ts ASC")
	QueryBuilder.WriteString(fmt.Sprintf(" LIMIT $%d OFFSET $%d", paramsCount, paramsCount+1))
	sqlParams = append(sqlParams, params.Limit)
	sqlParams = append(sqlParams, params.Offset)

	return QueryBuilder.String(), sqlParams
}

func (ts *TransactionApi) buildCountQuery(params *QueryTransactionsParams) (string, []any) {
	QueryBuilder := strings.Builder{}
	QueryBuilder.WriteString("SELECT COUNT(*) FROM transactions")

	whereClause, sqlParams := ts.buildWhereClause(params)
	QueryBuilder.WriteString(whereClause)

	return QueryBuilder.String(), sqlParams
}

func (ts *TransactionApi) fetchTransactions(ctx context.Context, params *QueryTransactionsParams) ([]internalpkg.Transaction, error) {
	queryCtx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	query, sqlParams := ts.buildQuery(params)
	rows, err := ts.pool.Query(queryCtx, query, sqlParams...)
	if err != nil {
		return nil, fmt.Errorf("fetch transactions failed: %w", err)
	}
	defer rows.Close()

	results := make([]internalpkg.Transaction, 0)
	for rows.Next() {
		var tx internalpkg.Transaction
		if err := rows.Scan(&tx.ID, &tx.UserId, &tx.TransactionType, &tx.Amount, &tx.Timestamp); err != nil {
			return nil, err
		}
		results = append(results, tx)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func (ts *TransactionApi) countTransactions(ctx context.Context, params *QueryTransactionsParams) (int, error) {
	queryCtx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	query, sqlParams := ts.buildCountQuery(params)
	rows, err := ts.pool.Query(queryCtx, query, sqlParams...)
	if err != nil {
		return 0, fmt.Errorf("count transactions failed: %w", err)
	}
	defer rows.Close()

	var count int
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return 0, fmt.Errorf("scan count failed: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("count transactions failed: %w", err)
	}

	return count, nil
}

func (ts *TransactionApi) Start(listener net.Listener) error {
	mux := http.NewServeMux()
	mux.Handle("/transactions", http.HandlerFunc(ts.transactionsHandler))

	server := &http.Server{
		Handler: mux,
	}

	go func() {
		<-ts.shutdownCtx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP server shutdown error: %v", err)
		}
	}()

	log.Printf("HTTP API listening at %s", listener.Addr())
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("HTTP server error: %w", err)
	}
	log.Println("HTTP API stopped")
	return nil
}

func (ts *TransactionApi) transactionsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "only GET method is allowed")
		return
	}

	params, err := ts.parseQueryParams(r.URL.Query())
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	total, err := ts.countTransactions(r.Context(), params)
	if err != nil {
		log.Printf("count transactions failed: %v", err)
		writeJSONError(w, http.StatusInternalServerError, errInternalMessage)
		return
	}

	results, err := ts.fetchTransactions(r.Context(), params)
	if err != nil {
		log.Printf("fetch transactions failed: %v", err)
		writeJSONError(w, http.StatusInternalServerError, errInternalMessage)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(TransactionsResponse{
		Items:  results,
		Offset: params.Offset,
		Limit:  params.Limit,
		Total:  total,
	}); err != nil {
		log.Printf("encode response failed: %v", err)
	}
}

func (ts *TransactionApi) parseQueryParams(p url.Values) (*QueryTransactionsParams, error) {
	userID := strings.TrimSpace(p.Get("user_id"))

	transactionType := strings.TrimSpace(p.Get("transaction_type"))
	if transactionType != "" && transactionType != internalpkg.TransactionTypeWin && transactionType != internalpkg.TransactionTypeBet {
		return nil, errors.New("transaction_type must be either win or bet")
	}

	var offset int
	var err error
	offsetStr := strings.TrimSpace(p.Get("offset"))
	if offsetStr != "" {
		offset, err = strconv.Atoi(offsetStr)
		if err != nil {
			return nil, fmt.Errorf("invalid offset: %w", err)
		}
		if offset < 0 {
			return nil, errors.New("offset must be non-negative")
		}
	}

	limit := limitDefault
	limitStr := strings.TrimSpace(p.Get("limit"))
	if limitStr != "" {
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			return nil, fmt.Errorf("invalid limit: %w", err)
		}
		if limit <= 0 {
			return nil, errors.New("limit must be positive")
		}
		if limit > limitMax {
			return nil, fmt.Errorf("limit must not exceed %d", limitMax)
		}
	}

	return &QueryTransactionsParams{
		UserID:          userID,
		TransactionType: transactionType,
		Offset:          offset,
		Limit:           limit,
	}, nil
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(errorResponse{Error: msg}); err != nil {
		log.Printf("failed to encode error response: %v", err)
	}
}
