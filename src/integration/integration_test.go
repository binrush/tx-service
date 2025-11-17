package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	internalpkg "github.com/binrush/tx-service/internal"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

const (
	APIBaseURL        = "http://transaction-service:8080"
	requestTimeout    = 5 * time.Second
	runIntegrationEnv = "RUN_INTEGRATION_TESTS"
	dlqTopic          = "transactions-dlq"
)

type transactionsResponse struct {
	Items  []internalpkg.Transaction `json:"items"`
	Offset int                       `json:"offset"`
	Limit  int                       `json:"limit"`
	Total  int                       `json:"total"`
}

func TestTransactionsFlow(t *testing.T) {
	if os.Getenv(runIntegrationEnv) != "1" {
		t.Skipf("set %s=1 to enable integration test", runIntegrationEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	producer := internalpkg.NewKafkaProducer(internalpkg.Brokers())
	defer producer.Close()

	inputTransactions := loadTransactionsFromJSONLines(t, testdataPath("transactions.jsonl"))
	require.NoError(t, producer.PublishTransactions(ctx, inputTransactions))

	client := &http.Client{Timeout: requestTimeout}
	err := waitForTransactions(ctx, client, APIBaseURL, len(inputTransactions))
	require.NoError(t, err)

	for _, scenario := range []struct {
		name                 string
		query                string
		expectedResponsePath string
	}{
		{
			name:                 "all transactions",
			query:                "",
			expectedResponsePath: "scenarios/all.json",
		},
		{
			name:                 "user=integration all",
			query:                "user_id=integration-user-fixture",
			expectedResponsePath: "scenarios/user_integration_all.json",
		},
		{
			name:                 "user=integration bet",
			query:                "user_id=integration-user-fixture&transaction_type=bet",
			expectedResponsePath: "scenarios/user_integration_bet.json",
		},
		{
			name:                 "user=integration win",
			query:                "user_id=integration-user-fixture&transaction_type=win",
			expectedResponsePath: "scenarios/user_integration_win.json",
		},
		{
			name:                 "user=integration-alt all",
			query:                "user_id=integration-user-fixture-alt",
			expectedResponsePath: "scenarios/user_integration_alt_all.json",
		},
		{
			name:                 "user=integration-alt bet",
			query:                "user_id=integration-user-fixture-alt&transaction_type=bet",
			expectedResponsePath: "scenarios/user_integration_alt_bet.json",
		},
		{
			name:                 "user=integration-alt win",
			query:                "user_id=integration-user-fixture-alt&transaction_type=win",
			expectedResponsePath: "scenarios/user_integration_alt_win.json",
		},
		{
			name:                 "user=integration with limit",
			query:                "user_id=integration-user-fixture&limit=2",
			expectedResponsePath: "scenarios/user_integration_limit_2.json",
		},
		{
			name:                 "user=integration with offset",
			query:                "user_id=integration-user-fixture&offset=2",
			expectedResponsePath: "scenarios/user_integration_offset_2.json",
		},
		{
			name:                 "user=integration with limit and offset",
			query:                "user_id=integration-user-fixture&limit=2&offset=1",
			expectedResponsePath: "scenarios/user_integration_limit_2_offset_1.json",
		},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			received, err := fetchTransactions(ctx, client, scenario.query)
			require.NoError(t, err)
			require.JSONEq(t, loadTransactionsResponse(t, testdataPath(scenario.expectedResponsePath)), received)
		})
	}
}

func waitForTransactions(ctx context.Context, client *http.Client, query string, expectedCount int) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		var lastErr error
		transactions, lastErr := fetchTransactions(ctx, client, query)
		var payload transactionsResponse

		if lastErr == nil {
			lastErr = json.Unmarshal([]byte(transactions), &payload)
		}

		if lastErr == nil && len(payload.Items) >= expectedCount {
			return nil
		}

		select {
		case <-ctx.Done():
			if lastErr != nil {
				return fmt.Errorf("context done while waiting for transactions: %w", lastErr)
			}
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func fetchTransactions(ctx context.Context, client *http.Client, query string) (string, error) {
	endpoint := fmt.Sprintf("%s/transactions?%s", APIBaseURL, query)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status %s", resp.Status)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read body: %w", err)
	}
	return string(body), nil
}

func loadTransactionsFromJSONLines(t *testing.T, path string) []internalpkg.Transaction {
	t.Helper()

	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()

	scanner := bufio.NewScanner(file)

	result := make([]internalpkg.Transaction, 0)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var tx internalpkg.Transaction
		require.NoErrorf(t, json.Unmarshal([]byte(line), &tx), "invalid json: %s", line)

		result = append(result, tx)
	}

	require.NoError(t, scanner.Err())
	return result
}

func loadTransactionsResponse(t *testing.T, path string) string {
	t.Helper()

	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()

	content, err := io.ReadAll(file)
	require.NoError(t, err)
	return string(content)
}

func testdataPath(name string) string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "testdata", name)
}

func TestInvalidMessagesGoToDLQ(t *testing.T) {
	if os.Getenv(runIntegrationEnv) != "1" {
		t.Skipf("set %s=1 to enable integration test", runIntegrationEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a DLQ reader
	dlqReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: internalpkg.Brokers(),
		Topic:   dlqTopic,
		GroupID: fmt.Sprintf("dlq-test-%d", time.Now().UnixNano()),
	})
	defer dlqReader.Close()

	// Publish invalid messages to the transactions topic
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: internalpkg.Brokers(),
		Topic:   internalpkg.TransactionsTopic,
	})
	defer writer.Close()

	invalidMessages := []struct {
		name  string
		value []byte
	}{
		{"invalid_json", []byte(`{"invalid": "json"`)},
		{"empty_user_id", []byte(`{"id":"550e8400-e29b-41d4-a716-446655440000","user_id":"","transaction_type":"bet","amount":100,"timestamp":"2025-11-17T10:00:00Z"}`)},
		{"invalid_transaction_type", []byte(`{"id":"650e8400-e29b-41d4-a716-446655440000","user_id":"test-user","transaction_type":"invalid","amount":100,"timestamp":"2025-11-17T10:00:00Z"}`)},
		{"zero_amount", []byte(`{"id":"750e8400-e29b-41d4-a716-446655440000","user_id":"test-user","transaction_type":"bet","amount":0,"timestamp":"2025-11-17T10:00:00Z"}`)},
		{"missing_id", []byte(`{"user_id":"test-user","transaction_type":"bet","amount":100,"timestamp":"2025-11-17T10:00:00Z"}`)},
	}

	messages := make([]kafka.Message, 0, len(invalidMessages))
	for _, msg := range invalidMessages {
		messages = append(messages, kafka.Message{Value: msg.value})
	}

	err := writer.WriteMessages(ctx, messages...)
	require.NoError(t, err, "failed to publish invalid messages")

	// Read from DLQ and verify invalid messages arrived
	receivedCount := 0
	deadline := time.Now().Add(20 * time.Second)

	for receivedCount < len(invalidMessages) && time.Now().Before(deadline) {
		readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		msg, err := dlqReader.ReadMessage(readCtx)
		cancel()

		if err != nil {
			if err == context.DeadlineExceeded {
				continue
			}
			t.Logf("error reading from DLQ: %v", err)
			continue
		}

		receivedCount++
		t.Logf("DLQ message %d: %s", receivedCount, string(msg.Value))
	}

	require.Equal(t, len(invalidMessages), receivedCount, "expected all invalid messages to be in DLQ")
}
