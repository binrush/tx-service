package main

import (
	"context"
	"flag"
	"log"
	"time"

	internalpkg "github.com/binrush/tx-service/internal"
)

func main() {
	userID, transactionType, amount := parseArgs()

	producer := internalpkg.NewKafkaProducer(internalpkg.Brokers())
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	transactions, err := producer.GenerateTransactions(ctx, internalpkg.ProducerParams{
		UserID:          userID,
		TransactionType: transactionType,
		Amount:          amount,
		Timestamp:       time.Now().UTC(),
		Count:           1,
	})
	if err != nil {
		log.Fatalf("failed to publish message: %v", err)
	}

	log.Printf("Produced transaction %s to topic %s", transactions[0].ID, internalpkg.TransactionsTopic)
}

func parseArgs() (string, string, uint32) {
	userID := flag.String("user", "", "User identifier")
	transactionType := flag.String("type", "", `Transaction type ("win" or "bet")`)
	amount := flag.Uint("amount", 0, "Transaction amount (positive integer)")

	flag.Parse()

	return *userID, *transactionType, uint32(*amount)
}
