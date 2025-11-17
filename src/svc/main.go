package main

import (
	"context"
	_ "embed"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	internalpkg "github.com/binrush/tx-service/internal"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

const (
	kafkaGroupId       = "transaction-reader"
	kafkaTopic         = "transactions"
	kafkaDLQTopic      = "transactions-dlq"
	defaultDatabaseURL = "postgres://postgres:postgres@postgres:5432/postgres?sslmode=disable"
	apiPort            = 8080
)

//go:embed db/schema.sql
var schemaSQL string

func main() {
	log.Println("Starting transaction service...")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var dsn string
	if dsn = strings.TrimSpace(os.Getenv("DATABASE_URL")); dsn == "" {
		dsn = defaultDatabaseURL
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, schemaSQL); err != nil {
		log.Fatalf("failed to create database schema: %v", err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: internalpkg.Brokers(),
		GroupID: kafkaGroupId,
		Topic:   kafkaTopic,
	})
	defer reader.Close()

	dlqWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  internalpkg.Brokers(),
		Topic:    kafkaDLQTopic,
		Balancer: &kafka.LeastBytes{},
	})
	defer dlqWriter.Close()

	consumer := NewConsumer(pool, reader, dlqWriter)

	api := NewTransactionApi(ctx, pool)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", apiPort))
	if err != nil {
		log.Fatalf("failed to create listener: %v", err)
	}
	defer listener.Close()

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		consumer.Start(ctx)
	}()
	go func() {
		defer wg.Done()
		api.Start(listener)
	}()
	
	log.Println("Service started successfully")
	wg.Wait()
	log.Println("Shutdown complete")
}
