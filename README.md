# Transaction processing service

The service consumes transactions from the Apache Kafka topic and stores them in PostgreSQL database.

Created for educational purposes.

## Running service

```bash
docker compose up -d
```
This command will run four containers:
- zookeeper
- kafka
- kafka-init (creates topics and exists)
- transaction-service

## CLI tool

The service provides a CLI tool for producing transactions
Usage:

```bash
cd src
go run ./cli/main.go -user <user_id> -type <transaction_type> -amount <amount>
```
All parameters are optional: if not provided, the tool will generate a random value

## API

The service provides a REST API for querying transactions. It is available at http://localhost:8080 by default

Examples: 

Get all transactions:
```bash
curl http://localhost:8080/transactions | jq
```

Get transactions for a specific user:
```bash
curl http://localhost:8080/transactions?user_id=123 | jq
```

Get transactions for a specific user and transaction type:
```bash
curl http://localhost:8080/transactions?user_id=123&transaction_type=win | jq
```

## Testing

To run tests, use the following command:
```bash
./run-tests.sh
```
(requires Docker and Go installed)

This command will run unit tests and integration tests.

To run unit tests only, use the following command:
```bash
cd src
go test ./... -v -race
```

## Design considerations/limitations

  * Database migrations are not implemented for simplicity - schema is made idempotent instead.
    In real system, dataase schema most probably will be managed by a migration tool.
  * Consumer and API are implemented in a single binary for simplicity, in real system they most probably will be implemented as separate services.
  * Most of parameters are hard-coded for simplicity
  * Authentication and authorization are not implemented (not listed in requirements)
