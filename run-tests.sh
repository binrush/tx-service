#!/bin/bash

set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Ensure we're in the script directory
cd "$(dirname "$0")"

echo ""
echo -e "${YELLOW}======================================${NC}"
echo -e "${YELLOW}  Running Unit Tests${NC}"
echo -e "${YELLOW}======================================${NC}"
echo ""

# Run unit tests with coverage
cd src
if go test ./... -v -race -coverprofile=coverage.out -covermode=atomic; then
    echo ""
    echo -e "${GREEN}✓ Unit tests passed!${NC}"
    
    # Generate HTML coverage report
    if [ -f coverage.out ]; then
        echo -e "${YELLOW}Generating unit test coverage report...${NC}"
        go tool cover -html=coverage.out -o coverage.html
        echo -e "${GREEN}✓ Unit test coverage report generated: src/coverage.html${NC}"
    fi
else
    echo ""
    echo -e "${RED}✗ Unit tests failed!${NC}"
    exit 1
fi
cd ..

echo ""
echo -e "${YELLOW}======================================${NC}"
echo -e "${YELLOW}  Running Integration Tests${NC}"
echo -e "${YELLOW}======================================${NC}"
echo ""

# Clean up function
cleanup() {
    echo ""
    echo -e "${YELLOW}Cleaning up...${NC}"
    docker compose --profile integration down -v
}

# Set trap to cleanup on exit
trap cleanup EXIT INT TERM

# Run integration tests with all dependencies
echo -e "${YELLOW}Starting services and running tests...${NC}"
echo ""

if docker compose up --abort-on-container-exit --exit-code-from integration-tests integration-tests; then
    echo ""
    echo -e "${GREEN}✓ Integration tests passed!${NC}"
    echo ""
    echo -e "${GREEN}======================================${NC}"
    echo -e "${GREEN}  All Tests Passed! ✓${NC}"
    echo -e "${GREEN}======================================${NC}"
    exit 0
else
    echo ""
    echo -e "${RED}✗ Integration tests failed!${NC}"
    exit 1
fi

