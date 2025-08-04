#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting FastAPI Email Server...${NC}"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo -e "${RED}Virtual environment not found. Creating one...${NC}"
    python3 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Check if dependencies are installed
if ! python -c "import fastapi" 2>/dev/null; then
    echo -e "${GREEN}Installing dependencies...${NC}"
    pip install -r requirements.txt
fi

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo -e "${RED}.env file not found. Creating from example...${NC}"
    cp .env.example .env
    echo -e "${RED}Please edit .env file with your configuration before running again.${NC}"
    exit 1
fi

# Check if MongoDB URI is configured
if grep -q "mongodb://localhost:27017/email_server" .env; then
    echo -e "${RED}Warning: Using default MongoDB URI. Make sure MongoDB is running or update .env file.${NC}"
fi

# Start the server
echo -e "${GREEN}Starting server on port 8001...${NC}"
echo -e "${GREEN}API docs available at: http://localhost:8001/docs${NC}"
echo -e "${GREEN}Press CTRL+C to stop the server${NC}"

uvicorn app.main:app --reload --host 0.0.0.0 --port 8001