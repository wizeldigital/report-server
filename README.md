# FastAPI Email Server

A FastAPI port of the Express.js email marketing analytics server with Klaviyo integration.

## Features

- Multi-tenant email marketing analytics platform
- Klaviyo API integration for campaign and flow statistics
- MongoDB with Beanie ODM for data persistence
- Private key authentication
- Granular permission system (VIEW_ANALYTICS, MANAGE_CAMPAIGNS, ADMIN)
- Asynchronous request handling
- Rate-limited API calls to Klaviyo
- Bulk data synchronization

## Requirements

- Python 3.8+
- MongoDB 4.0+
- Klaviyo API access

## Installation

1. Clone the repository and navigate to the FastAPI server directory:
```bash
cd fastapi-email-server
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Copy the example environment file and configure:
```bash
cp .env.example .env
```

5. Edit `.env` with your configuration:
   - Set `MONGODB_URI` to your MongoDB connection string
   - Set `PRIVATE_KEY` to a secure authentication key
   - Optionally set `KLAVIYO_API_KEY` for global Klaviyo access

## Running the Server

### Development Mode
```bash
uvicorn app.main:app --reload --port 8001
```

### Production Mode
```bash
uvicorn app.main:app --host 0.0.0.0 --port 8001 --workers 4
```

## API Documentation

Once the server is running, visit:
- Swagger UI: http://localhost:8001/docs
- ReDoc: http://localhost:8001/redoc

## API Endpoints

All API endpoints require authentication via private key, which can be provided through:
- `x-private-key` header
- `Authorization: Bearer <key>` header
- `privateKey` query parameter
- `privateKey` in request body (POST/PUT requests)

### Reports API (`/api/v1/reports`)

- `GET /` - Search campaign statistics with pagination
- `POST /` - Create new campaign statistics
- `GET /{id}` - Get specific campaign statistics
- `POST /sync` - Trigger Klaviyo sync for a store
- `GET /sync/status/{store_id}` - Check sync status
- `POST /sync/bulk` - Trigger sync for multiple stores

### Flows API (`/api/v1/flows`)

- `GET /` - Search flow statistics with pagination
- `POST /` - Create new flow statistics
- `GET /{id}` - Get specific flow statistics
- `POST /bulk` - Bulk upsert flow statistics

## Data Models

### Store
- Central entity for e-commerce stores
- Contains Klaviyo integration settings
- Tracks sync status and timestamps

### User
- Represents platform users
- Has access to multiple stores with specific permissions

### CampaignStats
- Campaign performance metrics (30+ statistics)
- Links to store and includes audience targeting

### FlowStats
- Email automation flow metrics
- Time-series data for trending
- Supports A/B testing experiments

## Permission System

Three permission levels:
- `VIEW_ANALYTICS` - View analytics data
- `MANAGE_CAMPAIGNS` - Manage campaigns
- `ADMIN` - Full access (overrides other permissions)

## Development

### Running Tests
```bash
pytest
```

### Code Formatting
```bash
black app/
ruff check app/
```

### Type Checking
```bash
mypy app/
```

## Deployment

For production deployment, consider:
1. Using environment variables for all sensitive configuration
2. Setting up MongoDB replica set for high availability
3. Implementing proper logging and monitoring
4. Using a process manager like systemd or Docker
5. Setting up SSL/TLS termination with a reverse proxy

## Differences from Express Version

This FastAPI implementation maintains feature parity with the Express.js version while adding:
- Async/await throughout for better performance
- Automatic API documentation
- Type hints and validation with Pydantic
- Built-in request validation
- Improved error handling with proper HTTP status codes

## License

[Your License Here]