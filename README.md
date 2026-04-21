# Customer Ingestion Pipeline (Assessment Submission)

I built this as a simple 3-service pipeline that is easy to run and easy to debug locally:

- `mock-server` (Flask) exposes customer data from a local JSON file
- `pipeline-service` (FastAPI) pulls data from Flask and upserts it into PostgreSQL
- `postgres` stores the final customer records

Flow: Flask JSON -> FastAPI ingestion -> PostgreSQL -> FastAPI query endpoints

## Project Structure

```text
.
├── docker-compose.yml
├── README.md
├── mock-server
│   ├── app.py
│   ├── data/customers.json
│   ├── Dockerfile
│   └── requirements.txt
└── pipeline-service
    ├── main.py
    ├── database.py
    ├── models/customer.py
    ├── services/ingestion.py
    ├── Dockerfile
    └── requirements.txt
```

## Prerequisites

- Docker Desktop (running)
- Docker Compose

## Run the Stack

```bash
docker compose up -d --build
```

If your environment uses legacy compose, this also works:

```bash
docker-compose up -d --build
```

## API Endpoints

### Flask Mock Server (`http://localhost:5000`)

- `GET /api/health`
- `GET /api/customers?page=1&limit=10`
- `GET /api/customers/{customer_id}`

Notes:
- Data source is `mock-server/data/customers.json` (22 records)
- Pagination params are sanitized and capped to avoid oversized responses

### FastAPI Pipeline (`http://localhost:8000`)

- `GET /api/health`
- `POST /api/ingest`
- `GET /api/customers?page=1&limit=10`
- `GET /api/customers/{customer_id}`

Notes:
- Ingestion automatically walks all Flask pages
- Upsert is done using PostgreSQL conflict handling on `customer_id`
- Ingestion returns clear error details for upstream payload/network failures

## Example Test Commands

```bash
# Test Flask
curl "http://localhost:5000/api/customers?page=1&limit=5"

# Ingest all data from Flask into PostgreSQL
curl -X POST "http://localhost:8000/api/ingest"

# Read paginated customers from PostgreSQL through FastAPI
curl "http://localhost:8000/api/customers?page=1&limit=5"
```

## Design Choices

- I kept the schema explicit in SQLAlchemy to match the assessment table spec.
- I used helper functions for repeated serialization and pagination cleanup.
- I kept service code relatively small so behavior is obvious during review.
- I added startup retry in FastAPI so DB init is resilient during compose boot.

## Known Limitations

- No authentication (intentionally omitted for scope).
- No test suite included due to time-boxed assessment constraints.
- Health checks confirm service availability, not deep dependency checks.
