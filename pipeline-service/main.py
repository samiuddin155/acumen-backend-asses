import os
import time
from typing import Generator

from fastapi import Depends, FastAPI, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from database import Base, SessionLocal, engine
from models.customer import Customer
from services.ingestion import IngestionError, ingest_customers


app = FastAPI(title="Customer Pipeline Service")

FLASK_BASE_URL = os.getenv("FLASK_BASE_URL", "http://mock-server:5000")
DEFAULT_PAGE_SIZE = 10


def _serialize_customer(row: Customer) -> dict:
    return {
        "customer_id": row.customer_id,
        "first_name": row.first_name,
        "last_name": row.last_name,
        "email": row.email,
        "phone": row.phone,
        "address": row.address,
        "date_of_birth": row.date_of_birth.isoformat() if row.date_of_birth else None,
        "account_balance": float(row.account_balance)
        if row.account_balance is not None
        else None,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }

def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/api/health")
def health():
    return {"status": "healthy", "service": "pipeline-service"}


@app.on_event("startup")
def create_tables_with_retry() -> None:
    retries = 10
    for attempt in range(1, retries + 1):
        try:
            Base.metadata.create_all(bind=engine)
            return
        except Exception:
            if attempt == retries:
                raise
            time.sleep(2)


@app.post("/api/ingest")
def ingest(db: Session = Depends(get_db)):
    try:
        records_processed = ingest_customers(db, FLASK_BASE_URL)
    except IngestionError as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail="Unexpected error during ingestion.",
        ) from exc

    return {"status": "success", "records_processed": records_processed}


@app.get("/api/customers")
def list_customers(
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=DEFAULT_PAGE_SIZE, ge=1, le=100),
    db: Session = Depends(get_db),
):
    offset = (page - 1) * limit

    total = db.query(Customer).count()
    rows = (
        db.execute(select(Customer).order_by(Customer.customer_id).offset(offset).limit(limit))
        .scalars()
        .all()
    )

    data = [_serialize_customer(row) for row in rows]

    return {"data": data, "total": total, "page": page, "limit": limit}


@app.get("/api/customers/{customer_id}")
def get_customer(customer_id: str, db: Session = Depends(get_db)):
    row = db.get(Customer, customer_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Customer not found")

    return {"data": _serialize_customer(row)}
