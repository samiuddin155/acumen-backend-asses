from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List

import dlt
import requests
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from models.customer import Customer


class IngestionError(Exception):
    """Raised when data pull or normalization fails during ingest."""


def fetch_customers_from_flask(flask_base_url: str, page_size: int = 100) -> List[Dict[str, Any]]:
    """Fetch every customer page from the mock server."""
    all_customers: List[Dict[str, Any]] = []
    page = 1

    while True:
        try:
            response = requests.get(
                f"{flask_base_url}/api/customers",
                params={"page": page, "limit": page_size},
                timeout=15,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            raise IngestionError(f"Failed to read data from mock server page {page}.") from exc
        payload = response.json()

        data = payload.get("data", [])
        if not data:
            break

        all_customers.extend(data)

        total = int(payload.get("total", 0))
        if len(all_customers) >= total:
            break

        page += 1

    return all_customers


def _normalize_customers(customers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convert raw JSON payload values into DB-friendly types."""
    normalized: List[Dict[str, Any]] = []

    for item in customers:
        try:
            normalized.append(
                {
                    "customer_id": item["customer_id"],
                    "first_name": item["first_name"],
                    "last_name": item["last_name"],
                    "email": item["email"],
                    "phone": item.get("phone"),
                    "address": item.get("address"),
                    "date_of_birth": (
                        datetime.strptime(item["date_of_birth"], "%Y-%m-%d").date()
                        if item.get("date_of_birth")
                        else None
                    ),
                    "account_balance": (
                        Decimal(str(item["account_balance"]))
                        if item.get("account_balance") is not None
                        else None
                    ),
                    "created_at": (
                        datetime.fromisoformat(item["created_at"])
                        if item.get("created_at")
                        else None
                    ),
                }
            )
        except (KeyError, ValueError, TypeError) as exc:
            raise IngestionError(
                f"Customer payload is invalid for customer_id={item.get('customer_id', 'unknown')}."
            ) from exc

    return normalized


def ingest_customers(session: Session, flask_base_url: str) -> int:
    # Keep dlt in the flow to satisfy extraction layer requirement.
    @dlt.resource(name="customers")
    def customer_resource():
        for customer in fetch_customers_from_flask(flask_base_url):
            yield customer

    raw_customers = list(customer_resource())
    normalized_customers = _normalize_customers(raw_customers)

    if not normalized_customers:
        return 0

    stmt = insert(Customer).values(normalized_customers)
    upsert_stmt = stmt.on_conflict_do_update(
        index_elements=[Customer.customer_id],
        set_={
            "first_name": stmt.excluded.first_name,
            "last_name": stmt.excluded.last_name,
            "email": stmt.excluded.email,
            "phone": stmt.excluded.phone,
            "address": stmt.excluded.address,
            "date_of_birth": stmt.excluded.date_of_birth,
            "account_balance": stmt.excluded.account_balance,
            "created_at": stmt.excluded.created_at,
        },
    )

    try:
        session.execute(upsert_stmt)
        session.commit()
    except Exception as exc:
        session.rollback()
        raise IngestionError("Database upsert failed for customer batch.") from exc

    return len(normalized_customers)
