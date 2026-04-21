import json
import logging
from pathlib import Path
from typing import Any, Dict, List

from flask import Flask, jsonify, request


app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mock-server")

DATA_FILE = Path(__file__).resolve().parent / "data" / "customers.json"
DEFAULT_PAGE_SIZE = 10
MAX_PAGE_SIZE = 100


def load_customers() -> List[Dict[str, Any]]:
    with DATA_FILE.open("r", encoding="utf-8") as file:
        return json.load(file)


def _sanitize_pagination(page: int, limit: int) -> tuple[int, int]:
    safe_page = max(page, 1)
    safe_limit = max(1, min(limit, MAX_PAGE_SIZE))
    return safe_page, safe_limit


def _parse_pagination() -> tuple[int, int]:
    raw_page = request.args.get("page", "1")
    raw_limit = request.args.get("limit", str(DEFAULT_PAGE_SIZE))
    try:
        page = int(raw_page)
        limit = int(raw_limit)
    except ValueError:
        raise ValueError("Query params 'page' and 'limit' must be integers.")
    return _sanitize_pagination(page, limit)


@app.get("/api/health")
def health_check():
    return jsonify({"status": "healthy", "service": "mock-server"})


@app.get("/api/customers")
def get_customers():
    try:
        page, limit = _parse_pagination()
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    customers = load_customers()
    total = len(customers)

    start_idx = (page - 1) * limit
    end_idx = start_idx + limit
    page_data = customers[start_idx:end_idx]
    logger.info("Serving customers page=%s limit=%s returned=%s", page, limit, len(page_data))

    return jsonify({"data": page_data, "total": total, "page": page, "limit": limit})


@app.get("/api/customers/<customer_id>")
def get_customer_by_id(customer_id: str):
    customers = load_customers()
    customer = next((item for item in customers if item["customer_id"] == customer_id), None)

    if customer is None:
        return jsonify({"error": "Customer not found"}), 404

    return jsonify({"data": customer})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
