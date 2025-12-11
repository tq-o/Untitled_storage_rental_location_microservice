from __future__ import annotations

import os
from typing import List, Optional
from uuid import UUID, uuid4

import hashlib
import json
import time

import mysql.connector
from fastapi import FastAPI, HTTPException, Query, Response, Request, BackgroundTasks, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any
from fastapi.responses import JSONResponse

from models.address import AddressCreate, AddressRead, AddressUpdate, AddressDelete, addresses_to_features
from google.cloud import pubsub_v1

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
port = int(os.environ.get("FASTAPIPORT", 8000))

# -----------------------------------------------------------------------------
# Database helper
# -----------------------------------------------------------------------------
def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "main-db"),
        port=int(os.getenv("MYSQL_PORT", 3306)),
        user=os.getenv("MYSQL_USER", "admin"),
        password=os.getenv("MYSQL_PASSWORD", "admin123"),
        database=os.getenv("MYSQL_DATABASE", "main_db"),
    )

# Google pub/sub
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("cloud-475420", "address-created")

# In-memory job store for async operations (demo purposes only)
jobs: dict = {}


def compute_etag(resource: dict) -> str:
    """Deterministic ETag derived from the resource content."""
    # Ensure stable ordering
    payload = json.dumps(resource, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

# -----------------------------------------------------------------------------
# FastAPI app
# -----------------------------------------------------------------------------
app = FastAPI(
    title="Address API",
    description="Demo FastAPI app using MySQL for Address storage",
    version="0.2.0",
)

# Enable CORS for all origins (no credentials allowed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# Address endpoints
# -----------------------------------------------------------------------------
@app.post("/addresses", response_model=AddressRead, status_code=201)
def create_address(address: AddressCreate, response: Response):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # Insert address into MySQL
        cursor.execute(
            """
            INSERT INTO addresses (id, name, street, unit, city, state, postal_code, country)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                str(address.id),
                address.name,
                address.street,
                address.unit,
                address.city,
                address.state,
                address.postal_code,
                address.country,
            ),
        )
        conn.commit()
    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

    # Build returned resource with linked data and timestamps
    created = AddressRead(**address.model_dump())
    created.links = [
        {"rel": "self", "href": f"/addresses/{created.id}"},
        {"rel": "collection", "href": "/addresses"}
    ]

    # Publish Pub/Sub event
    event = {
        "eventType": "address.created",
        "addressId": str(created.id),
        "payload": created.model_dump()
    }

    publisher.publish(topic_path, json.dumps(event).encode("utf-8"))
    print("📨 Published address-created event:", event)

    response.headers["Location"] = f"/addresses/{created.id}"
    response.headers["ETag"] = compute_etag(created.model_dump())
    return created

class AddressCollection(BaseModel):
    data: List[AddressRead]
    links: List[Dict[str, Any]]

@app.get("/addresses")
def list_addresses(
    response: Response,
    name: Optional[str] = Query(None),
    street: Optional[str] = Query(None),
    unit: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    postal_code: Optional[str] = Query(None),
    country: Optional[str] = Query(None),
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0),
    as_geojson: bool = Query(False, description="Return as GeoJSON FeatureCollection if true"),
):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    base_query = "FROM addresses WHERE 1=1"
    params: List[object] = []
    if name:
        base_query += " AND name=%s"
        params.append(name)
    if street:
        base_query += " AND street=%s"
        params.append(street)
    if unit:
        base_query += " AND unit=%s"
        params.append(unit)
    if city:
        base_query += " AND city=%s"
        params.append(city)
    if state:
        base_query += " AND state=%s"
        params.append(state)
    if postal_code:
        base_query += " AND postal_code=%s"
        params.append(postal_code)
    if country:
        base_query += " AND country=%s"
        params.append(country)
    count_query = f"SELECT COUNT(*) as cnt {base_query}"
    cursor.execute(count_query, tuple(params))
    total = cursor.fetchone()["cnt"]
    data_query = f"SELECT * {base_query} LIMIT %s OFFSET %s"
    exec_params = list(params) + [limit, offset]
    cursor.execute(data_query, tuple(exec_params))
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    items: List[AddressRead] = []
    for r in results:
        r["links"] = [
            {"rel": "self", "href": f"/addresses/{r['id']}"},
            {"rel": "collection", "href": "/addresses"}
        ]
        items.append(AddressRead(**r))
    def make_qs(extra: dict) -> str:
        parts = []
        if name:
            parts.append(f"name={name}")
        if street:
            parts.append(f"street={street}")
        if unit:
            parts.append(f"unit={unit}")
        if city:
            parts.append(f"city={city}")
        if state:
            parts.append(f"state={state}")
        if postal_code:
            parts.append(f"postal_code={postal_code}")
        if country:
            parts.append(f"country={country}")
        parts.append(f"limit={extra.get('limit', limit)}")
        parts.append(f"offset={extra.get('offset', offset)}")
        return "?" + "&".join(parts) if parts else ""
    base = "/addresses"
    links = []
    links.append({"rel": "current", "href": f"{base}{make_qs({'limit': limit, 'offset': offset})}"})
    links.append({"rel": "first", "href": f"{base}{make_qs({'limit': limit, 'offset': 0})}"})
    last_offset = max(0, ((total - 1) // limit) * limit) if total > 0 else 0
    links.append({"rel": "last", "href": f"{base}{make_qs({'limit': limit, 'offset': last_offset})}"})
    if offset > 0:
        prev_off = max(0, offset - limit)
        links.append({"rel": "prev", "href": f"{base}{make_qs({'limit': limit, 'offset': prev_off})}"})
    if offset + limit < total:
        next_off = offset + limit
        links.append({"rel": "next", "href": f"{base}{make_qs({'limit': limit, 'offset': next_off})}"})
    response.headers["X-Total-Count"] = str(total)
    if as_geojson:
        features = addresses_to_features(conn, [item.model_dump() for item in items])
        return JSONResponse(content={"type": "FeatureCollection", "features": features,
            "links": links,      # Include pagination links
            "total": total  })
    return {"data": items, "links": links, "total": total }

@app.get("/addresses/{address_id}", response_model=AddressRead)
def get_address(address_id: UUID, response: Response):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM addresses WHERE id=%s", (str(address_id),))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    if not result:
        raise HTTPException(status_code=404, detail="Address not found")
    result["links"] = [
        {"rel": "self", "href": f"/addresses/{result['id']}"},
        {"rel": "collection", "href": "/addresses"}
    ]
    etag = compute_etag(result)
    response.headers["ETag"] = etag
    return AddressRead(**result)

@app.patch("/addresses/{address_id}", response_model=AddressRead)
def update_address(address_id: UUID, update: AddressUpdate, request: Request, response: Response):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM addresses WHERE id=%s", (str(address_id),))
    stored = cursor.fetchone()
    if not stored:
        cursor.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Address not found")
    # If-Match ETag handling
    current_etag = compute_etag({
        **stored,
        "links": [
            {"rel": "self", "href": f"/addresses/{stored['id']}"},
            {"rel": "collection", "href": "/addresses"}
        ]
    })
    if_match = request.headers.get("if-match")
    if if_match and if_match != current_etag:
        cursor.close()
        conn.close()
        raise HTTPException(status_code=status.HTTP_412_PRECONDITION_FAILED, detail="ETag does not match")
    updated_data = {**stored, **update.model_dump(exclude_unset=True)}
    cursor.execute(
        """
        UPDATE addresses
        SET name=%s, street=%s, unit=%s, city=%s, state=%s, postal_code=%s, country=%s
        WHERE id=%s
        """,
        (
            updated_data.get("name"),
            updated_data.get("street"),
            updated_data.get("unit"),
            updated_data.get("city"),
            updated_data.get("state"),
            updated_data.get("postal_code"),
            updated_data.get("country"),
            str(address_id),
        ),
    )
    conn.commit()
    cursor.close()
    conn.close()
    updated_data["links"] = [
        {"rel": "self", "href": f"/addresses/{address_id}"},
        {"rel": "collection", "href": "/addresses"}
    ]
    new_etag = compute_etag(updated_data)
    response.headers["ETag"] = new_etag
    return AddressRead(**updated_data)

@app.delete("/addresses/{address_id}", response_model=dict)
def delete_address(address_id: UUID, background_tasks: BackgroundTasks, response: Response):
    # Start asynchronous delete and return 202 with polling location
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM addresses WHERE id=%s", (str(address_id),))
    stored = cursor.fetchone()
    cursor.close()
    conn.close()
    if not stored:
        raise HTTPException(status_code=404, detail="Address not found")

    job_id = str(uuid4())
    jobs[job_id] = {"status": "pending", "address_id": str(address_id)}
    background_tasks.add_task(process_delete_job, job_id, str(address_id))

    response.status_code = status.HTTP_202_ACCEPTED
    response.headers["Location"] = f"/jobs/{job_id}"
    return {"job_id": job_id, "status": "accepted", "location": f"/jobs/{job_id}"}


def process_delete_job(job_id: str, address_id: str):
    """Background worker to perform deletion; updates in-memory job status."""
    try:
        # simulate a delay for async processing
        time.sleep(2)
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM addresses WHERE id=%s", (address_id,))
        conn.commit()
        cursor.close()
        conn.close()
        jobs[job_id]["status"] = "completed"
    except Exception as e:
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["error"] = str(e)


@app.get("/jobs/{job_id}")
def get_job_status(job_id: str, response: Response):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job["status"] in ("pending",):
        response.status_code = status.HTTP_202_ACCEPTED
        response.headers["Retry-After"] = "2"
    else:
        response.status_code = status.HTTP_200_OK
    return job

# -----------------------------------------------------------------------------
# Root
# -----------------------------------------------------------------------------
@app.get("/")
def root():
    return {"message": "Welcome to the Address API. See /docs for OpenAPI UI."}

# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)