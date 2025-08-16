import hashlib
import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

NATURAL_KEY_FIELDS = [
    f.strip().lower() for f in os.getenv("NATURAL_KEY_FIELDS", "email,phone,external_id,national_id").split(",")
    if f.strip()
]

SIM_HARD_REDUNDANT = float(os.getenv("SIM_HARD_REDUNDANT", "0.90"))
SIM_REVIEW_LOWER = float(os.getenv("SIM_REVIEW_LOWER", "0.80"))
SIM_REVIEW_UPPER = float(os.getenv("SIM_REVIEW_UPPER", "0.90"))

app = FastAPI(title="Data Redundancy Removal System", version="1.0.0")
engine: Engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)

# ---------- Utilities ----------

def _flatten(obj: Any, parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
    """
    Flatten nested dicts/lists into a {path: value} map with stable key order.
    """
    items = []
    if isinstance(obj, dict):
        for k in sorted(obj.keys()):
            v = obj[k]
            new_key = f"{parent_key}{sep}{k}" if parent_key else str(k)
            items.extend(_flatten(v, new_key, sep=sep).items())
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            new_key = f"{parent_key}{sep}{i}" if parent_key else str(i)
            items.extend(_flatten(v, new_key, sep=sep).items())
    else:
        items.append((parent_key, obj))
    return dict(items)

def normalize_text(val: Any) -> str:
    """
    Normalize values to comparable text:
    - stringify
    - lowercase
    - trim whitespace
    - collapse internal spaces
    """
    s = "" if val is None else str(val)
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def canonicalize(payload: Dict[str, Any]) -> str:
    """
    Deterministic canonical string representation of the payload:
    flatten -> "path=value" lines -> sorted -> joined.
    """
    flat = _flatten(payload)
    parts = []
    for k in sorted(flat.keys()):
        v = normalize_text(flat[k])
        parts.append(f"{k}={v}")
    return "\n".join(parts)

def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def natural_key_hash(payload: Dict[str, Any], natural_fields: List[str]) -> Optional[str]:
    """
    Build a stable hash over present natural-key fields (order-independent).
    Only if at least one field exists in payload.
    """
    pairs = []
    for f in natural_fields:
        if f in payload and payload[f] not in (None, ""):
            # Normalize values per-field (e.g., strip non-digits for phone)
            v = payload[f]
            if f == "phone":
                v = re.sub(r"\D+", "", str(v))
            v = normalize_text(v)
            pairs.append((f, v))
    if not pairs:
        return None
    # Sort to ensure stability
    pairs.sort(key=lambda x: x[0])
    base = "|".join([f"{k}={v}" for k, v in pairs])
    return sha256_hex(base)

# ---------- Pydantic models ----------

class IngestRequest(BaseModel):
    data: Dict[str, Any] = Field(..., description="Arbitrary JSON payload to store")
    natural_keys: Optional[List[str]] = Field(
        default=None,
        description="Optional override list of fields to treat as natural keys",
    )

class IngestResult(BaseModel):
    status: str
    classification: str
    reason: Optional[str] = None
    existing_id: Optional[UUID] = None
    inserted_id: Optional[UUID] = None
    similarity: Optional[float] = None

# ---------- Core dedup logic ----------

def find_exact_by_hash(conn, content_hash: str) -> Optional[UUID]:
    row = conn.execute(
        text("SELECT id FROM records WHERE content_hash = :h LIMIT 1"),
        {"h": content_hash},
    ).one_or_none()
    return row[0] if row else None

def find_by_natural_key(conn, nk_hash: str) -> Optional[UUID]:
    row = conn.execute(
        text("SELECT id FROM records WHERE natural_key_hash = :h LIMIT 1"),
        {"h": nk_hash},
    ).one_or_none()
    return row[0] if row else None

def similar_candidates(conn, canonical: str, limit: int = 5) -> List[Tuple[UUID, float]]:
    """
    Use pg_trgm similarity and the % operator to fetch likely near-duplicates.
    """
    rows = conn.execute(
        text(
            """
            SELECT id, similarity(canonical_text, :cand) AS sim
            FROM records
            WHERE canonical_text % :cand
            ORDER BY sim DESC
            LIMIT :lim
            """
        ),
        {"cand": canonical, "lim": limit},
    ).all()
    return [(r[0], float(r[1])) for r in rows]

def insert_unique(conn, payload: Dict[str, Any], canonical: str, content_hash: str, nk_hash: Optional[str]) -> Optional[UUID]:
    sql = text(
        """
        INSERT INTO records (content_hash, natural_key_hash, canonical_text, payload)
        VALUES (:ch, :nk, :ct, CAST(:pl AS JSONB))
        ON CONFLICT DO NOTHING
        RETURNING id
        """
    )
    row = conn.execute(
        sql,
        {"ch": content_hash, "nk": nk_hash, "ct": canonical, "pl": json.dumps(payload)},
    ).one_or_none()
    return row[0] if row else None

def enqueue_review(conn, new_payload: Dict[str, Any], candidate_id: Optional[UUID], similarity: Optional[float], reason: str):
    conn.execute(
        text(
            """
            INSERT INTO review_queue (new_payload, candidate_id, similarity, reason)
            VALUES (CAST(:pl AS JSONB), :cid, :sim, :rsn)
            """
        ),
        {"pl": json.dumps(new_payload), "cid": str(candidate_id) if candidate_id else None, "sim": similarity, "rsn": reason},
    )

# ---------- API endpoints ----------

@app.post("/ingest", response_model=IngestResult)
def ingest(payload: IngestRequest):
    # 1) canonicalize + hash
    canonical = canonicalize(payload.data)
    chash = sha256_hex(canonical)

    # 2) natural keys (configurable per-request, falls back to env)
    nk_fields = [f.lower() for f in (payload.natural_keys or NATURAL_KEY_FIELDS)]
    nk_hash = natural_key_hash(payload.data, nk_fields)

    with engine.begin() as conn:
        # A) exact dup?
        existing = find_exact_by_hash(conn, chash)
        if existing:
            return IngestResult(
                status="rejected",
                classification="REDUNDANT_EXACT",
                reason="Exact content_hash already exists.",
                existing_id=existing,
            )

        # B) natural key dup?
        if nk_hash:
            nk_existing = find_by_natural_key(conn, nk_hash)
            if nk_existing:
                return IngestResult(
                    status="rejected",
                    classification="REDUNDANT_NATURAL_KEY",
                    reason="Natural-key collision (e.g., email/phone/id) indicates duplicate.",
                    existing_id=nk_existing,
                )

        # C) near-duplicate via similarity
        cands = similar_candidates(conn, canonical, limit=5)
        if cands:
            top_id, top_sim = cands[0]
            if top_sim >= SIM_HARD_REDUNDANT:
                enqueue_review(conn, payload.data, top_id, top_sim, "HARD_REDUNDANT_AUTO")
                return IngestResult(
                    status="rejected",
                    classification="REDUNDANT_NEAR",
                    reason=f"Similarity {top_sim:.3f} exceeds hard threshold.",
                    existing_id=top_id,
                    similarity=top_sim,
                )
            elif SIM_REVIEW_LOWER <= top_sim < SIM_REVIEW_UPPER:
                enqueue_review(conn, payload.data, top_id, top_sim, "POTENTIAL_DUP_REVIEW")
                return IngestResult(
                    status="queued",
                    classification="POTENTIAL_DUPLICATE",
                    reason=f"Similarity {top_sim:.3f} in review band; queued for manual review.",
                    existing_id=top_id,
                    similarity=top_sim,
                )
            # else: proceed to insert

        # D) insert unique (append-only)
        new_id = insert_unique(conn, payload.data, canonical, chash, nk_hash)
        if new_id:
            return IngestResult(
                status="inserted",
                classification="UNIQUE_OK",
                inserted_id=new_id,
            )
        # If ON CONFLICT DO NOTHING triggered (race), fetch winner to explain:
        existing = find_exact_by_hash(conn, chash)
        return IngestResult(
            status="rejected",
            classification="REDUNDANT_RACE",
            reason="Another writer inserted the same content concurrently.",
            existing_id=existing,
        )

@app.get("/records/{rec_id}")
def get_record(rec_id: UUID):
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT id, payload, created_at FROM records WHERE id = :id"),
            {"id": str(rec_id)},
        ).one_or_none()
        if not row:
            raise HTTPException(status_code=404, detail="Not found")
        return {"id": row[0], "payload": row[1], "created_at": row[2]}

@app.get("/reviews")
def list_reviews(status: Optional[str] = None, limit: int = 50):
    q = "SELECT id, new_payload, candidate_id, similarity, reason, status, created_at FROM review_queue"
    params = {}
    if status:
        q += " WHERE status = :s"
        params["s"] = status
    q += " ORDER BY created_at DESC LIMIT :lim"
    params["lim"] = limit
    with engine.begin() as conn:
        rows = conn.execute(text(q), params).all()
        return [
            {
                "id": r[0],
                "new_payload": r[1],
                "candidate_id": r[2],
                "similarity": float(r[3]) if r[3] is not None else None,
                "reason": r[4],
                "status": r[5],
                "created_at": r[6],
            }
            for r in rows
        ]

# Run: uvicorn app:app --reload --port 8080
