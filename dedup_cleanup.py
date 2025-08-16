import os
from uuid import UUID
from sqlalchemy import create_engine, text

from app import canonicalize, sha256_hex, natural_key_hash, NATURAL_KEY_FIELDS, insert_unique

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)

def approve(review_id: UUID):
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT new_payload FROM review_queue WHERE id = :id AND status='OPEN'"),
            {"id": str(review_id)},
        ).one_or_none()
        if not row:
            print("No OPEN review found.")
            return
        payload = row[0]
        canonical = canonicalize(payload)
        chash = sha256_hex(canonical)
        nk_hash = natural_key_hash(payload, NATURAL_KEY_FIELDS)

        new_id = insert_unique(conn, payload, canonical, chash, nk_hash)
        if new_id:
            conn.execute(
                text("UPDATE review_queue SET status='APPROVED_UNIQUE' WHERE id=:id"),
                {"id": str(review_id)},
            )
            print("Inserted", new_id)
        else:
            conn.execute(
                text("UPDATE review_queue SET status='REJECTED_DUP' WHERE id=:id"),
                {"id": str(review_id)},
            )
            print("Rejected as dup (race or duplicate).")

def reject(review_id: UUID):
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE review_queue SET status='REJECTED_DUP' WHERE id=:id AND status='OPEN'"),
            {"id": str(review_id)},
        )
        print("Marked as REJECTED_DUP")

if __name__ == "__main__":
    print("This module exposes approve(UUID) and reject(UUID).")
