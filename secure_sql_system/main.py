from fastapi import FastAPI, HTTPException, Depends, Header
from pydantic import BaseModel, constr
import os, time, hmac, hashlib, base64, secrets
from typing import Optional, Dict, Any, List
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, select
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
import databases
from security import encrypt_aes_gcm, decrypt_aes_gcm, hash_password, verify_password, create_capability_token, verify_capability_token

# Configuration (in production, use environment variables or secret manager)
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./demo.db")
CAPABILITY_SECRET = os.environ.get("CAPABILITY_SECRET", "change_this_secret_in_prod")
DB = databases.Database(DATABASE_URL)
metadata = MetaData()

users = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("username", String, unique=True, index=True),
    Column("password_hash", String),
    Column("ssn_enc", String, nullable=True),  # encrypted sensitive data
)

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {})
metadata.create_all(engine)

app = FastAPI(title="Secure SQL System Demo")

class RegisterIn(BaseModel):
    username: constr(min_length=3, max_length=100)
    password: constr(min_length=8, max_length=128)
    ssn: Optional[constr(min_length=4, max_length=64)] = None

class LoginIn(BaseModel):
    username: str
    password: str

class QueryRequest(BaseModel):
    # Allowed operations: "select_user", "update_ssn"
    operation: str
    params: Dict[str, Any] = {}

@app.on_event("startup")
async def startup():
    await DB.connect()

@app.on_event("shutdown")
async def shutdown():
    await DB.disconnect()

@app.post("/register")
async def register(data: RegisterIn):
    # 1) Hash password using bcrypt
    pwd_hash = hash_password(data.password)
    # 2) Encrypt sensitive SSN with AES-256-GCM
    ssn_enc = None
    if data.ssn:
        ssn_enc = encrypt_aes_gcm(data.ssn)
    # 3) Insert using parameterized queries via SQLAlchemy core / databases
    query = users.insert().values(username=data.username, password_hash=pwd_hash, ssn_enc=ssn_enc)
    try:
        user_id = await DB.execute(query)
        return {"status":"ok", "user_id": user_id}
    except Exception as e:
        raise HTTPException(status_code=400, detail="username may already exist or invalid data")

@app.post("/login")
async def login(data: LoginIn):
    query = select([users.c.id, users.c.username, users.c.password_hash]).where(users.c.username == data.username)
    row = await DB.fetch_one(query)
    if not row:
        raise HTTPException(status_code=401, detail="invalid credentials")
    if not verify_password(data.password, row["password_hash"]):
        raise HTTPException(status_code=401, detail="invalid credentials")
    # Create a capability token limited to "select_user" and "update_ssn" for user's own id
    token = create_capability_token(subject=str(row["id"]), allowed_actions=["select_user", "update_ssn"], ttl_seconds=3600)
    return {"access_token": token, "token_type": "capability"}

def require_capability(x_capability: Optional[str] = Header(None)):
    if not x_capability:
        raise HTTPException(status_code=401, detail="capability header missing")
    payload = verify_capability_token(x_capability)
    if not payload:
        raise HTTPException(status_code=403, detail="invalid or expired capability")
    return payload

@app.post("/query")
async def query_endpoint(req: QueryRequest, payload=Depends(require_capability)):
    # payload contains subject (user id) and allowed_actions
    subject = payload.get("sub")
    allowed = payload.get("allowed", [])
    op = req.operation
    if op not in allowed:
        raise HTTPException(status_code=403, detail="operation not allowed by capability token")

    # Server-side whitelist of allowed operations. Each operation maps to a safe parameterized action.
    if op == "select_user":
        # params: none. Return non-sensitive info. Decrypt ssn only if capability allows.
        q = select([users.c.id, users.c.username, users.c.ssn_enc]).where(users.c.id == int(subject))
        row = await DB.fetch_one(q)
        if not row:
            raise HTTPException(status_code=404, detail="not found")
        ssn = None
        if row["ssn_enc"]:
            # Only allow sending decrypted SSN if the capability explicitly allowed "reveal_ssn"
            if "reveal_ssn" in allowed:
                ssn = decrypt_aes_gcm(row["ssn_enc"])
            else:
                ssn = "REDACTED"
        return {"id": row["id"], "username": row["username"], "ssn": ssn}

    elif op == "update_ssn":
        # params: new_ssn
        new_ssn = req.params.get("new_ssn")
        if not new_ssn:
            raise HTTPException(status_code=400, detail="missing new_ssn")
        # encrypt and update using parameterized queries
        new_enc = encrypt_aes_gcm(new_ssn)
        upd = users.update().where(users.c.id == int(subject)).values(ssn_enc=new_enc)
        await DB.execute(upd)
        return {"status":"ok"}
    else:
        raise HTTPException(status_code=400, detail="unknown operation")
