from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
import os, base64, bcrypt, time, hmac, hashlib, json, secrets

# AES-256-GCM utilities
# For demo only: key is derived from environment variable. In production use a KMS/HSM.
_MASTER_KEY = os.environ.get("MASTER_KEY_BASE64")
if not _MASTER_KEY:
    # generate a random key for demo runs (persist via env in production)
    _MASTER_KEY = base64.b64encode(os.urandom(32)).decode()
_MASTER_KEY_BYTES = base64.b64decode(_MASTER_KEY)

def _derive_key(context: bytes = b"enc") -> bytes:
    # Use HKDF to derive per-purpose keys
    hkdf = HKDF(algorithm=hashes.SHA256(), length=32, salt=None, info=context)
    return hkdf.derive(_MASTER_KEY_BYTES)

def encrypt_aes_gcm(plaintext: str) -> str:
    key = _derive_key(b"aes-gcm")
    aesgcm = AESGCM(key)
    nonce = os.urandom(12)
    ct = aesgcm.encrypt(nonce, plaintext.encode(), None)
    payload = nonce + ct
    return base64.b64encode(payload).decode()

def decrypt_aes_gcm(payload_b64: str) -> str:
    key = _derive_key(b"aes-gcm")
    aesgcm = AESGCM(key)
    payload = base64.b64decode(payload_b64)
    nonce = payload[:12]
    ct = payload[12:]
    pt = aesgcm.decrypt(nonce, ct, None)
    return pt.decode()

# Password hashing (bcrypt)
def hash_password(password: str) -> str:
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password.encode(), salt).decode()

def verify_password(password: str, password_hash: str) -> bool:
    try:
        return bcrypt.checkpw(password.encode(), password_hash.encode())
    except Exception:
        return False

# Capability token creation and verification (HMAC-signed)
_CAP_SECRET = os.environ.get("CAPABILITY_SECRET", "change_me_for_prod").encode()

def create_capability_token(subject: str, allowed_actions: list, ttl_seconds: int = 3600) -> str:
    payload = {
        "sub": subject,
        "allowed": allowed_actions,
        "exp": int(time.time()) + int(ttl_seconds),
        "nonce": secrets.token_urlsafe(8)
    }
    body = json.dumps(payload, separators=(',', ':'), sort_keys=True).encode()
    sig = hmac.new(_CAP_SECRET, body, hashlib.sha256).digest()
    token = base64.urlsafe_b64encode(body).decode().rstrip('=') + '.' + base64.urlsafe_b64encode(sig).decode().rstrip('=')
    return token

def verify_capability_token(token: str) -> dict:
    try:
        parts = token.split('.')
        body_b64 = parts[0]
        sig_b64 = parts[1]
        # restore padding
        def _pad(s): return s + '=' * (-len(s) % 4)
        body = base64.urlsafe_b64decode(_pad(body_b64))
        sig = base64.urlsafe_b64decode(_pad(sig_b64))
        expected = hmac.new(_CAP_SECRET, body, hashlib.sha256).digest()
        if not hmac.compare_digest(expected, sig):
            return None
        payload = json.loads(body.decode())
        if payload.get('exp',0) < int(time.time()):
            return None
        return payload
    except Exception:
        return None
