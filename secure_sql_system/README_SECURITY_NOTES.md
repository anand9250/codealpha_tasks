## Security Notes / Design Rationale

1) Preventing SQL Injection
   - Use parameterized queries (SQLAlchemy core / ORM) instead of string concatenation.
   - Whitelist operations on the server (the `/query` endpoint only allows pre-defined safe operations).
   - Do not accept raw SQL from clients. If a capability must allow custom queries, implement a strict parser and whitelist columns and tables.

2) Double-layer security
   - Layer 1: Application-level defenses: parameterized queries, input validation, whitelist operations, capability tokens limiting allowed actions.
   - Layer 2: Data/DB-level defenses: use least-privilege DB accounts (limit to SELECT/UPDATE on specific tables), database roles, and encryption at rest.
   - Additionally: network-level protections (firewalls), logging & monitoring, rate-limiting, WAF.

3) Encryption
   - AES-256-GCM is used for authenticated encryption of sensitive fields.
   - Passwords are hashed using bcrypt (do not decrypt passwords).
   - In production, use a Key Management Service (KMS) to store master keys.

4) Capability Codes
   - Capability tokens are HMAC-signed JSON payloads that carry `sub` (subject id), `allowed` actions, and `exp` timestamp.
   - Tokens are validated using HMAC compare_digest to avoid timing attacks.

5) Deploying to cloud
   - The example uses SQLite to be runnable with minimal requirements.
   - For cloud deployment use PostgreSQL/MySQL and configure a secrets manager for `MASTER_KEY_BASE64` and `CAPABILITY_SECRET`.
   - Containerize with provided Dockerfile and deploy to services like Cloud Run, AWS ECS Fargate, or DigitalOcean App Platform.

6) Additional recommendations
   - Use prepared statements and parameter binding for all DB interactions.
   - Enable DB auditing and monitor for anomalous queries.
   - Use role separation: an 'app' DB user and an 'admin' DB user. The app user should have the minimum required privileges.
   - Rotate keys regularly and use short-lived capability tokens.
