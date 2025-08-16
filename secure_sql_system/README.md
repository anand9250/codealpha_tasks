# Secure SQL System Demo
This project demonstrates a minimal cloud-friendly system that defends against SQL injection,
stores sensitive fields using AES-256-GCM, uses bcrypt for password hashing, and implements
a capability-code (signed token) mechanism to allow controlled SQL-like operations.

Features:
- FastAPI backend (single-file for simplicity)
- SQLite (zero-ops) as demo database so it can run with minimal requirements
- SQLAlchemy ORM with parameterized queries (prevents SQL injection)
- AES-256-GCM encryption for sensitive fields (e.g., SSN)
- bcrypt for password hashing
- Capability tokens (HMAC-signed) that limit allowed operations and expire
- Example client script to demonstrate usage
- Dockerfile for containerized deployment

**Note:** This demo is for educational purposes. For production, use a managed database, KMS/HSM for keys,
secure secrets storage, strong network policies, and a Web Application Firewall (WAF).
