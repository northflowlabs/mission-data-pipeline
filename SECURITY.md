# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | ✅ Active |

---

## Reporting a Vulnerability

**Do not report security vulnerabilities through public GitHub Issues.**

Please report security issues by email to:  
**security@northflow.no**

Include in your report:
- A description of the vulnerability and its potential impact
- Steps to reproduce or a proof-of-concept
- Affected version(s) and environment details

You will receive an acknowledgement within **48 hours** and a full response within **7 business days**.

---

## Scope

This policy applies to the `mission-data-pipeline` library itself.

**In scope:**
- Arbitrary code execution via malformed telemetry input
- Path traversal vulnerabilities in file-based extractors or loaders
- Denial-of-service via crafted CCSDS packet streams (e.g., header length manipulation)

**Out of scope:**
- Vulnerabilities in third-party dependencies (report to the respective project)
- Issues requiring physical access to the machine running the pipeline

---

## Security Design Notes

- **No network access by default.** The core library and all built-in plugins perform only local file I/O. Network extractors (e.g., UDP, Kafka) are user-provided and not part of the core distribution.
- **Input validation.** All packet and frame parsers validate field ranges and data lengths before processing. Malformed inputs raise typed exceptions rather than producing silent corruption.
- **No eval or exec.** The pipeline engine and all built-in stages contain no use of `eval`, `exec`, or `pickle` deserialization.
- **Dependency minimisation.** Runtime dependencies are limited to well-maintained scientific Python libraries (NumPy, Pandas, PyArrow, h5py, Pydantic, structlog, Click, Rich).
