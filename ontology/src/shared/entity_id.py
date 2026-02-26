"""Entity ID and hash utilities."""

from __future__ import annotations

import hashlib
import uuid


ENTITY_ID_PREFIX = {
    "person": "pii_person_",
    "phone": "pii_phone_",
    "email": "pii_email_",
    "credential": "pii_cred_",
    "address": "pii_addr_",
    "id_number": "pii_idn_",
    "document": "doc_",
    "organization": "org_",
    "project": "proj_",
    "product": "prod_",
    "technology": "tech_",
    "customer": "cust_",
    "location": "loc_",
    "contract": "cont_",
    "department": "dept_",
    "event": "evt_",
    "concept": "cnpt_",
    "site": "site_",
    "team": "team_",
    "topic": "topic_",
}


def generate_entity_id(entity_type: str) -> str:
    """Generate entity identifier with type-specific prefix and UUID suffix."""
    prefix = ENTITY_ID_PREFIX.get(entity_type, "ent_")
    suffix = uuid.uuid4().hex[:12]
    return f"{prefix}{suffix}"


def compute_canonical_hash(value: str) -> str:
    """Generate SHA-256 hash for normalized canonical value."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
