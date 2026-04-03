from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class TenantContext:
    tenant_id: str
    username: str
    roles: list[str]
    is_admin: bool
    claims: dict[str, Any]

