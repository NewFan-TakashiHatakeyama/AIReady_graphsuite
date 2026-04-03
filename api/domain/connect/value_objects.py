"""Connect domain value objects."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse


@dataclass(frozen=True)
class ConnectTenantConfig:
    """Validated tenant-scoped connection configuration."""

    tenant_id: str
    client_id: str
    client_secret: str
    site_id: str
    drive_id: str
    notification_url: str
    client_state: str
    connection_name: str

    @classmethod
    def create(
        cls,
        *,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        site_id: str,
        drive_id: str,
        notification_url: str,
        client_state: str,
        connection_name: str,
    ) -> "ConnectTenantConfig":
        normalized_tenant_id = str(tenant_id or "").strip()
        normalized_client_id = str(client_id or "").strip()
        normalized_client_secret = str(client_secret or "").strip()
        normalized_site_id = str(site_id or "").strip()
        normalized_drive_id = str(drive_id or "").strip()
        normalized_notification_url = str(notification_url or "").strip()
        normalized_client_state = str(client_state or "").strip()
        normalized_connection_name = str(connection_name or "").strip()

        if not normalized_tenant_id:
            raise ValueError("tenant_id is required.")
        if not normalized_client_id:
            raise ValueError("client_id is required.")
        if not normalized_client_secret:
            raise ValueError("client_secret is required.")
        if not normalized_drive_id:
            raise ValueError("drive_id is required.")
        if not normalized_notification_url:
            raise ValueError("notification_url is required.")

        parsed = urlparse(normalized_notification_url)
        if parsed.scheme.lower() != "https" or not parsed.netloc:
            raise ValueError("notification_url must be a valid https URL.")

        return cls(
            tenant_id=normalized_tenant_id,
            client_id=normalized_client_id,
            client_secret=normalized_client_secret,
            site_id=normalized_site_id,
            drive_id=normalized_drive_id,
            notification_url=normalized_notification_url,
            client_state=normalized_client_state,
            connection_name=normalized_connection_name,
        )
