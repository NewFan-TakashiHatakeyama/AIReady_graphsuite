"""M365 Graph API client for remediation operations."""

from __future__ import annotations

import json
import time
import urllib.error as urllib_error
import urllib.parse as urllib_parse
import urllib.request as urllib_request
from typing import Any

from shared.logger import get_logger

logger = get_logger(__name__)


def _extract_permission_email(permission_backup: dict[str, Any]) -> str:
    for key in ("grantedToV2", "grantedTo"):
        granted = permission_backup.get(key)
        if not isinstance(granted, dict):
            continue
        user = granted.get("user")
        if not isinstance(user, dict):
            continue
        email = str(user.get("email") or "").strip()
        if email:
            return email
    for key in ("grantedToIdentitiesV2", "grantedToIdentities"):
        identities = permission_backup.get(key)
        if not isinstance(identities, list):
            continue
        for identity in identities:
            if not isinstance(identity, dict):
                continue
            user = identity.get("user")
            if not isinstance(user, dict):
                continue
            email = str(user.get("email") or "").strip()
            if email:
                return email
    invitation = permission_backup.get("invitation")
    if isinstance(invitation, dict):
        invited_email = str(invitation.get("email") or "").strip()
        if invited_email:
            return invited_email
    return ""


class RemediationGraphError(RuntimeError):
    """Raised when Graph API remediation request fails."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        response_body: str = "",
    ):
        super().__init__(message)
        self.status_code = int(status_code) if status_code is not None else None
        self.response_body = str(response_body or "")


class RemediationGraphClient:
    """Minimal Graph client for permission-level remediation."""

    def __init__(
        self,
        *,
        azure_tenant_id: str,
        client_id: str,
        client_secret: str,
        max_retries: int = 2,
    ):
        self._azure_tenant_id = str(azure_tenant_id or "").strip()
        self._client_id = str(client_id or "").strip()
        self._client_secret = str(client_secret or "").strip()
        self._max_retries = max(0, int(max_retries))
        self._access_token = ""

    def get_access_token(self) -> str:
        token_url = (
            f"https://login.microsoftonline.com/{self._azure_tenant_id}/oauth2/v2.0/token"
        )
        body = urllib_parse.urlencode(
            {
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "scope": "https://graph.microsoft.com/.default",
                "grant_type": "client_credentials",
            }
        ).encode("utf-8")
        request = urllib_request.Request(
            token_url,
            data=body,
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        try:
            with urllib_request.urlopen(request, timeout=30) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib_error.HTTPError as exc:
            raise RemediationGraphError(
                f"Token request failed with status={exc.code}"
            ) from exc
        except urllib_error.URLError as exc:
            raise RemediationGraphError(
                f"Token request failed: {exc.reason}"
            ) from exc
        token = str(payload.get("access_token") or "").strip()
        if not token:
            raise RemediationGraphError("Token response did not include access_token")
        self._access_token = token
        return token

    def _auth_header(self) -> dict[str, str]:
        if not self._access_token:
            self.get_access_token()
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Accept": "application/json",
        }

    def assign_sensitivity_label(
        self,
        *,
        drive_id: str,
        item_id: str,
        sensitivity_label_id: str,
        assignment_method: str = "standard",
        justification_text: str = "",
    ) -> dict[str, int | str]:
        """Assign M365 sensitivity label to a drive item."""
        url = (
            "https://graph.microsoft.com/beta/drives/"
            f"{urllib_parse.quote(str(drive_id).strip())}"
            f"/items/{urllib_parse.quote(str(item_id).strip())}/assignSensitivityLabel"
        )
        body: dict[str, Any] = {
            "sensitivityLabelId": str(sensitivity_label_id).strip(),
            "assignmentMethod": str(assignment_method or "standard").strip() or "standard",
        }
        if justification_text:
            body["justificationText"] = str(justification_text).strip()
        payload = json.dumps(body, ensure_ascii=True).encode("utf-8")
        headers = {
            **self._auth_header(),
            "Content-Type": "application/json",
        }
        request = urllib_request.Request(
            url,
            data=payload,
            method="POST",
            headers=headers,
        )
        for attempt in range(self._max_retries + 1):
            try:
                with urllib_request.urlopen(request, timeout=30) as response:
                    code = int(getattr(response, "status", 202) or 202)
                    return {
                        "status": "label_applied",
                        "http_status": code,
                        "sensitivity_label_id": str(sensitivity_label_id).strip(),
                    }
            except urllib_error.HTTPError as exc:
                if exc.code in {404, 410}:
                    return {
                        "status": "not_found",
                        "http_status": exc.code,
                        "sensitivity_label_id": str(sensitivity_label_id).strip(),
                    }
                if exc.code == 401 and attempt == 0:
                    self.get_access_token()
                    continue
                if exc.code in {429, 500, 502, 503, 504} and attempt < self._max_retries:
                    retry_after = int(exc.headers.get("Retry-After", "2") or "2")
                    time.sleep(max(1, retry_after))
                    continue
                error_body = ""
                try:
                    error_body = exc.read().decode("utf-8", errors="replace")
                except Exception:
                    error_body = ""
                raise RemediationGraphError(
                    f"Assign sensitivity label failed status={exc.code} label_id={sensitivity_label_id}",
                    status_code=exc.code,
                    response_body=error_body,
                ) from exc
            except urllib_error.URLError as exc:
                if attempt < self._max_retries:
                    time.sleep(1 + attempt)
                    continue
                raise RemediationGraphError(
                    f"Assign sensitivity label network error: {exc.reason}"
                ) from exc
        raise RemediationGraphError("Max retries exceeded while assigning sensitivity label")

    def delete_permission(
        self,
        *,
        drive_id: str,
        item_id: str,
        permission_id: str,
    ) -> dict[str, int | str]:
        """Delete a permission from drive item with retry on throttling/5xx."""
        url = (
            "https://graph.microsoft.com/v1.0/drives/"
            f"{urllib_parse.quote(str(drive_id).strip())}"
            f"/items/{urllib_parse.quote(str(item_id).strip())}"
            f"/permissions/{urllib_parse.quote(str(permission_id).strip())}"
        )
        request = urllib_request.Request(
            url,
            method="DELETE",
            headers=self._auth_header(),
        )
        for attempt in range(self._max_retries + 1):
            try:
                with urllib_request.urlopen(request, timeout=30):
                    return {"permission_id": permission_id, "status": "deleted", "http_status": 204}
            except urllib_error.HTTPError as exc:
                if exc.code in {404, 410}:
                    return {"permission_id": permission_id, "status": "not_found", "http_status": exc.code}
                if exc.code == 401 and attempt == 0:
                    self.get_access_token()
                    continue
                if exc.code in {429, 500, 502, 503, 504} and attempt < self._max_retries:
                    retry_after = int(exc.headers.get("Retry-After", "2") or "2")
                    time.sleep(max(1, retry_after))
                    continue
                raise RemediationGraphError(
                    f"Delete permission failed status={exc.code} permission_id={permission_id}"
                ) from exc
            except urllib_error.URLError as exc:
                if attempt < self._max_retries:
                    time.sleep(1 + attempt)
                    continue
                raise RemediationGraphError(
                    f"Delete permission network error: {exc.reason}"
                ) from exc
        raise RemediationGraphError("Max retries exceeded while deleting permission")

    def _post_json(
        self,
        *,
        url: str,
        body: dict[str, Any],
        operation_name: str,
    ) -> dict[str, Any]:
        payload = json.dumps(body, ensure_ascii=True).encode("utf-8")
        headers = {
            **self._auth_header(),
            "Content-Type": "application/json",
        }
        request = urllib_request.Request(
            url,
            data=payload,
            method="POST",
            headers=headers,
        )
        for attempt in range(self._max_retries + 1):
            try:
                with urllib_request.urlopen(request, timeout=30) as response:
                    code = int(getattr(response, "status", 200) or 200)
                    raw = response.read().decode("utf-8").strip()
                    parsed = json.loads(raw) if raw else {}
                    return {"http_status": code, "body": parsed}
            except urllib_error.HTTPError as exc:
                if exc.code == 401 and attempt == 0:
                    self.get_access_token()
                    continue
                if exc.code in {429, 500, 502, 503, 504} and attempt < self._max_retries:
                    retry_after = int(exc.headers.get("Retry-After", "2") or "2")
                    time.sleep(max(1, retry_after))
                    continue
                raise RemediationGraphError(
                    f"{operation_name} failed status={exc.code}"
                ) from exc
            except urllib_error.URLError as exc:
                if attempt < self._max_retries:
                    time.sleep(1 + attempt)
                    continue
                raise RemediationGraphError(
                    f"{operation_name} network error: {exc.reason}"
                ) from exc
        raise RemediationGraphError(f"Max retries exceeded while {operation_name}")

    def get_drive_item_with_permissions(
        self,
        *,
        drive_id: str,
        item_id: str,
    ) -> dict[str, Any]:
        """Fetch driveItem with permissions (same shape as Connect pull_file_metadata)."""
        url = (
            "https://graph.microsoft.com/v1.0/drives/"
            f"{urllib_parse.quote(str(drive_id).strip())}"
            f"/items/{urllib_parse.quote(str(item_id).strip())}"
            "?$expand=permissions"
        )
        request = urllib_request.Request(url, method="GET", headers=self._auth_header())
        for attempt in range(self._max_retries + 1):
            try:
                with urllib_request.urlopen(request, timeout=45) as response:
                    raw = response.read().decode("utf-8").strip()
                    return json.loads(raw) if raw else {}
            except urllib_error.HTTPError as exc:
                if exc.code == 401 and attempt == 0:
                    self.get_access_token()
                    request = urllib_request.Request(url, method="GET", headers=self._auth_header())
                    continue
                if exc.code in {429, 500, 502, 503, 504} and attempt < self._max_retries:
                    retry_after = int(exc.headers.get("Retry-After", "2") or "2")
                    time.sleep(max(1, retry_after))
                    continue
                raise RemediationGraphError(
                    f"get_drive_item failed status={exc.code} drive_id={drive_id}"
                ) from exc
            except urllib_error.URLError as exc:
                if attempt < self._max_retries:
                    time.sleep(1 + attempt)
                    continue
                raise RemediationGraphError(
                    f"get_drive_item network error: {exc.reason}"
                ) from exc
        raise RemediationGraphError("Max retries exceeded while get_drive_item")

    def restore_permission(
        self,
        *,
        drive_id: str,
        item_id: str,
        backup: dict[str, Any],
    ) -> dict[str, int | str]:
        """Restore a previously removed permission where possible."""
        permission_id = str(backup.get("id") or "").strip()
        roles = backup.get("roles") if isinstance(backup.get("roles"), list) else []
        normalized_roles = [str(role).strip().lower() for role in roles if str(role).strip()]
        write_like = any(role in {"write", "owner"} for role in normalized_roles)
        invite_roles = ["write"] if write_like else ["read"]

        link = backup.get("link") if isinstance(backup.get("link"), dict) else {}
        link_scope = str(link.get("scope") or "").strip().lower()
        if link_scope in {"anonymous", "organization"}:
            url = (
                "https://graph.microsoft.com/v1.0/drives/"
                f"{urllib_parse.quote(str(drive_id).strip())}"
                f"/items/{urllib_parse.quote(str(item_id).strip())}/createLink"
            )
            link_type = "edit" if write_like else "view"
            response = self._post_json(
                url=url,
                body={
                    "type": link_type,
                    "scope": link_scope,
                },
                operation_name="restore link permission",
            )
            return {
                "permission_id": permission_id,
                "status": "restored",
                "http_status": int(response["http_status"]),
            }

        email = _extract_permission_email(backup)
        if not email:
            return {
                "permission_id": permission_id,
                "status": "manual_required",
                "reason": "rollback_email_missing",
            }

        url = (
            "https://graph.microsoft.com/v1.0/drives/"
            f"{urllib_parse.quote(str(drive_id).strip())}"
            f"/items/{urllib_parse.quote(str(item_id).strip())}/invite"
        )
        response = self._post_json(
            url=url,
            body={
                "recipients": [{"email": email}],
                "requireSignIn": True,
                "sendInvitation": False,
                "roles": invite_roles,
            },
            operation_name="restore user permission",
        )
        return {
            "permission_id": permission_id,
            "status": "restored",
            "http_status": int(response["http_status"]),
        }
