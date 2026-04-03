from datetime import datetime, timedelta

import jwt
from jwt import PyJWKClient
from dotenv import load_dotenv
from fastapi import HTTPException, status
from pydantic import BaseModel

from config import global_args

# use the .env that is inside the current folder
# allows to use different .env file for each API instance
# the OS environment variables take precedence over the .env file
load_dotenv(dotenv_path=".env", override=False)


class TokenPayload(BaseModel):
    sub: str  # Username
    exp: datetime  # Expiration time
    role: str = "user"  # User role, default is regular user
    tenant_id: str | None = None  # Canonical tenant claim for API authorization
    metadata: dict = {}  # Additional metadata


class AuthHandler:
    def __init__(self):
        self.secret = global_args.token_secret
        self.algorithm = global_args.jwt_algorithm
        self.expire_hours = global_args.token_expire_hours
        self.guest_expire_hours = global_args.guest_token_expire_hours
        self.cognito_jwks_url = global_args.cognito_jwks_url
        self.cognito_issuer = global_args.cognito_issuer
        self.cognito_audience = global_args.cognito_audience
        self.tenant_claim_keys = [
            key.strip() for key in str(global_args.tenant_claim_keys).split(",") if key.strip()
        ]
        self.admin_roles = {
            role.strip().lower()
            for role in str(global_args.admin_roles).split(",")
            if role.strip()
        }
        self.default_tenant_id = global_args.default_tenant_id
        self._jwks_client = PyJWKClient(self.cognito_jwks_url) if self.cognito_jwks_url else None
        self.accounts = {}
        self.account_tenant_map: dict[str, str] = {}
        auth_accounts = global_args.auth_accounts
        if auth_accounts:
            for account in auth_accounts.split(","):
                user_pass = account.split(":", 1)
                if len(user_pass) != 2:
                    continue
                username, password = user_pass[0], user_pass[1]
                self.accounts[username] = password
        auth_account_tenants = str(getattr(global_args, "auth_account_tenants", "") or "")
        if auth_account_tenants:
            for entry in auth_account_tenants.split(","):
                user_tenant = entry.split(":", 1)
                if len(user_tenant) != 2:
                    continue
                username = str(user_tenant[0]).strip()
                tenant_id = str(user_tenant[1]).strip()
                if username and tenant_id:
                    self.account_tenant_map[username] = tenant_id

    def create_token(
        self,
        username: str,
        role: str = "user",
        custom_expire_hours: int = None,
        metadata: dict = None,
        tenant_id: str | None = None,
    ) -> str:
        """
        Create JWT token

        Args:
            username: Username
            role: User role, default is "user", guest is "guest"
            custom_expire_hours: Custom expiration time (hours), if None use default value
            metadata: Additional metadata

        Returns:
            str: Encoded JWT token
        """
        # Choose default expiration time based on role
        if custom_expire_hours is None:
            if role == "guest":
                expire_hours = self.guest_expire_hours
            else:
                expire_hours = self.expire_hours
        else:
            expire_hours = custom_expire_hours

        expire = datetime.utcnow() + timedelta(hours=expire_hours)

        # Create payload
        metadata_payload = metadata or {}
        metadata_tenant = str(metadata_payload.get("tenant_id", "")).strip()
        explicit_tenant = str(tenant_id).strip() if tenant_id is not None else ""
        default_tenant = str(self.default_tenant_id).strip()
        effective_tenant_id = explicit_tenant or metadata_tenant or default_tenant or "tenant-default"
        metadata_payload["tenant_id"] = effective_tenant_id
        payload = TokenPayload(
            sub=username,
            exp=expire,
            role=role,
            tenant_id=effective_tenant_id,
            metadata=metadata_payload,
        )

        return jwt.encode(payload.dict(), self.secret, algorithm=self.algorithm)

    def resolve_login_tenant_id(self, username: str) -> str | None:
        normalized_username = str(username or "").strip()
        if not normalized_username:
            return str(self.default_tenant_id).strip() or None
        mapped = str(self.account_tenant_map.get(normalized_username, "")).strip()
        if mapped:
            return mapped
        default_tenant = str(self.default_tenant_id).strip()
        return default_tenant or None

    @staticmethod
    def _read_nested_claim(payload: dict, path: str):
        current = payload
        for token in path.split("."):
            if not isinstance(current, dict) or token not in current:
                return None
            current = current[token]
        return current

    def _resolve_tenant_id(self, payload: dict) -> str | None:
        for claim_key in self.tenant_claim_keys:
            value = self._read_nested_claim(payload, claim_key)
            if value is None:
                continue
            tenant_id = str(value).strip()
            if tenant_id:
                return tenant_id
        if self.default_tenant_id:
            return str(self.default_tenant_id).strip() or None
        return None

    @staticmethod
    def _normalize_roles(payload: dict) -> list[str]:
        roles: list[str] = []
        candidate_fields = ["role", "roles", "cognito:groups", "groups"]
        for field in candidate_fields:
            value = payload.get(field)
            if isinstance(value, str):
                roles.extend([r.strip() for r in value.split(",") if r.strip()])
            elif isinstance(value, list):
                roles.extend([str(r).strip() for r in value if str(r).strip()])
        if not roles:
            roles = ["user"]
        return list(dict.fromkeys(roles))

    def _decode_with_jwks(self, token: str) -> dict:
        if self._jwks_client is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Cognito JWKS is not configured.",
            )
        signing_key = self._jwks_client.get_signing_key_from_jwt(token)
        decode_kwargs = {
            "algorithms": ["RS256"],
            "issuer": self.cognito_issuer or None,
            "options": {"verify_aud": bool(self.cognito_audience)},
        }
        if self.cognito_audience:
            decode_kwargs["audience"] = self.cognito_audience
        return jwt.decode(token, signing_key.key, **decode_kwargs)

    def _decode_with_local_secret(self, token: str) -> dict:
        return jwt.decode(token, self.secret, algorithms=[self.algorithm])

    def validate_token(self, token: str) -> dict:
        """
        Validate JWT token

        Args:
            token: JWT token

        Returns:
            dict: Dictionary containing user information

        Raises:
            HTTPException: If token is invalid or expired
        """
        try:
            if self._jwks_client is not None:
                payload = self._decode_with_jwks(token)
                auth_source = "cognito_jwks"
            else:
                payload = self._decode_with_local_secret(token)
                auth_source = "local_jwt"
            expire_timestamp = payload["exp"]
            expire_time = datetime.utcfromtimestamp(expire_timestamp)

            if datetime.utcnow() > expire_time:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired"
                )

            # Return complete payload instead of just username
            roles = self._normalize_roles(payload)
            tenant_id = self._resolve_tenant_id(payload)
            return {
                "username": payload["sub"],
                "role": payload.get("role", roles[0] if roles else "user"),
                "roles": roles,
                "metadata": payload.get("metadata", {}),
                "exp": expire_time,
                "tenant_id": tenant_id,
                "claims": payload,
                "auth_source": auth_source,
                "is_admin": any(role.lower() in self.admin_roles for role in roles),
            }
        except jwt.PyJWTError as e:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Invalid token: {str(e)}",
            )


auth_handler = AuthHandler()
