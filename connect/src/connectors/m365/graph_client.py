"""T-019: Graph API クライアント

Azure AD OAuth2 認証、リトライ、429 (Too Many Requests) ハンドリングを提供する。
"""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

from src.shared.config import get_config
from src.shared.ssm import get_param, put_param

logger = logging.getLogger(__name__)


class GraphApiError(Exception):
    """Graph API 呼び出しエラー"""

    def __init__(self, status_code: int, message: str, response_body: Any = None):
        self.status_code = status_code
        self.response_body = response_body
        super().__init__(f"Graph API error {status_code}: {message}")


class GraphClient:
    """Microsoft Graph API クライアント

    - OAuth2 Client Credentials でアクセストークンを取得
    - 429 (Throttling) 時に Retry-After に従って待機
    - 指数バックオフによるリトライ
    """

    def __init__(
        self,
        client_id: str | None = None,
        tenant_id: str | None = None,
        client_secret: str | None = None,
        access_token: str | None = None,
    ):
        cfg = get_config()
        self._cfg = cfg
        self._client_id = client_id
        self._tenant_id = tenant_id
        self._client_secret = client_secret
        self._access_token = access_token
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})

    @classmethod
    def from_ssm(cls) -> GraphClient:
        """SSM Parameter Store から認証情報を読み込んで初期化"""
        cfg = get_config()
        client = cls(
            client_id=get_param(cfg.ssm_client_id, decrypt=False),
            tenant_id=get_param(cfg.ssm_tenant_id, decrypt=False),
            client_secret=get_param(cfg.ssm_client_secret),
            access_token=get_param(cfg.ssm_access_token),
        )
        return client

    # ==========================================
    # トークン管理
    # ==========================================

    def get_access_token(self) -> str:
        """Azure AD から新しいアクセストークンを取得する

        Returns:
            アクセストークン文字列
        """
        cfg = self._cfg
        url = cfg.graph_token_url(self._tenant_id)

        resp = self._session.post(
            url,
            data={
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "scope": cfg.graph_scope,
            },
            timeout=cfg.graph_api_timeout,
        )

        if resp.status_code != 200:
            raise GraphApiError(
                resp.status_code,
                f"Token acquisition failed: {resp.text}",
                resp.json() if resp.text else None,
            )

        data = resp.json()
        self._access_token = data["access_token"]
        return self._access_token

    def refresh_and_store_token(self) -> str:
        """トークンを更新し SSM Parameter Store に保存する

        Returns:
            新しいアクセストークン
        """
        token = self.get_access_token()
        cfg = self._cfg
        put_param(cfg.ssm_access_token, token, param_type="SecureString")
        logger.info("Access token refreshed and stored in SSM")
        return token

    # ==========================================
    # HTTP メソッド (リトライ・429 ハンドリング付き)
    # ==========================================

    def _auth_headers(self) -> dict[str, str]:
        """Authorization ヘッダーを返す"""
        return {"Authorization": f"Bearer {self._access_token}"}

    def _request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
        timeout: int | None = None,
    ) -> requests.Response:
        """リトライ・429 ハンドリング付き HTTP リクエスト"""
        cfg = self._cfg
        timeout = timeout or cfg.graph_api_timeout
        max_retries = cfg.graph_api_max_retries
        backoff = cfg.graph_api_retry_backoff

        for attempt in range(max_retries + 1):
            try:
                resp = self._session.request(
                    method,
                    url,
                    headers=self._auth_headers(),
                    params=params,
                    json=json_body,
                    timeout=timeout,
                )

                # 成功
                if resp.status_code < 400:
                    return resp

                # 429 Too Many Requests — Retry-After に従う
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 10))
                    logger.warning(
                        f"429 Throttled. Retry-After={retry_after}s "
                        f"(attempt {attempt + 1}/{max_retries + 1})"
                    )
                    if attempt < max_retries:
                        time.sleep(retry_after)
                        continue

                # 401 Unauthorized — トークン再取得して1回だけリトライ
                if resp.status_code == 401 and attempt == 0:
                    logger.warning("401 Unauthorized — refreshing access token")
                    self.get_access_token()
                    continue

                # 5xx — リトライ
                if resp.status_code >= 500 and attempt < max_retries:
                    wait = backoff * (2**attempt)
                    logger.warning(
                        f"{resp.status_code} Server error. Retrying in {wait}s "
                        f"(attempt {attempt + 1}/{max_retries + 1})"
                    )
                    time.sleep(wait)
                    continue

                # それ以外のエラー
                raise GraphApiError(
                    resp.status_code,
                    resp.text[:500],
                    resp.json() if resp.headers.get("content-type", "").startswith("application/json") else None,
                )

            except requests.exceptions.Timeout:
                if attempt < max_retries:
                    wait = backoff * (2**attempt)
                    logger.warning(f"Timeout. Retrying in {wait}s")
                    time.sleep(wait)
                    continue
                raise

        # ここに到達するのは全リトライ失敗時のみ
        raise GraphApiError(429, "Max retries exceeded due to throttling")

    def get(
        self, url: str, *, params: dict[str, str] | None = None, timeout: int | None = None
    ) -> requests.Response:
        """GET リクエスト"""
        return self._request("GET", url, params=params, timeout=timeout)

    def post(
        self, url: str, *, json_body: dict[str, Any] | None = None, timeout: int | None = None
    ) -> requests.Response:
        """POST リクエスト"""
        return self._request("POST", url, json_body=json_body, timeout=timeout)

    def patch(
        self, url: str, *, json_body: dict[str, Any] | None = None, timeout: int | None = None
    ) -> requests.Response:
        """PATCH リクエスト"""
        return self._request("PATCH", url, json_body=json_body, timeout=timeout)

    def delete(self, url: str, *, timeout: int | None = None) -> requests.Response:
        """DELETE リクエスト"""
        return self._request("DELETE", url, timeout=timeout)

    # ==========================================
    # 便利メソッド
    # ==========================================

    def graph_get(
        self,
        path: str,
        *,
        params: dict[str, str] | None = None,
        timeout: int | None = None,
    ) -> dict[str, Any]:
        """Graph API v1.0 に GET リクエストし、JSON を返す

        Args:
            path: API パス（例: "/drives/{driveId}/root/delta"）
            params: クエリパラメータ
            timeout: タイムアウト秒

        Returns:
            JSON レスポンス dict
        """
        url = f"{self._cfg.graph_base_url}{path}"
        resp = self.get(url, params=params, timeout=timeout)
        return resp.json()

    def graph_post(
        self,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        timeout: int | None = None,
    ) -> dict[str, Any]:
        """Graph API v1.0 に POST リクエストし、JSON を返す"""
        url = f"{self._cfg.graph_base_url}{path}"
        resp = self.post(url, json_body=json_body, timeout=timeout)
        return resp.json()

    def graph_patch(
        self,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        timeout: int | None = None,
    ) -> dict[str, Any]:
        """Graph API v1.0 に PATCH リクエストし、JSON を返す"""
        url = f"{self._cfg.graph_base_url}{path}"
        resp = self.patch(url, json_body=json_body, timeout=timeout)
        return resp.json()
