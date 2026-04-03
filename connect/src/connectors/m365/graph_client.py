"""T-019: Graph API クライアント

Azure AD OAuth2 認証、リトライ、429 (Too Many Requests) ハンドリングを提供する。
"""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

from src.shared.config import get_config
from src.shared.ssm import get_graph_credentials_scoped, put_param

logger = logging.getLogger(__name__)


class GraphApiError(Exception):
    """Graph API 呼び出し失敗を表す例外。"""

    def __init__(self, status_code: int, message: str, response_body: Any = None):
        """GraphApiError を初期化する。

        Args:
            status_code: HTTP ステータスコード
            message: エラー概要
            response_body: 可能であれば API レスポンス本文
        """
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
        """GraphClient を初期化する。

        通常は `from_ssm` を経由して生成し、認証情報を明示的に渡すのは
        テストや特殊用途に限定する想定。

        Args:
            client_id: Azure AD アプリケーション ID
            tenant_id: Azure AD テナント ID
            client_secret: アプリケーションシークレット
            access_token: 既存アクセストークン
        """
        cfg = get_config()
        self._cfg = cfg
        self._client_id = client_id
        self._tenant_id = tenant_id
        self._client_secret = client_secret
        self._access_token = access_token
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})

    @classmethod
    def from_ssm(cls, *, tenant_id: str | None = None, connection_id: str | None = None) -> GraphClient:
        """SSM Parameter Store から認証情報を読み込んで初期化する。

        tenant/connection スコープを優先し、未設定時は互換キーへフォールバックする。

        Args:
            tenant_id: テナント識別子
            connection_id: 接続識別子

        Returns:
            初期化済み GraphClient
        """
        cfg = get_config()
        normalized_tenant_id = str(tenant_id or cfg.tenant_id).strip()
        normalized_connection_id = str(connection_id or "").strip()
        credentials = get_graph_credentials_scoped(
            tenant_id=normalized_tenant_id,
            connection_id=normalized_connection_id,
        )
        client = cls(
            client_id=credentials.get("client_id"),
            tenant_id=credentials.get("tenant_id"),
            client_secret=credentials.get("client_secret"),
            access_token=credentials.get("access_token"),
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

    def refresh_and_store_token(self, *, tenant_id: str | None = None, connection_id: str | None = None) -> str:
        """トークンを更新し SSM Parameter Store に保存する

        Args:
            tenant_id: 保存先のテナント識別子
            connection_id: 保存先の接続識別子

        Returns:
            新しいアクセストークン
        """
        token = self.get_access_token()
        cfg = self._cfg
        normalized_tenant_id = str(tenant_id or cfg.tenant_id).strip()
        normalized_connection_id = str(connection_id or "").strip()
        if normalized_tenant_id and normalized_connection_id:
            put_param(
                f"/aiready/connect/{normalized_tenant_id}/{normalized_connection_id}/access_token",
                token,
                param_type="SecureString",
            )
            put_param(
                f"/aiready/connect/{normalized_tenant_id}/access_token",
                token,
                param_type="SecureString",
            )
        if normalized_tenant_id and not normalized_connection_id:
            put_param(
                f"/aiready/connect/{normalized_tenant_id}/access_token",
                token,
                param_type="SecureString",
            )
        if not normalized_tenant_id:
            put_param(cfg.ssm_access_token, token, param_type="SecureString")
        else:
            # Compatibility path for legacy handlers still reading global key.
            put_param(cfg.ssm_access_token, token, param_type="SecureString")
        logger.info("Access token refreshed and stored in SSM")
        return token

    # ==========================================
    # HTTP メソッド (リトライ・429 ハンドリング付き)
    # ==========================================

    def _auth_headers(self) -> dict[str, str]:
        """Authorization ヘッダーを返す。

        Returns:
            Bearer トークン付きヘッダー
        """
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
        """リトライ・429 ハンドリング付き HTTP リクエストを実行する。

        Args:
            method: HTTP メソッド
            url: リクエスト先 URL
            params: クエリパラメータ
            json_body: JSON ボディ
            timeout: タイムアウト秒

        Returns:
            HTTP レスポンス

        Raises:
            GraphApiError: API 応答が失敗扱いの場合
            requests.exceptions.Timeout: リトライ上限後もタイムアウトした場合
        """
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

                # 2xx/3xx は成功としてそのまま返す。
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

                # 上記以外の 4xx は再試行せず呼び出し側へ例外を返す。
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
        """GET リクエストを実行する。

        Args:
            url: リクエスト先 URL
            params: クエリパラメータ
            timeout: タイムアウト秒

        Returns:
            HTTP レスポンス
        """
        return self._request("GET", url, params=params, timeout=timeout)

    def post(
        self, url: str, *, json_body: dict[str, Any] | None = None, timeout: int | None = None
    ) -> requests.Response:
        """POST リクエストを実行する。

        Args:
            url: リクエスト先 URL
            json_body: JSON ボディ
            timeout: タイムアウト秒

        Returns:
            HTTP レスポンス
        """
        return self._request("POST", url, json_body=json_body, timeout=timeout)

    def patch(
        self, url: str, *, json_body: dict[str, Any] | None = None, timeout: int | None = None
    ) -> requests.Response:
        """PATCH リクエストを実行する。

        Args:
            url: リクエスト先 URL
            json_body: JSON ボディ
            timeout: タイムアウト秒

        Returns:
            HTTP レスポンス
        """
        return self._request("PATCH", url, json_body=json_body, timeout=timeout)

    def delete(self, url: str, *, timeout: int | None = None) -> requests.Response:
        """DELETE リクエストを実行する。

        Args:
            url: リクエスト先 URL
            timeout: タイムアウト秒

        Returns:
            HTTP レスポンス
        """
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

    def graph_get_absolute(
        self,
        absolute_url: str,
        *,
        timeout: int | None = None,
    ) -> dict[str, Any]:
        """@odata.nextLink など Graph が返す絶対 URL へ GET する。"""
        resp = self.get(absolute_url, timeout=timeout)
        return resp.json()

    def graph_post(
        self,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        timeout: int | None = None,
    ) -> dict[str, Any]:
        """Graph API v1.0 に POST リクエストし、JSON を返す。

        Args:
            path: API パス
            json_body: JSON ボディ
            timeout: タイムアウト秒

        Returns:
            JSON レスポンス dict
        """
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
        """Graph API v1.0 に PATCH リクエストし、JSON を返す。

        Args:
            path: API パス
            json_body: JSON ボディ
            timeout: タイムアウト秒

        Returns:
            JSON レスポンス dict
        """
        url = f"{self._cfg.graph_base_url}{path}"
        resp = self.patch(url, json_body=json_body, timeout=timeout)
        return resp.json()
