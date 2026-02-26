from __future__ import annotations

from src.shared import aurora_client


class _Cursor:
    def execute(self, sql):
        return None


class _Conn:
    def __init__(self, closed=0, fail_healthcheck=False):
        self.closed = closed
        self.fail_healthcheck = fail_healthcheck
        self.autocommit = True

    def cursor(self):
        if self.fail_healthcheck:
            raise RuntimeError("broken")
        return _Cursor()


def test_get_connection_creates_new(monkeypatch) -> None:
    aurora_client._conn = None

    def _connect(*, secrets_client=None):
        return _Conn()

    conn = aurora_client.get_aurora_connection(connect_func=_connect)
    assert conn is not None
    assert conn.autocommit is False


def test_get_connection_reuses_existing(monkeypatch) -> None:
    existing = _Conn()
    aurora_client._conn = existing
    conn = aurora_client.get_aurora_connection(connect_func=lambda **kwargs: _Conn())
    assert conn is existing


def test_get_connection_reconnects_on_broken_connection(monkeypatch) -> None:
    aurora_client._conn = _Conn(fail_healthcheck=True)
    new_conn = _Conn()
    conn = aurora_client.get_aurora_connection(connect_func=lambda **kwargs: new_conn)
    assert conn is new_conn
