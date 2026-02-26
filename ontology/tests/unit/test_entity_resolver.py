from __future__ import annotations

from src.handlers import entity_resolver
from src.models.entity_candidate import EntityCandidate


class _Cursor:
    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self._conn.executed.append((sql, params))

    def fetchone(self):
        if self._conn.fetchone_results:
            return self._conn.fetchone_results.pop(0)
        return None

    def fetchall(self):
        if self._conn.fetchall_results:
            return self._conn.fetchall_results.pop(0)
        return []


class _Conn:
    def __init__(self):
        self.executed = []
        self.fetchone_results = []
        self.fetchall_results = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return _Cursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def _candidate(**overrides) -> EntityCandidate:
    c = EntityCandidate(
        candidate_id="cand-1",
        tenant_id="tenant-1",
        source_item_id="item-1",
        surface_form="田中太郎",
        normalized_form="タナカタロウ",
        entity_type="person",
        pii_flag=True,
        extraction_source="governance+ner",
        confidence=0.95,
        mention_count=3,
        context_snippet="",
        ner_label="",
        language="ja",
        source_title="",
        extracted_at="2026-02-25T00:00:00Z",
    )
    for key, value in overrides.items():
        setattr(c, key, value)
    return c


def test_create_entity_pii_uses_encryption_sql(monkeypatch) -> None:
    conn = _Conn()
    cur = conn.cursor()
    monkeypatch.setattr(entity_resolver, "generate_entity_id", lambda _t: "pii_person_fixed")
    candidate = _candidate(pii_flag=True, pii_category="person_name")

    entity_id = entity_resolver._create_entity(
        cur=cur, candidate=candidate, encryption_key="secret-key"
    )
    assert entity_id == "pii_person_fixed"
    assert "pgp_sym_encrypt" in conn.executed[-1][0]


def test_create_entity_non_pii_uses_plain_text_columns(monkeypatch) -> None:
    conn = _Conn()
    cur = conn.cursor()
    monkeypatch.setattr(entity_resolver, "generate_entity_id", lambda _t: "org_fixed")
    candidate = _candidate(
        pii_flag=False,
        entity_type="organization",
        normalized_form="ACME Corp",
        extraction_source="ner",
    )

    entity_id = entity_resolver._create_entity(
        cur=cur, candidate=candidate, encryption_key=""
    )
    assert entity_id == "org_fixed"
    sql, params = conn.executed[-1]
    assert "canonical_value_text" in sql
    assert params[3] == "ACME Corp"


def test_resolve_entity_match_path_adds_alias_and_commits(monkeypatch) -> None:
    conn = _Conn()
    candidate = _candidate()

    monkeypatch.setattr(
        entity_resolver,
        "_find_existing_by_hash",
        lambda **kwargs: {"entity_id": "ent-existing"},
    )
    monkeypatch.setattr(
        entity_resolver,
        "_add_alias",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        entity_resolver,
        "_update_entity_after_match",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        entity_resolver,
        "_insert_audit_log",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        entity_resolver,
        "check_pii_aggregation_alert",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(entity_resolver, "publish_metric", lambda *args, **kwargs: None)

    result = entity_resolver._resolve_entity(conn, candidate, "secret-key")
    assert result == {"action": "matched", "entity_id": "ent-existing"}
    assert conn.commits == 1
    assert conn.rollbacks == 0


def test_resolve_entity_create_path_commits(monkeypatch) -> None:
    conn = _Conn()
    candidate = _candidate(
        pii_flag=False, entity_type="organization", extraction_source="ner"
    )

    monkeypatch.setattr(entity_resolver, "_find_existing_by_hash", lambda **kwargs: None)
    monkeypatch.setattr(entity_resolver, "_find_blocking_candidates", lambda **kwargs: [])
    monkeypatch.setattr(entity_resolver, "_create_entity", lambda **kwargs: "ent-new")
    monkeypatch.setattr(entity_resolver, "_add_alias", lambda **kwargs: None)
    monkeypatch.setattr(entity_resolver, "_insert_audit_log", lambda **kwargs: None)
    monkeypatch.setattr(entity_resolver, "publish_metric", lambda *args, **kwargs: None)

    result = entity_resolver._resolve_entity(conn, candidate, "")
    assert result == {"action": "created", "entity_id": "ent-new"}
    assert conn.commits == 1
    assert conn.rollbacks == 0


def test_resolve_entity_rolls_back_on_error(monkeypatch) -> None:
    conn = _Conn()
    candidate = _candidate()
    monkeypatch.setattr(entity_resolver, "_find_existing_by_hash", lambda **kwargs: None)
    monkeypatch.setattr(entity_resolver, "_find_blocking_candidates", lambda **kwargs: [])
    monkeypatch.setattr(
        entity_resolver,
        "_create_entity",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(entity_resolver, "publish_metric", lambda *args, **kwargs: None)

    try:
        entity_resolver._resolve_entity(conn, candidate, "secret")
    except RuntimeError as exc:
        assert str(exc) == "boom"
    else:
        raise AssertionError("RuntimeError was not raised")

    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_check_pii_aggregation_alert_publishes_sns(monkeypatch) -> None:
    conn = _Conn()
    conn.fetchone_results.append({"alias_count": 5})
    monkeypatch.setenv("ALERT_TOPIC_ARN", "arn:aws:sns:ap-northeast-1:123456789012:topic")

    published = []

    class _SnsClient:
        def publish(self, **kwargs):
            published.append(kwargs)

    class _Boto3:
        @staticmethod
        def client(service_name):
            assert service_name == "sns"
            return _SnsClient()

    monkeypatch.setattr(entity_resolver, "boto3", _Boto3(), raising=False)
    monkeypatch.setattr(entity_resolver, "publish_metric", lambda *args, **kwargs: None)

    entity_resolver.check_pii_aggregation_alert(
        conn=conn,
        entity_id="ent-1",
        entity_type="person",
        tenant_id="tenant-1",
    )
    assert len(published) == 1
